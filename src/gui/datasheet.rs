//! Datasheet view: browse tables and views (`N sublist`, `q` `where` filter, `xasc`/`xdesc` sort),
//! and inline edit / add / delete rows on keyed tables.
use crate::backend::Backend;
use crate::ui::{display, Event, ERROR, KEY_HEADER};
use egui::{Key, RichText, Sense, TextEdit};
use egui_extras::{Column, TableBuilder};
use kixdb::inspect::TableInfo;
use kixdb::{tools, K};

const LIMITS: [usize; 4] = [100, 1000, 10_000, 100_000];

struct Edit {
    row: usize,
    col: usize,
    text: String,
    focused: bool,
}

pub struct Datasheet {
    pub table: String,
    info: Option<TableInfo>,
    pub columns: Vec<String>,
    pub key_count: usize,
    pub rows: Vec<Vec<K>>,
    pub total: Option<i64>,
    limit: usize,
    filter: String,
    applied_filter: String,
    sort: Option<(usize, bool)>,
    dirty: bool,
    error: Option<String>,
    editing: Option<Edit>,
    selected: Option<usize>,
    confirm_delete: bool,
    new_row: Vec<String>,
}

impl Datasheet {
    pub fn new(table: String) -> Self {
        Self {
            table,
            info: None,
            columns: vec![],
            key_count: 0,
            rows: vec![],
            total: None,
            limit: 1000,
            filter: String::new(),
            applied_filter: String::new(),
            sort: None,
            dirty: true,
            error: None,
            editing: None,
            selected: None,
            confirm_delete: false,
            new_row: vec![],
        }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn editable(&self) -> bool {
        self.info.as_ref().is_some_and(|i| !i.key_columns().is_empty())
    }

    pub fn load(&mut self, conn: &mut dyn Backend) {
        self.dirty = false;
        self.error = None;
        self.editing = None;
        self.confirm_delete = false;
        self.info = conn.table_info(&self.table);
        self.key_count = self.info.as_ref().map(|i| i.key_columns().len()).unwrap_or(0);

        let wh = if self.applied_filter.trim().is_empty() {
            String::new()
        } else {
            format!(" where {}", self.applied_filter)
        };
        let base_q = format!("select from {}{wh}", self.table);
        let sorted_q = match self.sort {
            Some((ci, desc)) if ci < self.columns.len() => {
                let op = if desc { "xdesc" } else { "xasc" };
                format!("`{} {op} ({base_q})", self.columns[ci])
            }
            _ => base_q.clone(),
        };
        let limited_q = format!("{} sublist ({sorted_q})", self.limit);

        let res = conn.execute(&limited_q);
        if let Some(e) = res.first_error() {
            self.error = Some(e.to_string());
            self.rows.clear();
            return;
        }
        if let Some(rs) = res.resultsets().first() {
            self.columns = rs.columns.clone();
            self.rows = rs.rows.clone();
            if self.key_count == 0 {
                self.key_count = rs.key_count;
            }
        }
        self.selected = self.selected.filter(|&r| r < self.rows.len());
        self.total = conn.eval(&format!("count ({base_q})")).ok().and_then(|k| k.as_i64().ok());
        if self.new_row.len() != self.columns.len() {
            self.new_row = vec![String::new(); self.columns.len()];
        }
    }

    fn where_for_row(&self, row: usize) -> Option<String> {
        let info = self.info.as_ref()?;
        let mut preds = Vec::new();
        for (i, cname) in self.columns.iter().enumerate() {
            if info.columns.iter().any(|c| c.is_key && c.name == *cname) {
                let val = self.rows.get(row)?.get(i)?;
                preds.push(format!("{cname} = {}", tools::q_literal(val)));
            }
        }
        if preds.is_empty() {
            None
        } else {
            Some(preds.join(", "))
        }
    }

    pub fn commit_edit(&mut self, conn: &mut dyn Backend, row: usize, col: usize, text: &str) -> Result<(), String> {
        let info = self.info.as_ref().ok_or("table metadata unavailable")?;
        let ci = info.columns.iter().find(|c| c.name == self.columns[col]).ok_or("unknown column")?;
        if ci.is_key {
            return Err("key columns cannot be edited in-place (insert a new key and delete the old row)".into());
        }
        let lit = tools::q_literal_for_column(text, ci.type_char).map_err(|e| e.to_string())?;
        let wh = self.where_for_row(row).ok_or("only keyed tables can be edited cell-by-cell")?;
        let q = format!("update {}: {lit} from `{} where {wh}", ci.name, self.table);
        conn.eval(&q).map(|_| ()).map_err(|e| e.to_string())
    }

    pub fn delete_row(&mut self, conn: &mut dyn Backend, row: usize) -> Result<(), String> {
        let wh = self.where_for_row(row).ok_or("only keyed tables support row deletion")?;
        conn.eval(&format!("delete from `{} where {wh}", self.table)).map(|_| ()).map_err(|e| e.to_string())
    }

    pub fn insert_new_row(&mut self, conn: &mut dyn Backend) -> Result<(), String> {
        let info = self.info.as_ref().ok_or("table metadata unavailable")?;
        let mut lits = Vec::with_capacity(info.columns.len());
        for (i, c) in info.columns.iter().enumerate() {
            let txt = self.new_row.get(i).map(String::as_str).unwrap_or("");
            lits.push(tools::q_literal_for_column(txt, c.type_char).map_err(|e| e.to_string())?);
        }
        let q = format!("`{} upsert ({})", self.table, lits.join("; "));
        conn.eval(&q).map(|_| ()).map_err(|e| e.to_string())?;
        for s in &mut self.new_row {
            s.clear();
        }
        Ok(())
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, conn: &mut dyn Backend, ev: &mut Vec<Event>) {
        if self.dirty {
            self.load(conn);
        }
        let editable = self.editable();

        ui.horizontal(|ui| {
            if ui.button("Refresh").clicked() {
                self.load(conn);
            }
            if self.info.is_some() && ui.button("Meta / Design").clicked() {
                ev.push(Event::OpenDesign(self.table.clone()));
            }
            ui.separator();
            ui.label("Rows:");
            egui::ComboBox::from_id_salt(("limit", &self.table))
                .selected_text(self.limit.to_string())
                .width(75.0)
                .show_ui(ui, |ui| {
                    for n in LIMITS {
                        if ui.selectable_value(&mut self.limit, n, n.to_string()).clicked() {
                            self.load(conn);
                        }
                    }
                });
            ui.label("where");
            let w_edit = ui.add(
                TextEdit::singleline(&mut self.filter)
                    .hint_text("e.g. sym = `AAPL, price > 180")
                    .desired_width(240.0),
            );
            if (w_edit.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter))) || ui.button("Apply").clicked() {
                self.applied_filter = self.filter.clone();
                self.load(conn);
            }
            if !self.applied_filter.is_empty() && ui.button("Clear").clicked() {
                self.filter.clear();
                self.applied_filter.clear();
                self.load(conn);
            }
            if let Some(tot) = self.total {
                ui.label(RichText::new(format!("({} of {tot} rows)", self.rows.len())).weak());
            }
        });

        if editable {
            ui.horizontal(|ui| {
                if let Some(sel) = self.selected {
                    if self.confirm_delete {
                        ui.colored_label(ERROR, format!("Delete row {}?", sel + 1));
                        if ui.button("Delete").clicked() {
                            match self.delete_row(conn, sel) {
                                Ok(()) => {
                                    self.load(conn);
                                    ev.push(Event::Changed);
                                }
                                Err(m) => self.error = Some(m),
                            }
                        }
                        if ui.button("Cancel").clicked() {
                            self.confirm_delete = false;
                        }
                    } else if ui.button(format!("Delete row {}", sel + 1)).clicked() {
                        self.confirm_delete = true;
                    }
                } else {
                    ui.label(RichText::new("Double-click a value cell to edit; click row # to select for deletion.").weak());
                }
            });
        } else if self.info.is_some() {
            ui.label(RichText::new("Unkeyed / partitioned table (read-only grid; apply xkey in Meta/Design to enable row edits).").weak());
        }

        if let Some(err) = &self.error {
            ui.colored_label(ERROR, err);
        }
        ui.separator();

        if editable && !self.columns.is_empty() {
            egui::CollapsingHeader::new("Upsert row").default_open(false).show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    for (i, c) in self.columns.iter().enumerate() {
                        ui.label(format!("{c}:"));
                        if let Some(buf) = self.new_row.get_mut(i) {
                            ui.add(TextEdit::singleline(buf).desired_width(90.0));
                        }
                    }
                    if ui.button("Upsert").clicked() {
                        match self.insert_new_row(conn) {
                            Ok(()) => {
                                self.load(conn);
                                ev.push(Event::Changed);
                            }
                            Err(m) => self.error = Some(m),
                        }
                    }
                });
            });
        }

        let mut header_sort: Option<usize> = None;
        let mut start_edit: Option<(usize, usize)> = None;
        let mut save_edit: Option<(usize, usize, String)> = None;
        let mut cancel_edit = false;
        let key_cnt = self.key_count;
        let cols_snap = self.columns.clone();

        TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(Column::exact(42.0))
            .columns(Column::initial(130.0).at_least(55.0).clip(true), cols_snap.len().max(1))
            .header(24.0, |mut h| {
                h.col(|ui| {
                    ui.label(RichText::new("#").weak());
                });
                for (ci, c) in cols_snap.iter().enumerate() {
                    h.col(|ui| {
                        let arrow = match self.sort {
                            Some((sc, false)) if sc == ci => " ▲",
                            Some((sc, true)) if sc == ci => " ▼",
                            _ => "",
                        };
                        let pfx = if ci < key_cnt { "🔑 " } else { "" };
                        let rt = if ci < key_cnt {
                            RichText::new(format!("{pfx}{c}{arrow}")).strong().color(KEY_HEADER)
                        } else {
                            RichText::new(format!("{c}{arrow}")).strong()
                        };
                        if ui.add(egui::Label::new(rt).sense(Sense::click())).clicked() {
                            header_sort = Some(ci);
                        }
                    });
                }
            })
            .body(|body| {
                body.rows(22.0, self.rows.len(), |mut row| {
                    let ri = row.index();
                    row.col(|ui| {
                        let sel = self.selected == Some(ri);
                        if ui.selectable_label(sel, (ri + 1).to_string()).clicked() {
                            self.selected = if sel { None } else { Some(ri) };
                            self.confirm_delete = false;
                        }
                    });
                    for ci in 0..cols_snap.len() {
                        row.col(|ui| {
                            let is_editing = self.editing.as_ref().is_some_and(|e| e.row == ri && e.col == ci);
                            if is_editing {
                                let ed = self.editing.as_mut().unwrap();
                                let resp = ui.add(TextEdit::singleline(&mut ed.text).desired_width(f32::INFINITY));
                                if !ed.focused {
                                    resp.request_focus();
                                    ed.focused = true;
                                }
                                if ui.input(|i| i.key_pressed(Key::Enter)) {
                                    save_edit = Some((ri, ci, ed.text.clone()));
                                } else if ui.input(|i| i.key_pressed(Key::Escape)) {
                                    cancel_edit = true;
                                }
                            } else if let Some(val) = self.rows.get(ri).and_then(|r| r.get(ci)) {
                                let label = if val.is_null() {
                                    egui::Label::new(RichText::new("null").weak().italics())
                                } else {
                                    egui::Label::new(display(val))
                                };
                                let resp = ui.add(label.truncate().sense(Sense::click()));
                                if editable && ci >= key_cnt && resp.double_clicked() {
                                    start_edit = Some((ri, ci));
                                }
                            }
                        });
                    }
                });
            });

        if let Some(ci) = header_sort {
            self.sort = Some(match self.sort {
                Some((prev, false)) if prev == ci => (ci, true),
                _ => (ci, false),
            });
            self.load(conn);
        }
        if let Some((r, c)) = start_edit {
            let cur = self.rows.get(r).and_then(|row| row.get(c)).map(|v| v.cell_text()).unwrap_or_default();
            self.editing = Some(Edit { row: r, col: c, text: cur, focused: false });
        }
        if cancel_edit {
            self.editing = None;
        }
        if let Some((r, c, txt)) = save_edit {
            self.editing = None;
            match self.commit_edit(conn, r, c, &txt) {
                Ok(()) => {
                    self.load(conn);
                    ev.push(Event::Changed);
                }
                Err(m) => self.error = Some(m),
            }
        }
    }
}
