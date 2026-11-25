//! Schema / `meta` Designer view: inspect & modify columns, keys (`xkey`), and attributes (`` `s# ``, `` `u# ``, `` `p# ``, `` `g# ``).
use crate::backend::Backend;
use crate::ui::{Event, ACCENT, ERROR};
use egui::{RichText, TextEdit};
use egui_extras::{Column, TableBuilder};
use kixdb::inspect::TableInfo;
use kixdb::tools;

const COL_TYPES: [(&str, char); 11] = [
    ("symbol (s)", 's'),
    ("long (j)", 'j'),
    ("int (i)", 'i'),
    ("float (f)", 'f'),
    ("boolean (b)", 'b'),
    ("timestamp (p)", 'p'),
    ("date (d)", 'd'),
    ("month (m)", 'm'),
    ("time (t)", 't'),
    ("minute (u)", 'u'),
    ("string (C)", 'C'),
];

const ATTRS: [(&str, &str); 5] = [
    ("none", ""),
    ("sorted (`s#)", "s"),
    ("unique (`u#)", "u"),
    ("parted (`p#)", "p"),
    ("grouped (`g#)", "g"),
];

pub struct Design {
    pub table: String,
    info: Option<TableInfo>,
    dirty: bool,
    new_col_name: String,
    new_col_type: usize,
    error: Option<String>,
}

impl Design {
    pub fn new(table: String) -> Self {
        Self {
            table,
            info: None,
            dirty: true,
            new_col_name: String::new(),
            new_col_type: 0,
            error: None,
        }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn load(&mut self, conn: &mut dyn Backend) {
        self.dirty = false;
        self.info = conn.table_info(&self.table);
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, conn: &mut dyn Backend, ev: &mut Vec<Event>) {
        if self.dirty {
            self.load(conn);
        }
        let Some(info) = self.info.clone() else {
            ui.colored_label(ERROR, format!("Table '{}' does not exist.", self.table));
            return;
        };

        ui.horizontal(|ui| {
            ui.heading(&self.table);
            ui.label(RichText::new(format!("({} · {} rows)", info.kind.label(), info.nrows)).weak());
            if ui.button("Open Datasheet").clicked() {
                ev.push(Event::OpenDatasheet(self.table.clone()));
            }
            if ui.button("Script in Query Pad").clicked() {
                ev.push(Event::OpenQuery {
                    name: Some(format!("{}_schema", self.table)),
                    code: tools::table_script(&info),
                });
            }
        });

        if let Some(e) = &self.error {
            ui.colored_label(ERROR, e);
        }
        ui.separator();

        let mut toggle_key: Option<(String, bool)> = None;
        let mut change_attr: Option<(String, &'static str)> = None;
        let mut drop_col: Option<String> = None;

        TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(Column::initial(150.0).at_least(80.0))
            .column(Column::exact(65.0))
            .column(Column::initial(110.0))
            .column(Column::exact(75.0))
            .column(Column::initial(145.0))
            .column(Column::exact(80.0))
            .header(24.0, |mut h| {
                for title in ["Column (c)", "Char (t)", "Type", "Key", "Attribute (a)", "Action"] {
                    h.col(|ui| {
                        ui.label(RichText::new(title).strong().color(ACCENT));
                    });
                }
            })
            .body(|body| {
                body.rows(26.0, info.columns.len(), |mut r| {
                    let c = &info.columns[r.index()];
                    r.col(|ui| {
                        ui.label(RichText::new(&c.name).strong());
                    });
                    r.col(|ui| {
                        ui.monospace(c.type_char.to_string());
                    });
                    r.col(|ui| {
                        ui.label(&c.type_name);
                    });
                    r.col(|ui| {
                        let mut k = c.is_key;
                        if ui.checkbox(&mut k, "").changed() {
                            toggle_key = Some((c.name.clone(), k));
                        }
                    });
                    r.col(|ui| {
                        let cur_a = c.attr.map(|ch| ch.to_string()).unwrap_or_default();
                        egui::ComboBox::from_id_salt(("attr", &c.name))
                            .selected_text(if cur_a.is_empty() { "none".into() } else { format!("`{cur_a}#") })
                            .width(120.0)
                            .show_ui(ui, |ui| {
                                for (lbl, code) in ATTRS {
                                    if ui.selectable_label(cur_a == code, lbl).clicked() {
                                        change_attr = Some((c.name.clone(), code));
                                    }
                                }
                            });
                    });
                    r.col(|ui| {
                        if info.columns.len() > 1 && ui.small_button("Drop").clicked() {
                            drop_col = Some(c.name.clone());
                        }
                    });
                });
            });

        if let Some((col, make_key)) = toggle_key {
            let mut new_keys: Vec<String> = info.columns.iter().filter(|c| c.is_key).map(|c| c.name.clone()).collect();
            if make_key && !new_keys.contains(&col) {
                new_keys.push(col);
            } else if !make_key {
                new_keys.retain(|x| x != &col);
            }
            let q = if new_keys.is_empty() {
                format!("{0}: 0 ! {0}", self.table)
            } else {
                let syms: String = new_keys.iter().map(|k| format!("`{k}")).collect();
                format!("{0}: {syms} xkey 0 ! {0}", self.table)
            };
            match conn.eval(&q) {
                Ok(_) => {
                    self.error = None;
                    self.load(conn);
                    ev.push(Event::Changed);
                }
                Err(e) => self.error = Some(e.to_string()),
            }
        }

        if let Some((col, attr_code)) = change_attr {
            let q = format!("update `{attr_code}#{col} from `{}", self.table);
            match conn.eval(&q) {
                Ok(_) => {
                    self.error = None;
                    self.load(conn);
                    ev.push(Event::Changed);
                }
                Err(e) => self.error = Some(e.to_string()),
            }
        }

        if let Some(col) = drop_col {
            let q = format!("delete {col} from `{}", self.table);
            match conn.eval(&q) {
                Ok(_) => {
                    self.error = None;
                    self.load(conn);
                    ev.push(Event::Changed);
                }
                Err(e) => self.error = Some(e.to_string()),
            }
        }

        ui.add_space(12.0);
        ui.separator();
        ui.label(RichText::new("Add column").strong());
        ui.horizontal(|ui| {
            ui.label("Name:");
            ui.add(TextEdit::singleline(&mut self.new_col_name).desired_width(140.0));
            ui.label("Type:");
            egui::ComboBox::from_id_salt("new_col_type")
                .selected_text(COL_TYPES[self.new_col_type].0)
                .show_ui(ui, |ui| {
                    for (idx, (lbl, _)) in COL_TYPES.iter().enumerate() {
                        ui.selectable_value(&mut self.new_col_type, idx, *lbl);
                    }
                });
            if ui.button("Add").clicked() && !self.new_col_name.trim().is_empty() {
                let cname = self.new_col_name.trim().to_string();
                let tc = COL_TYPES[self.new_col_type].1;
                let null_lit = tools::q_literal_for_column("", tc).unwrap_or_else(|_| "0N".into());
                let q = format!("update {cname}: {null_lit} from `{}", self.table);
                match conn.eval(&q) {
                    Ok(_) => {
                        self.new_col_name.clear();
                        self.error = None;
                        self.load(conn);
                        ev.push(Event::Changed);
                    }
                    Err(e) => self.error = Some(e.to_string()),
                }
            }
        });
    }
}
