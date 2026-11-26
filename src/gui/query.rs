//! `q` / `qSQL` Query Pad: code editor (`Ctrl+Enter` / `F5` runs selection or script on background `Session`),
//! `Explain` execution/cluster plan, result grids, console value output, and CSV export.
use crate::session::{Finished, Session};
use crate::ui::{result_grid, Event, ERROR};
use egui::{Key, RichText, TextEdit};
use kixdb::{tools, Event as KEvent, KError, ResultSet, K};
use std::path::PathBuf;

enum OutputItem {
    Rows(ResultSet),
    Value(K),
    Message(String),
    Error(KError),
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Pane {
    Results(usize),
    Console,
    Plan,
}

pub struct QueryTab {
    pub name: Option<String>,
    pub code: String,
    file: Option<PathBuf>,
    items: Vec<OutputItem>,
    pane: Pane,
    elapsed_ms: Option<f64>,
    plan: Vec<String>,
    selection: Option<(usize, usize)>,
    pub running: bool,
}

impl QueryTab {
    pub fn new(name: Option<String>, code: String) -> Self {
        Self {
            name,
            code,
            file: None,
            items: vec![],
            pane: Pane::Console,
            elapsed_ms: None,
            plan: vec![],
            selection: None,
            running: false,
        }
    }

    pub fn title(&self) -> String {
        match (&self.file, &self.name) {
            (Some(f), _) => f.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "Pad.q".into()),
            (None, Some(n)) => n.clone(),
            _ => "Pad.q".into(),
        }
    }

    fn sets(&self) -> Vec<&ResultSet> {
        self.items
            .iter()
            .filter_map(|i| if let OutputItem::Rows(r) = i { Some(r) } else { None })
            .collect()
    }

    pub fn shown_results(&self) -> Option<&ResultSet> {
        match self.pane {
            Pane::Results(i) => self.sets().get(i).copied(),
            _ => self.sets().first().copied(),
        }
    }

    pub fn error_count(&self) -> usize {
        self.items.iter().filter(|i| matches!(i, OutputItem::Error(_))).count()
    }

    fn text_to_run(&self) -> String {
        match self.selection {
            Some((a, b)) if b > a => self.code.chars().skip(a).take(b - a).collect(),
            _ => self.code.clone(),
        }
    }

    pub fn start(&mut self, sess: &mut Session, ctx: &egui::Context) {
        let text = self.text_to_run();
        if text.trim().is_empty() || sess.is_busy() {
            return;
        }
        self.running = true;
        let ctx = ctx.clone();
        sess.start(text, move || ctx.request_repaint());
    }

    pub fn finish(&mut self, fin: Finished, ev: &mut Vec<Event>) {
        self.running = false;
        self.elapsed_ms = Some(fin.elapsed.as_secs_f64() * 1e3);
        self.items.clear();
        self.plan.clear();

        for batch in fin.results {
            for e in batch.events {
                match e {
                    KEvent::Rows(rs) => self.items.push(OutputItem::Rows(rs)),
                    KEvent::Value(v) => self.items.push(OutputItem::Value(v)),
                    KEvent::Message(m) => self.items.push(OutputItem::Message(m)),
                    KEvent::Error(err) => self.items.push(OutputItem::Error(err)),
                }
            }
        }

        self.pane = if self.error_count() > 0 || self.sets().is_empty() {
            Pane::Console
        } else {
            Pane::Results(0)
        };
        ev.push(Event::Changed);
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, sess: &mut Session, ctx: &egui::Context, ev: &mut Vec<Event>) {
        let shortcut = ui.input(|i| i.key_pressed(Key::F5) || (i.modifiers.ctrl && i.key_pressed(Key::Enter)));
        if shortcut && !sess.is_busy() {
            self.start(sess, ctx);
        }

        ui.horizontal(|ui| {
            let has_sel = self.selection.is_some_and(|(a, b)| b > a);
            let run_lbl = if has_sel { "▶ Run Selection (Ctrl+Enter)" } else { "▶ Run (Ctrl+Enter)" };
            if ui.add_enabled(!sess.is_busy(), egui::Button::new(run_lbl)).clicked() {
                self.start(sess, ctx);
            }
            if ui.add_enabled(!sess.is_busy(), egui::Button::new("Explain Plan")).clicked() {
                let txt = self.text_to_run();
                match sess.explain(&txt) {
                    Ok(lines) => {
                        self.plan = lines;
                        self.pane = Pane::Plan;
                    }
                    Err(e) => ev.push(Event::Error(e.to_string())),
                }
            }
            ui.separator();
            if ui.button("Open .q...").clicked() {
                if let Some(p) = rfd::FileDialog::new().add_filter("q script", &["q", "k"]).pick_file() {
                    match std::fs::read_to_string(&p) {
                        Ok(s) => {
                            self.code = s;
                            self.file = Some(p);
                        }
                        Err(e) => ev.push(Event::Error(format!("Cannot open file: {e}"))),
                    }
                }
            }
            if ui.button("Save .q...").clicked() {
                let target = self
                    .file
                    .clone()
                    .or_else(|| rfd::FileDialog::new().add_filter("q script", &["q"]).save_file());
                if let Some(p) = target {
                    match std::fs::write(&p, &self.code) {
                        Ok(()) => {
                            self.file = Some(p.clone());
                            ev.push(Event::Status(format!("Saved {}", p.display())));
                        }
                        Err(e) => ev.push(Event::Error(format!("Cannot save file: {e}"))),
                    }
                }
            }
            if self.shown_results().is_some() && ui.button("Export CSV...").clicked() {
                if let Some(rs) = self.shown_results().cloned() {
                    if let Some(p) = rfd::FileDialog::new().add_filter("CSV", &["csv"]).save_file() {
                        match std::fs::write(&p, tools::to_csv(&rs)) {
                            Ok(()) => ev.push(Event::Status(format!("Exported {} rows to {}", rs.rows.len(), p.display()))),
                            Err(e) => ev.push(Event::Error(format!("Export failed: {e}"))),
                        }
                    }
                }
            }
            if let Some(ms) = self.elapsed_ms {
                ui.label(RichText::new(format!("{ms:.2} ms")).weak());
            }
        });

        ui.separator();

        let avail_h = ui.available_height();
        let editor_h = (avail_h * 0.42).max(120.0);
        egui::ScrollArea::vertical().id_salt(("q_editor_scroll", self.title())).max_height(editor_h).show(ui, |ui| {
            let out = TextEdit::multiline(&mut self.code)
                .code_editor()
                .desired_width(f32::INFINITY)
                .desired_rows(10)
                .hint_text("/ Enter q or qSQL (e.g. select vwap: size wavg price by sym from trade)\n/ Press Ctrl+Enter or F5 to execute")
                .show(ui);
            self.selection = out.cursor_range.map(|cr| {
                let a = cr.primary.index;
                let b = cr.secondary.index;
                (a.min(b), a.max(b))
            });
        });

        ui.separator();

        // Bottom output tabs
        let sets_len = self.sets().len();
        ui.horizontal(|ui| {
            for i in 0..sets_len {
                let nrows = self.sets()[i].rows.len();
                let lbl = if sets_len == 1 {
                    format!("Table ({nrows} rows)")
                } else {
                    format!("Table {} ({nrows} rows)", i + 1)
                };
                if ui.selectable_label(self.pane == Pane::Results(i), lbl).clicked() {
                    self.pane = Pane::Results(i);
                }
            }
            let errs = self.error_count();
            let con_lbl = if errs > 0 {
                RichText::new(format!("Console ({errs} error)")).color(ERROR)
            } else {
                RichText::new("Console")
            };
            if ui.selectable_label(self.pane == Pane::Console, con_lbl).clicked() {
                self.pane = Pane::Console;
            }
            if !self.plan.is_empty() && ui.selectable_label(self.pane == Pane::Plan, "Plan").clicked() {
                self.pane = Pane::Plan;
            }
        });

        ui.separator();

        match self.pane {
            Pane::Results(idx) => {
                if let Some(rs) = self.sets().get(idx) {
                    result_grid(ui, &format!("qtab_res_{idx}"), &rs.columns, rs.key_count, &rs.rows);
                }
            }
            Pane::Console => {
                egui::ScrollArea::vertical().id_salt("qtab_console").show(ui, |ui| {
                    if self.items.is_empty() {
                        ui.label(RichText::new("(no output)").weak());
                    }
                    for item in &self.items {
                        match item {
                            OutputItem::Rows(rs) => {
                                ui.monospace(format!("({} row(s), {} col(s))", rs.rows.len(), rs.columns.len()));
                            }
                            OutputItem::Value(k) => {
                                ui.monospace(kixdb::display::console(k));
                            }
                            OutputItem::Message(m) => {
                                ui.monospace(m);
                            }
                            OutputItem::Error(e) => {
                                ui.colored_label(ERROR, e.to_string());
                            }
                        }
                    }
                });
            }
            Pane::Plan => {
                egui::ScrollArea::vertical().id_salt("qtab_plan").show(ui, |ui| {
                    for line in &self.plan {
                        ui.monospace(line);
                    }
                });
            }
        }
    }
}
