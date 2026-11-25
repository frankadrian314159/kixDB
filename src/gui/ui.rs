//! Shared `egui` palette, view events, and virtualized columnar result grid.
use egui::{Color32, RichText};
use egui_extras::{Column, TableBuilder};
use kixdb::K;

pub const ACCENT: Color32 = Color32::from_rgb(24, 98, 168);
pub const KEY_HEADER: Color32 = Color32::from_rgb(16, 74, 130);
pub const ERROR: Color32 = Color32::from_rgb(180, 30, 30);

pub enum Event {
    Changed,
    Error(String),
    Status(String),
    OpenDatasheet(String),
    OpenDesign(String),
    OpenQuery { name: Option<String>, code: String },
}

pub fn display(v: &K) -> String {
    v.cell_text().replace(['\r', '\n'], " ")
}

/// Read-only virtualized grid of `K` cells (highlights key columns when `key_count > 0`).
pub fn result_grid(ui: &mut egui::Ui, id: &str, columns: &[String], key_count: usize, rows: &[Vec<K>]) {
    ui.push_id(id, |ui| {
        TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .columns(Column::initial(125.0).at_least(50.0).clip(true), columns.len().max(1))
            .header(24.0, |mut h| {
                for (i, c) in columns.iter().enumerate() {
                    h.col(|ui| {
                        let label = if c.is_empty() { "(col)" } else { c.as_str() };
                        if i < key_count {
                            ui.label(RichText::new(format!("🔑 {label}")).strong().color(KEY_HEADER));
                        } else {
                            ui.label(RichText::new(label).strong());
                        }
                    });
                }
            })
            .body(|body| {
                body.rows(22.0, rows.len(), |mut row| {
                    let r = &rows[row.index()];
                    for (ci, v) in r.iter().enumerate() {
                        row.col(|ui| {
                            if v.is_null() {
                                ui.add(egui::Label::new(RichText::new("null").weak().italics()).truncate());
                            } else {
                                let txt = display(v);
                                if ci < key_count {
                                    ui.add(egui::Label::new(RichText::new(txt).strong()).truncate());
                                } else {
                                    ui.add(egui::Label::new(txt).truncate());
                                }
                            }
                        });
                    }
                });
            });
    });
}
