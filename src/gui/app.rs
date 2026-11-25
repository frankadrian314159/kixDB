//! Main `kix-gui` application shell: menu bar, workspace navigation pane, tabs, dialogs, and status bar.
use crate::datasheet::Datasheet;
use crate::design::Design;
use crate::query::QueryTab;
use crate::remote::{RemoteBackend, Target};
use crate::sample::SAMPLE_Q;
use crate::session::Session;
use crate::ui::{result_grid, Event, ACCENT, ERROR};
use egui::{Key, RichText};
use kixdb::inspect::{ObjectEntry, ObjectKind};
use kixdb::{tools, Server, StorageOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

enum Tab {
    Datasheet(Datasheet),
    Design(Design),
    Query(QueryTab),
}

impl Tab {
    fn title(&self) -> String {
        match self {
            Tab::Datasheet(d) => d.table.clone(),
            Tab::Design(d) => format!("{} (Meta)", d.table),
            Tab::Query(q) => q.title(),
        }
    }

    fn key(&self) -> Option<String> {
        match self {
            Tab::Datasheet(d) => Some(format!("ds:{}", d.table)),
            Tab::Design(d) => Some(format!("meta:{}", d.table)),
            Tab::Query(_) => None,
        }
    }

    fn mark_dirty(&mut self) {
        match self {
            Tab::Datasheet(d) => d.mark_dirty(),
            Tab::Design(d) => d.mark_dirty(),
            Tab::Query(_) => {}
        }
    }
}

struct ConnectDialog {
    target: Target,
    port: String,
    error: Option<String>,
}

struct ImportDialog {
    file: String,
    text: String,
    table_name: String,
    create_new: bool,
    preview: Vec<Vec<String>>,
    error: Option<String>,
}

const DELIMITERS: [(&str, char); 4] = [("comma (,)", ','), ("tab (\\t)", '\t'), ("semicolon (;)", ';'), ("pipe (|)", '|')];

struct BulkDialog {
    path: String,
    table_name: String,
    delimiter: usize,
    has_header: bool,
    create_new: bool,
    batch_size: String,
    error: Option<String>,
}

struct BackgroundTask {
    label: String,
    progress: Option<Arc<AtomicU64>>,
    cancel: Arc<AtomicBool>,
}

pub struct App {
    held: Vec<Box<dyn std::any::Any>>,
    server: Server,
    remote: Option<String>,
    connect_dlg: Option<ConnectDialog>,
    import_dlg: Option<ImportDialog>,
    bulk_dlg: Option<BulkDialog>,
    bg_task: Option<BackgroundTask>,
    confirm_drop: Option<ObjectEntry>,
    conn: Session,
    dir: Option<PathBuf>,
    opts: StorageOptions,
    tabs: Vec<Tab>,
    active: usize,
    status: String,
    error: Option<String>,
    objects: Vec<ObjectEntry>,
    namespaces: Vec<String>,
    current_ns: String,
    nav_filter: String,
    storage_window: bool,
    about: bool,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, dir: Option<PathBuf>, opts: StorageOptions) -> Self {
        Self::with_context(&cc.egui_ctx, dir, opts)
    }

    /// Construct `App` against any `egui::Context` so headless tests can drive every view without opening an OS window.
    pub fn with_context(ctx: &egui::Context, dir: Option<PathBuf>, opts: StorageOptions) -> Self {
        let mut visuals = egui::Visuals::light();
        visuals.selection.bg_fill = ACCENT;
        visuals.selection.stroke.color = egui::Color32::WHITE;
        visuals.hyperlink_color = ACCENT;
        ctx.set_visuals(visuals);

        let server = Server::new();
        let conn = Session::new(server.connect());
        let mut app = Self {
            held: vec![],
            server,
            remote: None,
            connect_dlg: None,
            import_dlg: None,
            bulk_dlg: None,
            bg_task: None,
            confirm_drop: None,
            conn,
            dir: None,
            opts,
            tabs: vec![Tab::Query(QueryTab::new(None, String::new()))],
            active: 0,
            status: "Ready. Load the sample tick database from the Workspace menu or open a query pad.".into(),
            error: None,
            objects: vec![],
            namespaces: vec![".".into()],
            current_ns: ".".into(),
            nav_filter: String::new(),
            storage_window: false,
            about: false,
        };
        if let Some(d) = dir {
            app.open_dir(Some(d));
        }
        app.refresh_nav();
        app
    }

    pub fn hold(&mut self, item: impl std::any::Any) {
        self.held.push(Box::new(item));
    }

    #[cfg(test)]
    pub fn conn_for_test(&mut self) -> &mut Session {
        &mut self.conn
    }

    pub fn open_dir(&mut self, dir: Option<PathBuf>) {
        self.tabs = vec![Tab::Query(QueryTab::new(None, String::new()))];
        self.active = 0;
        self.server = Server::new();
        self.conn.replace(self.server.connect());
        self.remote = None;
        self.dir = None;

        match dir {
            None => {
                self.status = "Started new in-memory workspace".into();
                self.error = None;
            }
            Some(d) => match Server::open_with(&d, self.opts) {
                Ok(s) => {
                    self.conn.replace(s.connect());
                    self.server = s;
                    self.status = format!("Opened workspace {}", d.display());
                    self.dir = Some(d);
                    self.error = None;
                }
                Err(e) => {
                    self.error = Some(format!("Could not open '{}': {e}", d.display()));
                }
            },
        }
        self.refresh_nav();
    }

    pub fn connect_or_ask(&mut self, target: Target) {
        let addr = target.addr();
        let port = target.port.to_string();
        match RemoteBackend::connect(&target) {
            Ok(b) => {
                self.tabs = vec![Tab::Query(QueryTab::new(None, String::new()))];
                self.active = 0;
                self.server = Server::new();
                self.conn.replace(b);
                self.dir = None;
                self.remote = Some(addr.clone());
                self.status = format!("Connected to {addr} (kdb+ IPC v3)");
                self.error = None;
                self.refresh_nav();
            }
            Err(e) => {
                self.connect_dlg = Some(ConnectDialog {
                    target,
                    port,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    pub fn refresh_nav(&mut self) {
        if self.conn.is_busy() {
            return;
        }
        self.objects = self.conn.objects();
        self.namespaces = self.conn.namespaces();
        self.current_ns = self.conn.current_namespace();
        for t in &mut self.tabs {
            t.mark_dirty();
        }
    }

    pub fn run_q(&mut self, code: &str, ok_status: &str) -> bool {
        match self.conn.eval(code) {
            Ok(_) => {
                self.status = ok_status.into();
                self.error = None;
                self.refresh_nav();
                true
            }
            Err(e) => {
                self.error = Some(e.to_string());
                false
            }
        }
    }

    fn open_or_focus(&mut self, tab: Tab) {
        if let Some(k) = tab.key() {
            if let Some(idx) = self.tabs.iter().position(|t| t.key().as_deref() == Some(&k)) {
                self.active = idx;
                return;
            }
        }
        self.tabs.push(tab);
        self.active = self.tabs.len() - 1;
    }

    pub fn handle_events(&mut self, evs: Vec<Event>) {
        for e in evs {
            match e {
                Event::Changed => self.refresh_nav(),
                Event::Error(m) => self.error = Some(m),
                Event::Status(s) => {
                    self.status = s;
                    self.error = None;
                }
                Event::OpenDatasheet(t) => self.open_or_focus(Tab::Datasheet(Datasheet::new(t))),
                Event::OpenDesign(t) => self.open_or_focus(Tab::Design(Design::new(t))),
                Event::OpenQuery { name, code } => self.open_or_focus(Tab::Query(QueryTab::new(name, code))),
            }
        }
    }

    fn export_active_grid(&mut self) {
        let csv_opt: Option<(String, usize)> = match self.tabs.get(self.active) {
            Some(Tab::Datasheet(d)) => {
                let rs = kixdb::ResultSet {
                    columns: d.columns.clone(),
                    types: vec![],
                    key_count: d.key_count,
                    rows: d.rows.clone(),
                };
                Some((tools::to_csv(&rs), rs.rows.len()))
            }
            Some(Tab::Query(q)) => q.shown_results().map(|rs| (tools::to_csv(rs), rs.rows.len())),
            _ => None,
        };
        let Some((csv, nrows)) = csv_opt else {
            self.error = Some("Open a Datasheet or Table result first to export CSV.".into());
            return;
        };
        if let Some(p) = rfd::FileDialog::new().add_filter("CSV", &["csv"]).save_file() {
            match std::fs::write(&p, csv) {
                Ok(()) => self.status = format!("Exported {nrows} rows to {}", p.display()),
                Err(e) => self.error = Some(format!("Cannot write CSV: {e}")),
            }
        }
    }

    fn menu_bar(&mut self, ctx: &egui::Context, ev: &mut Vec<Event>) {
        let busy = self.conn.is_busy();
        egui::TopBottomPanel::top("menu").show(ctx, |ui| {
            ui.add_enabled_ui(!busy, |ui| {
                egui::MenuBar::new().ui(ui, |ui| {
                    ui.menu_button("File", |ui| {
                        if ui.button("Open HDB / workspace folder...").clicked() {
                            ui.close();
                            if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                self.open_dir(Some(p));
                            }
                        }
                        if ui.button("New in-memory workspace").clicked() {
                            ui.close();
                            self.open_dir(None);
                        }
                        if ui
                            .button("Connect to IPC server...")
                            .on_hover_text("Connect to kix-serve, kix-cluster, or kdb+ over binary IPC (default port 5000)")
                            .clicked()
                        {
                            ui.close();
                            self.connect_dlg = Some(ConnectDialog {
                                target: Target::default(),
                                port: "5000".into(),
                                error: None,
                            });
                        }
                        ui.separator();
                        if ui.button("Import CSV...").clicked() {
                            ui.close();
                            if let Some(p) = rfd::FileDialog::new().add_filter("CSV", &["csv", "txt"]).pick_file() {
                                if let Ok(text) = std::fs::read_to_string(&p) {
                                    let stem = p
                                        .file_stem()
                                        .map(|s| tools::sanitize_ident(&s.to_string_lossy(), 0))
                                        .unwrap_or_else(|| "imported".into());
                                    let preview = tools::parse_csv(&text).unwrap_or_default().into_iter().take(8).collect();
                                    self.import_dlg = Some(ImportDialog {
                                        file: p.display().to_string(),
                                        text,
                                        table_name: stem,
                                        create_new: true,
                                        preview,
                                        error: None,
                                    });
                                }
                            }
                        }
                        if ui.button("Bulk import delimited file...").clicked() {
                            ui.close();
                            self.bulk_dlg = Some(BulkDialog {
                                path: String::new(),
                                table_name: "imported".into(),
                                delimiter: 0,
                                has_header: true,
                                create_new: true,
                                batch_size: "1000".into(),
                                error: None,
                            });
                        }
                        if ui.button("Export active grid to CSV...").clicked() {
                            ui.close();
                            self.export_active_grid();
                        }
                        ui.separator();
                        if ui.button("Exit").clicked() {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                    });

                    ui.menu_button("Create", |ui| {
                        if ui.button("Query Pad  (Ctrl+N)").clicked() {
                            ui.close();
                            ev.push(Event::OpenQuery { name: None, code: String::new() });
                        }
                        if ui.button("Table").clicked() {
                            ui.close();
                            ev.push(Event::OpenQuery {
                                name: None,
                                code: "ticks: ([] time: `time$(); sym: `symbol$(); price: `float$(); size: `long$())".into(),
                            });
                        }
                        if ui.button("Keyed Table").clicked() {
                            ui.close();
                            ev.push(Event::OpenQuery {
                                name: None,
                                code: "ref: ([sym: `AAPL`MSFT] sector: `Tech`Tech; lot: 100 100)".into(),
                            });
                        }
                        if ui.button("View (::)").clicked() {
                            ui.close();
                            ev.push(Event::OpenQuery {
                                name: None,
                                code: "summary:: select count i, vwap: size wavg price by sym from trade".into(),
                            });
                        }
                        if ui.button("Function").clicked() {
                            ui.close();
                            ev.push(Event::OpenQuery {
                                name: None,
                                code: "vwap_for: {[s] exec size wavg price from trade where sym = s}".into(),
                            });
                        }
                    });

                    ui.menu_button("Workspace", |ui| {
                        if ui.button("Load sample tick database").clicked() {
                            ui.close();
                            self.run_q(SAMPLE_Q, "Loaded sample tick database (instrument, trade, quote, vwap_summary, ohlc5m)");
                        }
                        if ui.button("Refresh workspace").clicked() {
                            ui.close();
                            self.refresh_nav();
                        }
                        ui.separator();
                        if ui.button("Checkpoint / Save snapshot").clicked() {
                            ui.close();
                            if self.remote.is_none() {
                                match self.server.checkpoint() {
                                    Ok(()) => self.status = "Checkpoint written".into(),
                                    Err(e) => self.error = Some(e.to_string()),
                                }
                            } else {
                                self.status = "Connected to remote server (managed by server)".into();
                            }
                        }
                        if ui.button("Memory & storage statistics (\\w)...").clicked() {
                            ui.close();
                            self.storage_window = true;
                        }
                    });

                    ui.menu_button("Help", |ui| {
                        if ui.button("About kixDB").clicked() {
                            ui.close();
                            self.about = true;
                        }
                    });
                });
            });
        });
    }

    fn nav_pane(&mut self, ctx: &egui::Context, ev: &mut Vec<Event>) {
        let busy = self.conn.is_busy();
        egui::SidePanel::left("nav_panel").resizable(true).default_width(245.0).show(ctx, |ui| {
            if busy {
                ui.disable();
            }
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("Namespace").strong().color(ACCENT));
                let mut switch_ns: Option<String> = None;
                egui::ComboBox::from_id_salt("ns_combo").selected_text(&self.current_ns).show_ui(ui, |ui| {
                    for ns in &self.namespaces {
                        if ui.selectable_label(*ns == self.current_ns, ns).clicked() {
                            switch_ns = Some(ns.clone());
                        }
                    }
                });
                if let Some(ns) = switch_ns {
                    self.run_q(&format!("\\d {ns}"), &format!("Switched to namespace {ns}"));
                }
            });

            ui.add(
                egui::TextEdit::singleline(&mut self.nav_filter)
                    .hint_text("filter workspace...")
                    .desired_width(f32::INFINITY),
            );
            ui.separator();

            let filt = self.nav_filter.to_lowercase();
            let filtered: Vec<ObjectEntry> = self
                .objects
                .iter()
                .filter(|o| filt.is_empty() || o.name.to_lowercase().contains(&filt))
                .cloned()
                .collect();

            egui::ScrollArea::vertical().show(ui, |ui| {
                let sections = [
                    ("Tables", ObjectKind::Table, true),
                    ("Keyed Tables", ObjectKind::KeyedTable, true),
                    ("Partitioned HDB", ObjectKind::PartitionedTable, true),
                    ("Views (::)", ObjectKind::View, true),
                    ("Functions", ObjectKind::Function, false),
                    ("Dictionaries", ObjectKind::Dictionary, false),
                    ("Variables", ObjectKind::Variable, false),
                ];

                for (title, kind, open_default) in sections {
                    let items: Vec<&ObjectEntry> = filtered.iter().filter(|o| o.kind == kind).collect();
                    egui::CollapsingHeader::new(RichText::new(format!("{title} ({})", items.len())).strong())
                        .default_open(open_default)
                        .show(ui, |ui| {
                            if items.is_empty() {
                                ui.label(RichText::new("(none)").weak());
                            }
                            for o in items {
                                let full = o.full_name();
                                let resp = ui.selectable_label(false, &full);
                                if resp.double_clicked() {
                                    if kind.is_tabular() || kind == ObjectKind::View {
                                        ev.push(Event::OpenDatasheet(full.clone()));
                                    } else if let Some(def) = self.conn.definition(&full) {
                                        ev.push(Event::OpenQuery { name: Some(o.name.clone()), code: def });
                                    }
                                }
                                resp.context_menu(|ui| {
                                    if kind.is_tabular() || kind == ObjectKind::View {
                                        if ui.button("Open Datasheet").clicked() {
                                            ev.push(Event::OpenDatasheet(full.clone()));
                                            ui.close();
                                        }
                                        if ui.button("Select 100 in Pad").clicked() {
                                            ev.push(Event::OpenQuery {
                                                name: None,
                                                code: format!("100 sublist select from {full}"),
                                            });
                                            ui.close();
                                        }
                                    }
                                    if matches!(kind, ObjectKind::Table | ObjectKind::KeyedTable) && ui.button("Meta / Design").clicked() {
                                        ev.push(Event::OpenDesign(full.clone()));
                                        ui.close();
                                    }
                                    if ui.button("Show Definition").clicked() {
                                        if let Some(def) = self.conn.definition(&full) {
                                            ev.push(Event::OpenQuery { name: Some(o.name.clone()), code: def });
                                        }
                                        ui.close();
                                    }
                                    if ui.button("Delete...").clicked() {
                                        self.confirm_drop = Some(o.clone());
                                        ui.close();
                                    }
                                });
                            }
                        });
                }
            });
        });
    }

    fn dialogs(&mut self, ctx: &egui::Context, ev: &mut Vec<Event>) {
        // Connect to server dialog
        if self.connect_dlg.is_some() {
            let mut open = true;
            let mut do_connect = false;
            egui::Window::new("Connect to kdb+ / kixDB IPC Server")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    if let Some(d) = &mut self.connect_dlg {
                        egui::Grid::new("conn_grid").num_columns(2).spacing([8.0, 6.0]).show(ui, |ui| {
                            ui.label("Host:");
                            ui.text_edit_singleline(&mut d.target.host);
                            ui.end_row();
                            ui.label("Port:");
                            ui.text_edit_singleline(&mut d.port);
                            ui.end_row();
                            ui.label("User:");
                            ui.text_edit_singleline(&mut d.target.login);
                            ui.end_row();
                            ui.label("Password:");
                            ui.add(egui::TextEdit::singleline(&mut d.target.password).password(true));
                            ui.end_row();
                            ui.label("TLS:");
                            ui.checkbox(&mut d.target.tls, "Encrypt with TLS (tcps://)");
                            ui.end_row();
                            if d.target.tls {
                                ui.label("CA cert:");
                                ui.text_edit_singleline(&mut d.target.ca_cert);
                                ui.end_row();
                            }
                        });
                        if let Some(err) = &d.error {
                            ui.colored_label(ERROR, err);
                        }
                        ui.add_space(6.0);
                        ui.horizontal(|ui| {
                            if ui.button("Connect").clicked() {
                                do_connect = true;
                            }
                        });
                    }
                });
            if do_connect {
                let mut dlg = self.connect_dlg.take().unwrap();
                match dlg.port.trim().parse::<u16>() {
                    Ok(p) => {
                        dlg.target.port = p;
                        self.connect_or_ask(dlg.target);
                    }
                    Err(_) => {
                        dlg.error = Some("Port must be an integer between 1 and 65535".into());
                        self.connect_dlg = Some(dlg);
                    }
                }
            } else if !open {
                self.connect_dlg = None;
            }
        }

        // CSV Import dialog
        if self.import_dlg.is_some() {
            let mut open = true;
            let mut run_import = false;
            egui::Window::new("Import CSV").open(&mut open).collapsible(false).show(ctx, |ui| {
                if let Some(d) = &mut self.import_dlg {
                    ui.label(RichText::new(&d.file).weak());
                    ui.horizontal(|ui| {
                        ui.label("Target table:");
                        ui.text_edit_singleline(&mut d.table_name);
                        ui.checkbox(&mut d.create_new, "Create new table (infer column types)");
                    });
                    if !d.preview.is_empty() {
                        ui.separator();
                        ui.label(RichText::new("Preview:").strong());
                        let cols = d.preview[0].clone();
                        let rows: Vec<Vec<kixdb::K>> = d.preview[1..]
                            .iter()
                            .map(|r| r.iter().map(|s| kixdb::K::string(s)).collect())
                            .collect();
                        ui.set_max_height(160.0);
                        result_grid(ui, "csv_prev", &cols, 0, &rows);
                    }
                    if let Some(err) = &d.error {
                        ui.colored_label(ERROR, err);
                    }
                    if ui.button("Import").clicked() {
                        run_import = true;
                    }
                }
            });
            if run_import {
                let mut d = self.import_dlg.take().unwrap();
                match tools::import_csv(&mut *self.conn, &d.text, d.table_name.trim(), d.create_new) {
                    Ok(n) => {
                        self.status = format!("Imported {n} rows into {}", d.table_name.trim());
                        ev.push(Event::Changed);
                        ev.push(Event::OpenDatasheet(d.table_name));
                    }
                    Err(e) => {
                        d.error = Some(e.to_string());
                        self.import_dlg = Some(d);
                    }
                }
            } else if !open {
                self.import_dlg = None;
            }
        }

        // Streaming Bulk Import dialog
        if self.bulk_dlg.is_some() {
            let mut open = true;
            let mut start_bulk = false;
            egui::Window::new("Bulk Import Delimited File").open(&mut open).collapsible(false).show(ctx, |ui| {
                if let Some(d) = &mut self.bulk_dlg {
                    ui.horizontal(|ui| {
                        ui.label("File:");
                        ui.text_edit_singleline(&mut d.path);
                        if ui.button("Browse...").clicked() {
                            if let Some(p) = rfd::FileDialog::new().pick_file() {
                                d.path = p.display().to_string();
                            }
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("Table:");
                        ui.text_edit_singleline(&mut d.table_name);
                        ui.checkbox(&mut d.create_new, "Create new");
                        ui.checkbox(&mut d.has_header, "Header row");
                    });
                    ui.horizontal(|ui| {
                        ui.label("Delimiter:");
                        egui::ComboBox::from_id_salt("bulk_delim")
                            .selected_text(DELIMITERS[d.delimiter].0)
                            .show_ui(ui, |ui| {
                                for (i, (lbl, _)) in DELIMITERS.iter().enumerate() {
                                    ui.selectable_value(&mut d.delimiter, i, *lbl);
                                }
                            });
                        ui.label("Batch rows:");
                        ui.add(egui::TextEdit::singleline(&mut d.batch_size).desired_width(70.0));
                    });
                    if let Some(err) = &d.error {
                        ui.colored_label(ERROR, err);
                    }
                    if ui.button("Start Import").clicked() {
                        start_bulk = true;
                    }
                }
            });
            if start_bulk {
                let d = self.bulk_dlg.take().unwrap();
                let path = PathBuf::from(d.path.trim());
                let tbl = d.table_name.trim().to_string();
                let delim = DELIMITERS[d.delimiter].1;
                let has_hdr = d.has_header;
                let create = d.create_new;
                let batch = d.batch_size.parse::<usize>().unwrap_or(1000).max(1);
                let prog = Arc::new(AtomicU64::new(0));
                let cancel = Arc::new(AtomicBool::new(false));
                let prog_bg = prog.clone();
                let cancel_bg = cancel.clone();
                let ctx_bg = ctx.clone();

                self.bg_task = Some(BackgroundTask {
                    label: format!("Bulk importing into {tbl}"),
                    progress: Some(prog),
                    cancel,
                });
                self.conn.start_with(
                    move |c| {
                        let res = match tools::bulk_import(c, &path, &tbl, delim, has_hdr, create, batch, Some(&prog_bg), Some(&cancel_bg)) {
                            Ok(n) => kixdb::BatchResult {
                                events: vec![kixdb::Event::Message(format!("Bulk imported {n} rows into {tbl}"))],
                            },
                            Err(e) => kixdb::BatchResult { events: vec![kixdb::Event::Error(e)] },
                        };
                        vec![res]
                    },
                    move || ctx_bg.request_repaint(),
                );
            } else if !open {
                self.bulk_dlg = None;
            }
        }

        // Drop confirmation dialog
        if let Some(obj) = self.confirm_drop.clone() {
            let mut open = true;
            egui::Window::new("Delete Object").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.label(format!("Delete {} '{}' from workspace?", obj.kind.label(), obj.full_name()));
                ui.horizontal(|ui| {
                    if ui.button("Delete").clicked() {
                        let ns = if obj.namespace == "." || obj.namespace.is_empty() { "." } else { &obj.namespace };
                        let _ = self.run_q(&format!("delete {} from `{ns}", obj.name), &format!("Deleted {}", obj.full_name()));
                        self.confirm_drop = None;
                    }
                    if ui.button("Cancel").clicked() {
                        self.confirm_drop = None;
                    }
                });
            });
            if !open {
                self.confirm_drop = None;
            }
        }

        // Storage & memory stats window
        if self.storage_window {
            let mut open = true;
            egui::Window::new("Workspace & Storage Statistics (\\w)").open(&mut open).resizable(false).show(ctx, |ui| {
                if self.remote.is_none() {
                    let st = self.server.storage_stats();
                    egui::Grid::new("st_grid").num_columns(2).spacing([14.0, 6.0]).show(ui, |ui| {
                        ui.label("Columnar heap bytes:");
                        ui.monospace(format!("{} B", st.memory_bytes));
                        ui.end_row();
                        ui.label("Interned symbols:");
                        ui.monospace(st.sym_count.to_string());
                        ui.end_row();
                        ui.label("Tables / Views:");
                        ui.monospace(format!("{} / {}", st.table_count, st.view_count));
                        ui.end_row();
                        ui.label("WAL records / size:");
                        ui.monospace(format!("{} ({} B)", st.wal_records, st.wal_bytes));
                        ui.end_row();
                        ui.label("Snapshot size:");
                        ui.monospace(format!("{} B", st.snap_bytes));
                        ui.end_row();
                        ui.label("Data directory:");
                        ui.monospace(st.data_dir.as_deref().unwrap_or("(in-memory)"));
                        ui.end_row();
                    });
                } else {
                    ui.label("Connected over kdb+ IPC; run `\\w` in a Query Pad to inspect remote workspace memory.");
                }
            });
            self.storage_window = open;
        }

        if self.about {
            let mut open = true;
            egui::Window::new("About kixDB").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.heading("kixDB 0.1");
                ui.label("Clean-room Rust clone of KX's kdb+ columnstore and the q query language.");
                ui.label("Supports columnar vectors, attributes (`s# `u# `p# `g#), qSQL, as-of & window joins (aj/wj),");
                ui.label("splayed & partitioned HDBs, native kdb+ IPC v3, and distributed Raft + Arrow Flight clusters.");
            });
            self.about = open;
        }
    }

    fn status_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                match (&self.remote, &self.dir) {
                    (Some(addr), _) => ui.label(RichText::new(format!("IPC {addr}")).strong().color(ACCENT)),
                    (None, Some(d)) => ui.label(RichText::new(d.display().to_string()).weak()),
                    (None, None) => ui.label(RichText::new("embedded in-memory").weak()),
                };
                ui.separator();
                ui.label(RichText::new(format!("user: {}  ·  ns: {}", self.conn.login(), self.current_ns)).weak());
                ui.separator();
                if let Some(dur) = self.conn.running_for() {
                    if let Some(task) = &self.bg_task {
                        let rows = task.progress.as_ref().map(|p| format!(" ({} rows)", p.load(Ordering::Relaxed))).unwrap_or_default();
                        ui.label(RichText::new(format!("{}{} — {:.1}s", task.label, rows, dur.as_secs_f64())).color(ACCENT));
                        if ui.small_button("Stop").clicked() {
                            task.cancel.store(true, Ordering::Relaxed);
                        }
                    } else {
                        ui.label(RichText::new(format!("Running query ({:.1}s)...", dur.as_secs_f64())).color(ACCENT));
                    }
                    ctx.request_repaint();
                } else {
                    ui.label(&self.status);
                }
            });
            if let Some(err) = self.error.clone() {
                ui.horizontal(|ui| {
                    ui.colored_label(ERROR, err);
                    if ui.small_button("dismiss").clicked() {
                        self.error = None;
                    }
                });
            }
        });
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut ev = Vec::new();

        // Poll finished worker job
        if let Some(fin) = self.conn.poll() {
            if self.bg_task.take().is_some() {
                for b in &fin.results {
                    for e in &b.events {
                        match e {
                            kixdb::Event::Message(m) => self.status = m.clone(),
                            kixdb::Event::Error(err) => self.error = Some(err.to_string()),
                            _ => {}
                        }
                    }
                }
                ev.push(Event::Changed);
            } else if let Some(Tab::Query(q)) = self.tabs.iter_mut().find(|t| matches!(t, Tab::Query(q) if q.running)) {
                q.finish(fin, &mut ev);
            }
        }

        if !self.conn.is_busy() && ctx.input(|i| i.modifiers.ctrl && i.key_pressed(Key::N)) {
            ev.push(Event::OpenQuery { name: None, code: String::new() });
        }

        self.menu_bar(ctx, &mut ev);
        self.status_bar(ctx);
        self.nav_pane(ctx, &mut ev);
        self.dialogs(ctx, &mut ev);

        egui::CentralPanel::default().show(ctx, |ui| {
            let mut close_idx = None;
            ui.horizontal_wrapped(|ui| {
                if self.conn.is_busy() {
                    ui.disable();
                }
                for (i, t) in self.tabs.iter().enumerate() {
                    if ui.selectable_label(self.active == i, t.title()).clicked() {
                        self.active = i;
                    }
                    if ui.small_button("×").on_hover_text("Close tab").clicked() {
                        close_idx = Some(i);
                    }
                    ui.add_space(6.0);
                }
                if ui.small_button("+ Pad").clicked() {
                    ev.push(Event::OpenQuery { name: None, code: String::new() });
                }
            });
            if let Some(ci) = close_idx {
                self.tabs.remove(ci);
                self.active = self.active.min(self.tabs.len().saturating_sub(1));
            }
            ui.separator();

            if let Some(tab) = self.tabs.get_mut(self.active) {
                match tab {
                    Tab::Datasheet(ds) => {
                        if !self.conn.is_busy() {
                            ds.ui(ui, &mut *self.conn, &mut ev);
                        }
                    }
                    Tab::Design(dg) => {
                        if !self.conn.is_busy() {
                            dg.ui(ui, &mut *self.conn, &mut ev);
                        }
                    }
                    Tab::Query(qt) => qt.ui(ui, &mut self.conn, ctx, &mut ev),
                }
            }
        });

        if !ev.is_empty() {
            self.handle_events(ev);
        }
    }
}
