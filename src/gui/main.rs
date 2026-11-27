//! `kix-gui` — desktop application for `kixdb`: browse & edit tables, inspect `meta` & attributes,
//! run `q` / `qSQL` queries, and monitor the workspace or a remote cluster over kdb+ IPC.
//!
//! ```text
//! kix-gui [DATA_DIR] [--embedded]
//! kix-gui --connect HOST[:PORT] [-U USER] [-P PASSWORD]
//! ```
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod backend;
mod datasheet;
mod design;
mod query;
mod remote;
mod sample;
mod session;
mod ui;

fn main() -> Result<(), eframe::Error> {
    kixdb::engine::par::one_thread_a_core();
    let mut dir: Option<std::path::PathBuf> = None;
    let opts = kixdb::StorageOptions::default();
    let mut target: Option<remote::Target> = None;
    let mut embedded = false;
    let mut allow_shell = false;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--connect" | "-S" => {
                let raw = args.next().unwrap_or_default();
                let mut t = target.take().unwrap_or_default();
                let spec = match raw.strip_prefix("tcps://") {
                    Some(rest) => {
                        t.tls = true;
                        rest
                    }
                    None => raw.strip_prefix("tcp://").unwrap_or(&raw),
                };
                match spec.rsplit_once(':') {
                    Some((h, p)) if p.parse::<u16>().is_ok() => {
                        t.host = h.to_string();
                        t.port = p.parse().unwrap_or(5000);
                    }
                    _ if !spec.is_empty() => t.host = spec.to_string(),
                    _ => {
                        eprintln!("--connect needs [tcps://]HOST or HOST:PORT");
                        std::process::exit(2);
                    }
                }
                target = Some(t);
            }
            "--tls" => {
                target.get_or_insert_with(Default::default).tls = true;
            }
            "--tls-ca" => {
                let ca = args.next().unwrap_or_default();
                let t = target.get_or_insert_with(Default::default);
                t.tls = true;
                t.ca_cert = ca;
            }
            "-U" | "-P" => {
                let v = args.next().unwrap_or_default();
                let t = target.get_or_insert_with(Default::default);
                if a == "-U" {
                    t.login = v;
                } else {
                    t.password = v;
                }
            }
            "--embedded" => embedded = true,
            "--allow-shell" => allow_shell = true,
            "-h" | "--help" => {
                println!(
                    "usage: kix-gui [DATA_DIR] [--embedded] [--allow-shell]\n       \
                            kix-gui --connect HOST[:PORT] [-U USER] [-P PASSWORD]\n\n\
                     Without --connect, kix-gui connects to 127.0.0.1:5000 if a kix-serve / kix-cluster\n\
                     server is listening there, or starts an in-process IPC server on port 5000.\n\
                     Use --embedded to run strictly in-process with no TCP port. Its own server runs shell\n\
                     commands only with --allow-shell, as any client then can run programs on this machine."
                );
                return Ok(());
            }
            other if !other.starts_with('-') && dir.is_none() => dir = Some(other.into()),
            other => {
                eprintln!("unknown argument '{other}'");
                std::process::exit(2);
            }
        }
    }

    let mut held: Option<kixdb::ipc::Listener> = None;
    if target.is_none() && !embedded {
        let mut t = remote::Target::default();
        if let Ok(pw) = std::env::var("KIX_PASSWORD") {
            t.password = pw;
        }
        if let Ok(u) = std::env::var("KIX_USER") {
            t.login = u;
        }
        let addr: std::net::SocketAddr = ([127, 0, 0, 1], t.port).into();
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(300)).is_ok() {
            target = Some(t);
        } else if let Ok((listener, pw)) = own_server(dir.as_deref(), opts, t.port, allow_shell) {
            t.password = pw;
            held = Some(listener);
            target = Some(t);
            dir = None;
        }
    }

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1220.0, 780.0])
            .with_min_inner_size([640.0, 420.0])
            .with_title("kixDB"),
        ..Default::default()
    };

    eframe::run_native(
        "kixDB",
        options,
        Box::new(move |cc| {
            let mut app = app::App::new(cc, dir, opts);
            if let Some(l) = held {
                app.hold(l);
            }
            if let Some(t) = target {
                app.connect_or_ask(t);
            }
            Ok(Box::new(app))
        }),
    )
}

fn own_server(
    dir: Option<&std::path::Path>,
    opts: kixdb::StorageOptions,
    port: u16,
    allow_shell: bool,
) -> Result<(kixdb::ipc::Listener, String), String> {
    use rand::Rng;
    let server = match dir {
        Some(d) => kixdb::Server::open_with(d, opts).map_err(|e| e.to_string())?,
        None => kixdb::Server::new(),
    };
    let password: String = rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(24)
        .map(char::from)
        .collect();
    let mut cfg = kixdb::ipc::IpcConfig::with_single_user("admin", &password);
    cfg.allow_shell = allow_shell;
    let listener = kixdb::ipc::Listener::bind(server, ("127.0.0.1", port), cfg).map_err(|e| e.to_string())?;
    Ok((listener, password))
}

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;

    #[test]
    fn own_ipc_server_accepts_generated_credentials_and_rejects_bad_password() {
        let (listener, password) = own_server(None, kixdb::StorageOptions::default(), 0, false).expect("start own IPC server");
        let mut t = remote::Target {
            port: listener.local_addr().port(),
            password,
            ..Default::default()
        };
        let mut b = remote::RemoteBackend::connect(&t).expect("connect with generated password");
        assert_eq!(b.eval("sum 1 + til 5").unwrap().to_string(), "15");

        t.password = "wrong".into();
        assert!(remote::RemoteBackend::connect(&t).is_err());
    }

    #[test]
    fn headless_gui_drives_sample_workspace_datasheet_and_remote_ipc() {
        let (listener, password) = own_server(None, kixdb::StorageOptions::default(), 0, false).unwrap();
        let target = remote::Target {
            port: listener.local_addr().port(),
            password,
            ..Default::default()
        };

        let ctx = egui::Context::default();
        let mut app = app::App::with_context(&ctx, None, kixdb::StorageOptions::default());
        app.connect_or_ask(target);
        assert!(app.run_q(sample::SAMPLE_Q, "loaded sample"));

        // Drive a full headless egui frame
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            // Test Datasheet loading, filtering, sorting, and keyed editing over RemoteBackend!
            let mut ds = datasheet::Datasheet::new("instrument".into());
            let mut ev = Vec::new();
            ds.load(&mut **app.conn_for_test());
            assert_eq!(ds.rows.len(), 5);
            assert!(ds.editable());
            ds.commit_edit(&mut **app.conn_for_test(), 0, 3, "250").unwrap();
            ds.load(&mut **app.conn_for_test());
            assert_eq!(ds.rows[0][3].to_string(), "250");

            let mut design_view = design::Design::new("trade".into());
            egui::CentralPanel::default().show(ctx, |ui| {
                ds.ui(ui, &mut **app.conn_for_test(), &mut ev);
                design_view.ui(ui, &mut **app.conn_for_test(), &mut ev);
            });
        });

        // Verify RemoteBackend explain, non-evaluating view definition, and user namespaces!
        app.conn_for_test().eval("hits: 0; lazy_v:: (hits:: hits + 1; 99); .ops.limit: 5").unwrap();
        let def = app.conn_for_test().definition("lazy_v").unwrap();
        assert!(def.starts_with("lazy_v::"), "got {def}");
        assert_eq!(app.conn_for_test().eval("hits").unwrap().to_string(), "0");
        assert!(app.conn_for_test().namespaces().contains(&".ops".to_string()));
        let plan = app.conn_for_test().explain("select from trade where sym = `AAPL").unwrap();
        assert!(plan[0].contains("qSQL SELECT"), "{plan:?}");
    }
}
