//! The values of the `.z` namespace about the process, the command line and the connections.
use crate::engine::files::Handle;
use crate::engine::{namespaces, Engine};
use crate::sym::intern;
use crate::types::*;
use chrono::{Datelike, Local, NaiveDateTime, Timelike, Utc};
use std::sync::Arc;

/// Options whose values are not shown in `.z.X`: they hold passwords.
const SECRET: [&str; 3] = ["-u", "--auth", "-P"];

fn strings(items: &[String]) -> K {
    K::Vector(ColVec::General(Attr::None, Arc::new(items.iter().map(|s| K::string(s)).collect())))
}

/// The nanoseconds into the day.
fn timespan(t: NaiveDateTime) -> K {
    K::Timespan(i64::from(t.num_seconds_from_midnight()) * NANOS_PER_SEC + i64::from(t.nanosecond()))
}

/// Days since 2000.01.01, with the time as their fraction.
fn datetime(t: NaiveDateTime) -> K {
    let days = date_from_ymd(t.year(), t.month(), t.day()).unwrap_or(0);
    let seconds = f64::from(t.num_seconds_from_midnight()) + f64::from(t.nanosecond()) * 1e-9;
    K::Datetime(f64::from(days) + seconds / 86_400.0)
}

/// The arguments the process was started with (passwords left out), those after `--`, and the script among the others.
fn command_line(args: Vec<String>) -> (Vec<String>, Vec<String>, String) {
    let own = args.iter().position(|a| a == "--").unwrap_or(args.len());
    let script = args[..own].iter().skip(1).find(|a| a.ends_with(".q")).cloned().unwrap_or_default();
    let shown = |(i, a): (usize, &String)| match i > 0 && SECRET.contains(&args[i - 1].as_str()) {
        true => "***".to_string(),
        false => a.clone(),
    };
    let all: Vec<String> = args.iter().enumerate().map(shown).collect();
    let after = all.get(own + 1..).map_or_else(Vec::new, <[String]>::to_vec);
    (all, after, script)
}

impl Engine<'_> {
    pub(crate) fn system_value(&self, name: &str) -> Option<K> {
        let host = || gethostname::gethostname().to_string_lossy().into_owned();
        let arguments = || command_line(std::env::args_os().map(|a| a.to_string_lossy().into_owned()).collect());
        // the handles of the clients that are connected, and of the connections that were made to servers
        let handles = || {
            let mut all: Vec<i32> = self.ws.host.clients.lock().unwrap_or_else(|e| e.into_inner()).keys().copied().collect();
            all.extend(self.ws.host.handles().iter().filter(|h| matches!(h.1, Handle::Server(_))).map(|h| *h.0));
            all.sort_unstable();
            all
        };
        let ints = |attr, v: Vec<i32>| K::Vector(ColVec::Int(attr, Arc::new(v)));
        Some(match name {
            // (in a callback, the client's)
            ".z.a" => K::Int(self.session.client.map_or_else(|| namespaces::address(&host()), |c| c.1)),
            ".z.w" => K::Int(self.session.client.map_or(0, |c| c.0)),
            ".z.H" => ints(Attr::Sorted, handles()),
            ".z.W" => {
                // the bytes that wait to be sent on each (a server is sent to at once)
                let keys = handles();
                let clients = self.ws.host.clients.lock().unwrap_or_else(|e| e.into_inner());
                let queued = |h| clients.get(h).map_or(0, |c| i32::try_from(c.waiting()).unwrap_or(i32::MAX));
                let waiting = keys.iter().map(queued).collect();
                K::Dict(Arc::new(Dict { keys: Box::new(ints(Attr::None, keys)), vals: Box::new(ints(Attr::None, waiting)) }))
            }
            ".z.c" => K::Int(std::thread::available_parallelism().map_or(1, |n| n.get() as i32)),
            ".z.h" => K::Sym(intern(&host())),
            ".z.f" => K::Sym(intern(&arguments().2)),
            ".z.X" => strings(&arguments().0),
            ".z.x" => strings(&arguments().1),
            // (no quiet mode, no blocked updates, no licence)
            ".z.q" | ".z.r" => K::Bool(false),
            ".z.l" => strings(&[]),
            ".z.n" => timespan(Utc::now().naive_utc()),
            ".z.N" => timespan(Local::now().naive_local()),
            ".z.z" => datetime(Utc::now().naive_utc()),
            ".z.Z" => datetime(Local::now().naive_local()),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::command_line;

    #[test]
    fn arguments_are_split_and_passwords_hidden() {
        let args = ["kix-serve", "-u", "me:secret", "init.q", "--", "-abc", "1"].map(String::from).to_vec();
        let (all, after, script) = command_line(args);
        assert_eq!(all, ["kix-serve", "-u", "***", "init.q", "--", "-abc", "1"]);
        assert_eq!(after, ["-abc", "1"]);
        assert_eq!(script, "init.q");
    }
}
