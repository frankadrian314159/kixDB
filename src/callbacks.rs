//! The `.z` callbacks that a shell or server makes, calling the functions that the workspace holds under those names: on
//! connections (`.z.pw`, `.z.po`, `.z.pc`), messages (`.z.pg`, `.z.ps`, `.z.bm`), the timer (`.z.ts`), console input
//! (`.z.pi`) and leaving (`.z.exit`). (`.z.vs`, on setting a global, is the engine's.) Also what a process does between
//! statements: its timer, and running what the servers it is connected to send.
use crate::engine::Engine;
use crate::error::KResult;
use crate::ipc::Outbox;
use crate::sym::intern;
use crate::types::*;
use crate::{Connection, Server};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The first handle given to a client that connects.
const FIRST_CLIENT: i32 = 1000;

/// How often the background thread looks for a change of the timer's interval (`\t`).
const TICK: Duration = Duration::from_millis(20);

impl Connection {
    /// Call the callback `name`, if there is one.
    fn callback(&mut self, name: &str, args: &[K]) -> Option<KResult<K>> {
        let function = self.server.read_workspace(|ws| ws.vars.get(&intern(name)).cloned())?;
        let args: Vec<Option<K>> = args.iter().cloned().map(Some).collect();
        Some(self.alone(|engine| engine.apply_value(&function, &args)))
    }

    /// Run `f` in the root namespace and alone on the workspace. What it prints goes to the console.
    fn alone<R>(&mut self, f: impl FnOnce(&mut Engine) -> R) -> R {
        let namespace = std::mem::take(&mut self.session.current_ns);
        let result = {
            let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
            let mut engine = Engine::new(&mut ws, &mut self.session);
            let result = f(&mut engine);
            engine.take_messages().iter().for_each(|m| println!("{m}"));
            result
        };
        self.session.current_ns = namespace;
        result
    }

    /// Run a message that a server this process is connected to has sent, if one waits, as a client's asynchronous one
    /// is run (`.z.ps`), with the handle as `.z.w`; `.z.pc` hears of such a connection ending. Gives the handle.
    fn take_from_server(&mut self, after: i32) -> Option<i32> {
        // (looked for under the lock, so that a statement that waits on the handle, as with `h[]`, keeps its messages)
        let (handle, sent) = self.server.read_workspace(|ws| ws.host.next_sent(after))?;
        match sent {
            None => self.notify(".z.pc", &[K::Int(handle)]),
            Some(message) => {
                self.session.client = Some((handle, 0));
                let result = message.and_then(|m| {
                    self.callback(".z.ps", std::slice::from_ref(&m)).unwrap_or_else(|| self.alone(|engine| engine.eval_message(&m)))
                });
                self.session.client = None;
                if let Err(e) = result {
                    eprintln!("'{} in a message from handle {handle}: {e}", e.signal);
                }
            }
        }
        Some(handle)
    }

    /// A callback whose result is not wanted: an error is printed.
    fn notify(&mut self, name: &str, args: &[K]) {
        if let Some(Err(e)) = self.callback(name, args) {
            eprintln!("'{} in {name}: {e}", e.signal);
        }
    }

    /// Take this connection to be a client's, from the IPv4 `address`: it gets a handle (`.z.w`), under which `outbox`
    /// sends to it.
    pub(crate) fn connected_from(&mut self, address: i32, outbox: &Outbox) {
        let handle = self.server.read_workspace(|ws| {
            let mut clients = ws.host.clients.lock().unwrap_or_else(|e| e.into_inner());
            let free = (FIRST_CLIENT..).find(|h| !clients.contains_key(h)).unwrap_or(FIRST_CLIENT);
            clients.insert(free, outbox.clone());
            free
        });
        // a client disconnected for falling behind is forgotten at once, not when its connection's thread notices
        let server = Arc::downgrade(&self.server.shared);
        outbox.when_dropped(move |outbox| {
            if let Some(shared) = server.upgrade() {
                let (server, outbox) = (Server { shared }, outbox.clone());
                std::thread::spawn(move || server.connect_internal().forget(handle, &outbox, true));
            }
        });
        self.session.client = Some((handle, address));
    }

    /// Whether `.z.pw` lets the user in: anything but true, an error included, keeps them out.
    pub(crate) fn authorized(&mut self, user: &str, password: &str) -> bool {
        let answer = self.callback(".z.pw", &[K::sym(user), K::string(password)]);
        answer.is_none_or(|a| matches!(a, Ok(K::Bool(true))))
    }

    pub(crate) fn opened(&mut self) {
        if let Some((handle, _)) = self.session.client {
            self.notify(".z.po", &[K::Int(handle)]);
        }
    }

    /// Forget this connection's client, calling `.z.pc` for one that was `opened`.
    pub(crate) fn closed(&mut self, opened: bool, outbox: &Outbox) {
        if let Some((handle, _)) = self.session.client.take() {
            self.forget(handle, outbox, opened);
        }
    }

    /// Forget the client's handle and, if `tell`, call `.z.pc` with it. A handle that was forgotten already, as one
    /// that the server closed (`hclose`) is, may be another client's by now: nothing is done.
    fn forget(&mut self, handle: i32, outbox: &Outbox, tell: bool) {
        let ours = self.server.read_workspace(|ws| {
            let mut clients = ws.host.clients.lock().unwrap_or_else(|e| e.into_inner());
            clients.get(&handle).is_some_and(|o| o.is(outbox)) && clients.remove(&handle).is_some()
        });
        if ours && tell {
            self.notify(".z.pc", &[K::Int(handle)]);
        }
    }

    /// The answer to a client's message: what `.z.pg` (`.z.ps` for an asynchronous one) makes of it, or else its value.
    pub(crate) fn answer(&mut self, message: &K, sync: bool) -> KResult<K> {
        let name = if sync { ".z.pg" } else { ".z.ps" };
        self.callback(name, std::slice::from_ref(message)).unwrap_or_else(|| self.eval_ipc_message(message))
    }

    /// Give `.z.bm` a message that could not be read; whether there is one (the connection is then closed).
    pub(crate) fn bad_message(&mut self, bytes: &[u8]) -> bool {
        let handle = K::Int(self.session.client.map_or(0, |c| c.0));
        let raw = K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes.to_vec())));
        let report = K::Vector(ColVec::General(Attr::None, Arc::new(vec![handle, raw])));
        let handled = self.callback(".z.bm", &[report]);
        if let Some(Err(e)) = &handled {
            eprintln!("'{} in .z.bm: {e}", e.signal);
        }
        handled.is_some()
    }

    /// What `.z.pi` makes of a line typed at the console, if there is one.
    pub fn console_input(&mut self, line: &str) -> Option<KResult<K>> {
        self.callback(".z.pi", &[K::string(line)])
    }

    /// Call `.z.exit` with the exit code, before the process ends.
    pub fn leaving(&mut self, code: i32) {
        self.notify(".z.exit", &[K::Int(code)]);
    }
}

/// The thread that calls `.z.ts` every `\t` milliseconds and runs what servers send; dropping this stops it.
pub struct Background {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Background {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

impl Server {
    /// Run the timer, which `\t` sets, and the messages that arrive on the handles of `hopen`, until what this returns
    /// is dropped.
    pub fn start_background(&self) -> Background {
        let stop = Arc::new(AtomicBool::new(false));
        let (stopping, mut conn) = (stop.clone(), self.connect_internal());
        let host = self.read_workspace(|ws| ws.host.clone());
        let thread = std::thread::spawn(move || {
            // the interval that was set and when it next ends
            let mut next: Option<(u64, Instant)> = None;
            // the handle whose message was run last
            let mut last = 0;
            while !stopping.load(Ordering::SeqCst) {
                let now = Instant::now();
                next = match (host.timer_ms.load(Ordering::SeqCst), next) {
                    (0, _) => None,
                    (ms, Some(running)) if running.0 == ms => Some(running),
                    (ms, _) => now.checked_add(Duration::from_millis(ms)).map(|due| (ms, due)),
                };
                if let Some((ms, due)) = next.filter(|n| n.1 <= now) {
                    if let Ok(time) = conn.eval(".z.p") {
                        conn.notify(".z.ts", &[time]);
                    }
                    // (intervals missed while the call ran are not made up)
                    next = Some((ms, (due + Duration::from_millis(ms)).max(Instant::now())));
                }
                // (one message at a time, so that the timer and other statements get their turns; the list of handles is
                // let go before the message is looked for, which takes it again)
                let connected = !host.handles().is_empty();
                if let Some(handle) = connected.then(|| conn.take_from_server(last)).flatten() {
                    last = handle;
                    continue;
                }
                std::thread::park_timeout(next.map_or(TICK, |n| n.1.saturating_duration_since(Instant::now()).min(TICK)));
            }
        });
        let waking = self.read_workspace(|ws| ws.host.events.clone());
        *waking.lock().unwrap_or_else(|e| e.into_inner()) = Some(thread.thread().clone());
        Background { stop, thread: Some(thread) }
    }
}
