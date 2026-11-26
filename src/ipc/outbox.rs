//! What a server sends to a client. Each connection has a queue of messages and a thread that writes them, so that
//! adding to the queue never waits for the client: a publisher, which holds the workspace, is not held up by a client
//! that reads slowly. For that a connection is split in two: one half is read by the connection's thread; the other is
//! written by whichever thread holds it, the writer's or, for an answer that need not wait its turn, the connection's.
use super::{write_ipc_frame, MSG_ASYNC, MSG_RESPONSE};
use crate::error::{KError, KResult};
use rustls::{ServerConnection, StreamOwned};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

/// The bytes of asynchronous messages that may wait for a client, unless the listener sets another limit, before it is
/// disconnected.
pub const QUEUE_LIMIT: usize = 256 << 20;

/// How long a connection that is being closed has to take what is queued for it.
const CLOSE_WITHIN: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Queue {
    /// The messages to write, in order, with their types.
    messages: VecDeque<(u8, Arc<Vec<u8>>)>,
    /// The bytes queued and not yet written: all of them, and those of asynchronous messages.
    waiting: usize,
    pushed: usize,
    /// An answer is queued and not yet written.
    answering: bool,
    /// What is written to, and whether to compress: taken out by the thread that is writing (`None` before the client
    /// has been let in).
    sink: Option<(Box<dyn Write + Send>, bool)>,
    /// Nothing more is queued.
    closing: bool,
    /// Nothing more will be written.
    ended: bool,
}

/// What to do when a client is disconnected for falling behind.
type Dropped = Box<dyn Fn(&Outbox) + Send + Sync>;

struct State {
    queue: Mutex<Queue>,
    changed: Condvar,
    limit: usize,
    socket: TcpStream,
    dropped: OnceLock<Dropped>,
}

/// The sending side of a client's connection.
#[derive(Clone)]
pub struct Outbox(Arc<State>);

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Outbox({} bytes waiting)", self.waiting())
    }
}

impl Outbox {
    pub(super) fn new(socket: TcpStream, limit: usize) -> Outbox {
        Outbox(Arc::new(State { queue: Mutex::default(), changed: Condvar::new(), limit, socket, dropped: OnceLock::new() }))
    }

    pub(crate) fn when_dropped(&self, then: impl Fn(&Outbox) + Send + Sync + 'static) {
        let _ = self.0.dropped.set(Box::new(then));
    }

    /// Whether `other` is the same connection's.
    pub(crate) fn is(&self, other: &Outbox) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.0.queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait<'a>(&self, queue: MutexGuard<'a, Queue>) -> MutexGuard<'a, Queue> {
        self.0.changed.wait(queue).unwrap_or_else(|e| e.into_inner())
    }

    /// The bytes that wait to be sent.
    pub(crate) fn waiting(&self) -> usize {
        self.queue().waiting
    }

    pub(super) fn is_closed(&self) -> bool {
        self.queue().closing
    }

    fn add(&self, queue: &mut Queue, kind: u8, payload: Arc<Vec<u8>>) {
        queue.waiting += payload.len();
        if kind == MSG_ASYNC {
            queue.pushed += payload.len();
        }
        queue.messages.push_back((kind, payload));
    }

    /// Queue an asynchronous message. A client that has fallen more than the limit behind is disconnected instead.
    pub(crate) fn push(&self, payload: Arc<Vec<u8>>) {
        let mut queue = self.queue();
        if queue.closing {
            return;
        }
        if queue.pushed <= self.0.limit {
            self.add(&mut queue, MSG_ASYNC, payload);
            return self.0.changed.notify_all();
        }
        drop(queue);
        eprintln!("disconnecting a client: more than {} bytes wait to be sent to it", self.0.limit);
        self.discard();
        if let Some(dropped) = self.0.dropped.get() {
            dropped(self);
        }
    }

    /// Queue the answer to a request and see it written, so that a client's requests are taken no faster than it reads
    /// their answers. It is written here, after what is queued before it, unless the writer is at work.
    pub(super) fn respond(&self, payload: Vec<u8>) -> KResult<()> {
        let closed = || KError::ipc("the connection is closed");
        let mut queue = self.queue();
        if queue.closing {
            return Err(closed());
        }
        queue.answering = true;
        self.add(&mut queue, MSG_RESPONSE, Arc::new(payload));
        while queue.answering && !queue.ended {
            queue = if queue.sink.is_some() { self.write_next(queue) } else { self.wait(queue) };
        }
        if !queue.messages.is_empty() || queue.closing {
            self.0.changed.notify_all();
        }
        if queue.answering {
            Err(closed())
        } else {
            Ok(())
        }
    }

    fn stop_queuing(&self) {
        self.queue().closing = true;
        self.0.changed.notify_all();
    }

    /// Forget what is queued and shut the connection down. (On Windows that wakes neither a thread that waits to read
    /// from the socket nor one that waits to write to it: those end when the client closes or reads.)
    fn discard(&self) {
        let mut queue = self.queue();
        queue.messages.clear();
        (queue.waiting, queue.pushed, queue.closing, queue.ended) = (0, 0, true, true);
        drop(queue);
        self.0.changed.notify_all();
        let _ = self.0.socket.shutdown(Shutdown::Both);
    }

    /// Send what is queued, then close the connection; returns at once.
    pub(crate) fn close(&self) {
        self.stop_queuing();
        let outbox = self.clone();
        std::thread::spawn(move || outbox.finish());
    }

    /// Send what is queued, giving up after `CLOSE_WITHIN`, then close the connection.
    pub(super) fn finish(&self) {
        self.stop_queuing();
        drop(self.0.changed.wait_timeout_while(self.queue(), CLOSE_WITHIN, |q| !q.ended));
        self.discard();
    }

    /// Write the first queued message, if there is one and the sink is free, without holding the queue meanwhile.
    fn write_next<'a>(&'a self, mut queue: MutexGuard<'a, Queue>) -> MutexGuard<'a, Queue> {
        if queue.sink.is_none() || queue.messages.is_empty() {
            return queue;
        }
        let (Some((mut sink, compress)), Some((kind, payload))) = (queue.sink.take(), queue.messages.pop_front()) else { return queue };
        drop(queue);
        if write_ipc_frame(&mut sink, kind, &payload, compress).is_err() {
            self.discard();
            return self.queue();
        }
        let mut queue = self.queue();
        queue.sink = Some((sink, compress));
        // (what was discarded meanwhile is counted no longer)
        queue.waiting = queue.waiting.saturating_sub(payload.len());
        if kind == MSG_ASYNC {
            queue.pushed = queue.pushed.saturating_sub(payload.len());
        } else if !queue.ended {
            queue.answering = false;
        }
        queue
    }

    /// Start the thread that writes the queued messages to `sink` until the connection is closed or fails.
    pub(super) fn write_to(&self, sink: Box<dyn Write + Send>, compress: bool) {
        self.queue().sink = Some((sink, compress));
        let outbox = self.clone();
        std::thread::spawn(move || {
            let mut queue = outbox.queue();
            // (to the end of what is queued, and of the answer that the connection's thread may be writing)
            while !(queue.ended || queue.closing && queue.messages.is_empty() && queue.sink.is_some()) {
                if queue.sink.is_some() && !queue.messages.is_empty() {
                    queue = outbox.write_next(queue);
                    outbox.0.changed.notify_all();
                } else {
                    queue = outbox.wait(queue);
                }
            }
            drop(queue);
            outbox.discard();
        });
    }
}

// ----- A connection's two halves --------------------------------------------

/// A client's connection as it is while it logs in.
pub(super) enum Incoming {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Read for Incoming {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Incoming::Plain(s) => s.read(buf),
            Incoming::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Incoming {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Incoming::Plain(s) => s.write(buf),
            Incoming::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Incoming::Plain(s) => s.flush(),
            Incoming::Tls(s) => s.flush(),
        }
    }
}

/// The halves of a connection: what is read from and what is written to.
type Halves = (Box<dyn Read>, Box<dyn Write + Send>);

impl Incoming {
    pub(super) fn split(self) -> io::Result<Halves> {
        Ok(match self {
            Incoming::Plain(socket) => (Box::new(socket.try_clone()?), Box::new(socket)),
            Incoming::Tls(stream) => {
                let StreamOwned { conn, sock } = *stream;
                let tls = Arc::new(Mutex::new(conn));
                let reader = TlsReader { tls: tls.clone(), socket: sock.try_clone()?, raw: vec![0; 16 << 10], unread: 0..0 };
                (Box::new(reader), Box::new(TlsWriter { tls, socket: sock }))
            }
        })
    }
}

fn locked(tls: &Mutex<ServerConnection>) -> MutexGuard<'_, ServerConnection> {
    tls.lock().unwrap_or_else(|e| e.into_inner())
}

/// The reading half of a TLS connection. It holds the connection's state only to decrypt, never while it waits.
struct TlsReader {
    tls: Arc<Mutex<ServerConnection>>,
    socket: TcpStream,
    /// Bytes from the socket, and those of them that have yet to be decrypted.
    raw: Vec<u8>,
    unread: std::ops::Range<usize>,
}

impl Read for TlsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            {
                let mut tls = locked(&self.tls);
                match tls.reader().read(buf) {
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    read => return read,
                }
                if !self.unread.is_empty() {
                    self.unread.start += tls.read_tls(&mut &self.raw[self.unread.clone()])?;
                    tls.process_new_packets().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    continue;
                }
            }
            self.unread = 0..self.socket.read(&mut self.raw)?;
            if self.unread.is_empty() {
                return Ok(0);
            }
        }
    }
}

/// The writing half of a TLS connection, and the only one to write to its socket (so that records leave in order).
struct TlsWriter {
    tls: Arc<Mutex<ServerConnection>>,
    socket: TcpStream,
}

impl TlsWriter {
    /// Give the connection `plain` to encrypt, and send all that it has ready.
    fn send(&mut self, plain: &[u8]) -> io::Result<usize> {
        let mut ready = Vec::new();
        let taken = {
            let mut tls = locked(&self.tls);
            let taken = tls.writer().write(plain)?;
            while tls.wants_write() {
                tls.write_tls(&mut ready)?;
            }
            taken
        };
        self.socket.write_all(&ready)?;
        Ok(taken)
    }
}

impl Write for TlsWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.send(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.send(&[])?;
        self.socket.flush()
    }
}
