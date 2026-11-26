//! Files and handles: `hsym`, `hdel`, `key`, `read0`, `read1`, `save`, `load`, `rsave`, `.Q.fs`, the handles of
//! `hopen` (to a file as well as to a server), and sending through a handle, the clients' too. Relative paths are taken
//! from the workspace's directory, if it has one.
use crate::engine::{ops, Engine};
use crate::error::{KError, KResult};
use crate::ipc::{encode_k_payload, IpcClient, Outbox};
use crate::storage;
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What a handle that `hopen` gave leads to.
pub enum Handle {
    Server(IpcClient),
    File(PathBuf),
}

/// The server a handle's name gives, as `(host:port, user, password)`: it is written `host:port[:user:password]` after
/// any `:`, and no host means this machine. `None`: the name is a file's.
pub fn server_address(name: &str) -> Option<(String, Option<&str>, &str)> {
    let name = name.strip_prefix(':').unwrap_or(name);
    let (scheme, rest) = match name.strip_prefix("tcps://") {
        Some(rest) => ("tcps://", rest),
        None => ("", name),
    };
    let mut parts = rest.splitn(4, ':');
    let host = parts.next()?;
    let port: u16 = parts.next()?.parse().ok()?;
    let host = if host.is_empty() { "127.0.0.1" } else { host };
    Some((format!("{scheme}{host}:{port}"), parts.next(), parts.next().unwrap_or("")))
}

fn symbols(names: impl IntoIterator<Item = SymId>) -> K {
    K::Vector(ColVec::Sym(Attr::None, Arc::new(names.into_iter().collect())))
}

/// `x` as file symbols (`` `:path ``).
pub fn hsym(x: &K) -> KResult<K> {
    let file = |s: SymId| match resolve(s) {
        name if name.starts_with(':') => s,
        name => intern(&format!(":{name}")),
    };
    match x {
        K::Sym(s) => Ok(K::Sym(file(*s))),
        K::Vector(ColVec::Sym(_, names)) => Ok(symbols(names.iter().map(|&s| file(s)))),
        _ => Err(KError::ty("hsym takes symbols")),
    }
}

/// The lines of `bytes`, without their ends.
fn lines(bytes: &[u8]) -> Vec<K> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    if bytes.is_empty() {
        return vec![];
    }
    bytes.split(|&b| b == b'\n').map(|l| chars(l.strip_suffix(b"\r").unwrap_or(l))).collect()
}

fn chars(bytes: &[u8]) -> K {
    K::Vector(ColVec::Char(Attr::None, Arc::new(bytes.to_vec())))
}

/// The bytes that stand for `x` in a file that holds no `q` value: those of its items, one after another.
fn raw_bytes(x: &K) -> KResult<Vec<u8>> {
    if let K::Vector(ColVec::General(_, items)) = x {
        return Ok(items.iter().map(raw_bytes).collect::<KResult<Vec<_>>>()?.concat());
    }
    // (an atom's encoding is its type and bytes; a list's also has its attribute and length)
    let encoded = encode_k_payload(x);
    let kind = encoded[0] as i8;
    match kind.unsigned_abs() {
        1 | 2 | 4..=10 | 12..=19 => Ok(encoded[if kind < 0 { 1 } else { 6 }..].to_vec()),
        _ => Err(KError::ty("this value has no bytes to append to a file")),
    }
}

/// Add `x` to the end of the file: as lines of text, as items of the list the file holds, or as bytes.
fn append(path: &Path, x: &K, text: bool) -> KResult<()> {
    let bytes = if text {
        let given = match x {
            K::Vector(ColVec::General(_, items)) => items.to_vec(),
            line => vec![line.clone()],
        };
        let mut bytes = Vec::new();
        for line in given {
            if !matches!(line, K::Char(_) | K::Vector(ColVec::Char(..))) {
                return Err(KError::ty("a negative file handle takes a string or a list of strings"));
            }
            bytes.extend(raw_bytes(&line)?);
            bytes.push(b'\n');
        }
        bytes
    } else if storage::holds_value(path) {
        let joined = ops::eval_binary_prim(",", &storage::read_flat_k(path)?, x)?;
        return storage::write_flat_k(path, &joined);
    } else {
        raw_bytes(x)?
    };
    Ok(fs::OpenOptions::new().append(true).create(true).open(path)?.write_all(&bytes)?)
}

impl Engine<'_> {
    /// A connection to the server `name` gives; `None` if it names a file.
    pub(crate) fn connect_to(&self, name: &str) -> Option<KResult<IpcClient>> {
        let (address, user, password) = server_address(name)?;
        Some(IpcClient::connect_addr(&address, user.unwrap_or(&self.session.login), password))
    }

    /// `hopen` of a server (`` `:host:port ``, or a port on this machine) or of a file, which is made if it is not there.
    pub(crate) fn eval_hopen(&mut self, x: &K) -> KResult<K> {
        let name = match x {
            K::Int(_) | K::Long(_) => format!("::{}", x.as_i64()?),
            _ => x.as_str().ok_or_else(|| KError::ty("hopen expects `:host:port, `:file or a port"))?,
        };
        let handle = match self.connect_to(&name) {
            Some(client) => Handle::Server(client?),
            None => {
                let path = storage::path_of(self.ws, &K::sym(&name))?;
                storage::create_parent(&path)?;
                fs::OpenOptions::new().append(true).create(true).open(&path)?;
                Handle::File(path)
            }
        };
        // (the lowest number that is free)
        let h = (3..).find(|h| !self.session.handles.contains_key(h)).unwrap_or(i32::MAX);
        self.session.handles.insert(h, handle);
        Ok(K::Int(h))
    }

    /// `hclose` of a handle of `hopen`'s, or of a client's, which is sent what waits for it and then disconnected.
    pub(crate) fn eval_hclose(&mut self, x: &K) -> KResult<K> {
        let h = (x.as_i64()? as i32).saturating_abs();
        if self.session.handles.remove(&h).is_none() {
            if let Some(client) = self.ws.host.clients.lock().unwrap_or_else(|e| e.into_inner()).remove(&h) {
                client.close();
            }
        }
        Ok(K::Null)
    }

    /// The connected client with the handle `h`.
    fn client(&self, h: i32) -> Option<Outbox> {
        self.ws.host.clients.lock().unwrap_or_else(|e| e.into_inner()).get(&h).cloned()
    }

    /// `x` as an open handle, negative for the asynchronous form.
    pub(crate) fn open_handle(&self, x: &K) -> Option<i32> {
        let h = match x {
            K::Int(h) => *h,
            K::Long(h) => i32::try_from(*h).ok()?,
            _ => return None,
        };
        let to = h.saturating_abs();
        (self.session.handles.contains_key(&to) || self.client(to).is_some()).then_some(h)
    }

    /// Apply an open handle to `x`: a server is asked (`h`) or told (`neg h`), a client can only be told, and a file is
    /// added to. With nothing to send, `h[]` waits for the server's next message and `neg[h][]` does nothing, as
    /// nothing is held back.
    pub(crate) fn apply_handle(&mut self, h: i32, x: K) -> KResult<K> {
        match self.session.handles.get_mut(&h.saturating_abs()) {
            Some(Handle::Server(client)) if x == K::Null && h > 0 => client.receive(),
            _ if h < 0 && x == K::Null => Ok(K::Null),
            Some(Handle::File(path)) => append(path, &x, h < 0).map(|_| K::Int(h)),
            Some(Handle::Server(client)) if h > 0 => client.query_k(&x),
            Some(Handle::Server(client)) => client.send_async_k(&x).map(|_| K::Null),
            None if h > 0 => Err(KError::nyi("a synchronous call to a client (send to it with neg)")),
            None => {
                // (a copy of the workspace sends nothing: the statement may be run again)
                self.ws.writable()?;
                let client = self.client(-h).ok_or_else(|| KError::ipc("closed handle"))?;
                client.push(Arc::new(encode_k_payload(&x.untagged())));
                Ok(K::Null)
            }
        }
    }

    /// `-25!(handles; message)`: send the message, encoded once, to each handle. Nothing is sent unless all are open.
    pub(crate) fn broadcast(&mut self, x: &K) -> KResult<K> {
        if x.count() != 2 {
            return Err(KError::length("-25! takes (handles; message)"));
        }
        let part = |i| ops::index_k(x, &[Some(K::Long(i))]);
        let (to, message) = (part(0)?, part(1)?);
        let handles = match &to {
            K::Vector(v) => v.as_i64_vec(),
            one => one.as_i64().ok().map(|h| vec![h]),
        };
        let handles = handles.ok_or_else(|| KError::ty("-25! takes handles"))?;
        let handles: Vec<i32> = handles.into_iter().map(|h| i32::try_from(h).unwrap_or(0)).collect();
        let open = |h: &i32| matches!(self.session.handles.get(h), Some(Handle::Server(_))) || self.client(*h).is_some();
        if !handles.iter().all(open) {
            return Err(KError::ipc("-25!: not all of the handles are open connections"));
        }
        self.ws.writable()?;
        let message = message.untagged();
        let payload = Arc::new(encode_k_payload(&message));
        for h in handles {
            match self.session.handles.get_mut(&h) {
                Some(Handle::Server(server)) => server.send_async_k(&message)?,
                _ => self.client(h).into_iter().for_each(|c| c.push(payload.clone())),
            }
        }
        Ok(K::Null)
    }

    /// The value a global name holds, to be changed: for a file not yet there, the empty list.
    pub(crate) fn held(&mut self, name: SymId) -> KResult<K> {
        match resolve(name).strip_prefix(':') {
            Some(file) if !storage::path_of(self.ws, &K::sym(file))?.exists() => Ok(K::Vector(ColVec::General(Attr::None, Arc::new(vec![])))),
            _ => self.get_var(name),
        }
    }

    /// `key` of a file symbol: the names in a directory, the symbol of a file, or `()` if there is nothing there.
    pub(crate) fn key_of_path(&mut self, x: &K) -> KResult<K> {
        let path = storage::path_of(self.ws, x)?;
        if path.is_dir() {
            let mut names = fs::read_dir(path)?.map(|e| Ok(e?.file_name().to_string_lossy().into_owned())).collect::<KResult<Vec<_>>>()?;
            names.sort();
            return Ok(symbols(names.iter().map(|n| intern(n))));
        }
        Ok(if path.exists() { x.clone() } else { K::Vector(ColVec::General(Attr::None, Arc::new(vec![]))) })
    }

    /// `hdel`: remove a file or an empty directory.
    pub(crate) fn hdel(&mut self, x: &K) -> KResult<K> {
        let path = storage::path_of(self.ws, x)?;
        if path.is_dir() {
            fs::remove_dir(path)?;
        } else {
            fs::remove_file(path)?;
        }
        hsym(x)
    }

    /// `read0` (lines, or the characters of a part) and `read1` (bytes) of `` `:file `` or ``(`:file; offset; length)``.
    pub(crate) fn read_file(&mut self, x: &K, text: bool) -> KResult<K> {
        let bytes = storage::read_bytes(self.ws, x)?;
        Ok(match x {
            _ if !text => K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes))),
            K::Sym(_) => K::Vector(ColVec::General(Attr::None, Arc::new(lines(&bytes)))),
            _ => chars(&bytes),
        })
    }

    /// `save`: the global that the file is named after, as text for the extensions in `.h.tx`, else as a `q` value.
    pub(crate) fn save(&mut self, x: &K) -> KResult<K> {
        let target = hsym(x)?;
        let path = storage::path_of(self.ws, &target)?;
        let part = |p: Option<&std::ffi::OsStr>| p.map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let extension = part(path.extension());
        let text = matches!(extension.as_str(), "csv" | "txt" | "xml" | "xls");
        let name = part(if text { path.file_stem() } else { path.file_name() });
        let value = self.get_var(intern(&name))?;
        if text {
            let formats = self.eval_builtin_ns(".h.tx")?.ok_or_else(|| KError::value(".h.tx"))?;
            let format = ops::index_k(&formats, &[Some(K::sym(&extension))])?;
            let lines = self.apply_value(&format, &[Some(value)])?;
            storage::zero_colon(self.ws, &target, &lines)?;
        } else {
            self.set_global(intern(&resolve_symbol(&target)?), value)?;
        }
        Ok(target)
    }

    /// `rsave`: the global that the directory is named after, splayed.
    pub(crate) fn rsave(&mut self, x: &K) -> KResult<K> {
        let target = resolve_symbol(&hsym(x)?)?;
        let directory = target.trim_end_matches(['/', '\\']);
        let name = Path::new(&directory[1..]).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let value = self.get_var(intern(&name))?;
        let splayed = intern(&format!("{directory}/"));
        self.set_global(splayed, value)?;
        Ok(K::Sym(splayed))
    }

    /// `load` and `rload` (and `\l` of a file holding a value): set the global that the path is named after.
    pub(crate) fn load(&mut self, x: &K) -> KResult<K> {
        let path = storage::path_of(self.ws, x)?;
        let name = path.file_name().map(|n| intern(&n.to_string_lossy())).ok_or_else(|| KError::domain("load needs a file name"))?;
        let value = storage::load_path(self.ws, &path.to_string_lossy())?;
        self.set_global(name, value)?;
        Ok(K::Sym(name))
    }

    /// `.Q.fs` and `.Q.fsn`: apply `f` to the lines of a file, about `size` bytes of them at a time. Gives the bytes read.
    pub(crate) fn in_chunks(&mut self, f: &K, file: &K, size: usize) -> KResult<K> {
        let mut reader = BufReader::new(fs::File::open(storage::path_of(self.ws, file)?)?);
        let mut total = 0;
        loop {
            let mut chunk = Vec::new();
            while chunk.len() < size.max(1) && reader.read_until(b'\n', &mut chunk)? > 0 {}
            if chunk.is_empty() {
                return Ok(K::Long(total));
            }
            total += chunk.len() as i64;
            self.apply_value(f, &[Some(K::Vector(ColVec::General(Attr::None, Arc::new(lines(&chunk)))))])?;
        }
    }
}

fn resolve_symbol(x: &K) -> KResult<String> {
    match x {
        K::Sym(s) => Ok(resolve(*s)),
        _ => Err(KError::ty("expected a symbol")),
    }
}
