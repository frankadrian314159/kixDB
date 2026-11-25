//! An action too large for one message is sent as pieces, one request each, which the receiving node puts together:
//! table loads and replication snapshots have no size limit of their own.
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The action that carries a piece of another.
pub const ACTION_PIECE: &str = "kix.piece";
/// The largest body sent as one message (tests make it smaller).
pub static PIECE: AtomicUsize = AtomicUsize::new(64 << 20);
/// A transfer that has had no piece for this long is forgotten.
const ABANDONED: Duration = Duration::from_secs(600);

/// The `(action, body)` messages that carry the action `kind` with `body`: itself, or its pieces in order.
pub fn pieces(kind: &str, body: Vec<u8>) -> Vec<(String, Vec<u8>)> {
    let size = PIECE.load(Ordering::Relaxed).max(1);
    if body.len() <= size {
        return vec![(kind.to_string(), body)];
    }
    // tells this transfer from every other one under way
    let id: u64 = std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), body.len());
    let count = body.len().div_ceil(size);
    let piece = |(i, chunk): (usize, &[u8])| {
        // id, number, whether it is the last, the length of the kind, the kind, the bytes
        let head = [&id.to_le_bytes()[..], &(i as u32).to_le_bytes(), &[u8::from(i + 1 == count), kind.len() as u8], kind.as_bytes()];
        (ACTION_PIECE.to_string(), [&head.concat(), chunk].concat())
    };
    body.chunks(size).enumerate().map(piece).collect()
}

struct Transfer {
    next: u32,
    body: Vec<u8>,
    heard: Instant,
}

/// The transfers a node is receiving.
#[derive(Default)]
pub struct Assembly(Mutex<HashMap<u64, Transfer>>);

impl Assembly {
    /// Take a piece: the whole action once its last piece has arrived.
    pub fn add(&self, piece: &[u8]) -> Result<Option<(String, Vec<u8>)>, String> {
        let bad = || "a damaged piece".to_string();
        let head = piece.get(..14).ok_or_else(bad)?;
        let id = u64::from_le_bytes(head[..8].try_into().map_err(|_| bad())?);
        let number = u32::from_le_bytes(head[8..12].try_into().map_err(|_| bad())?);
        let (last, length) = (head[12] != 0, head[13] as usize);
        let kind = std::str::from_utf8(piece.get(14..14 + length).ok_or_else(bad)?).map_err(|_| bad())?;
        let mut transfers = self.0.lock().unwrap_or_else(|e| e.into_inner());
        transfers.retain(|_, t| t.heard.elapsed() < ABANDONED);
        // a first piece starts afresh: the whole is sent again after a failure
        let mut t = match transfers.remove(&id) {
            _ if number == 0 => Transfer { next: 0, body: vec![], heard: Instant::now() },
            Some(t) if t.next == number => t,
            _ => return Err(format!("piece {number} of a transfer arrived out of turn")),
        };
        t.body.extend_from_slice(&piece[14 + length..]);
        if last {
            return Ok(Some((kind.to_string(), t.body)));
        }
        (t.next, t.heard) = (number + 1, Instant::now());
        transfers.insert(id, t);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pieces_make_the_whole_again_and_only_in_turn() {
        let body: Vec<u8> = (0..1000u32).flat_map(u32::to_le_bytes).collect();
        PIECE.store(1024, Ordering::Relaxed);
        let (parts, again) = (pieces("load", body.clone()), pieces("load", body.clone()));
        PIECE.store(64 << 20, Ordering::Relaxed);
        assert_eq!(parts.len(), 4);
        assert_eq!(pieces("load", body.clone()), [("load".to_string(), body.clone())]);
        let node = Assembly::default();
        // two transfers at once keep apart
        for (a, b) in parts.iter().zip(&again).take(3) {
            assert_eq!(node.add(&a.1), Ok(None));
            assert_eq!(node.add(&b.1), Ok(None));
        }
        assert_eq!(node.add(&parts[3].1), Ok(Some(("load".to_string(), body.clone()))));
        // a piece out of turn ends its transfer; sent from the start it succeeds
        assert!(node.add(&parts[2].1).is_err());
        let whole = parts.iter().map(|p| node.add(&p.1).unwrap()).last().unwrap();
        assert_eq!(whole, Some(("load".to_string(), body)));
        assert!(node.add(b"short").is_err());
    }
}
