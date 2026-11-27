//! Work over large vectors split across threads. Results are the same however many threads there are: pieces have fixed
//! sizes and are put together in order. (A sum of many floating point numbers may differ in its last digits from the
//! sum added one after another.)
use rayon::prelude::*;
use foldhash::{HashMap, HashMapExt};
use std::ops::Range;

/// Inputs shorter than this are not worth handing to other threads.
pub const PAR: usize = 100_000;
/// The items a thread takes at a time.
const PIECE: usize = 32_768;

/// Share work among a thread for each processor core, not one for each of the two that a core may run: the second adds
/// processor time and no speed. `RAYON_NUM_THREADS` says otherwise. For a program, before its first query: one that
/// embeds the engine sets rayon up as it likes.
pub fn one_thread_a_core() {
    #[cfg(target_arch = "x86_64")]
    if std::env::var_os("RAYON_NUM_THREADS").is_none() {
        // (the processor's topology leaf says how many threads share a core)
        #[allow(unused_unsafe)]
        let per_core = unsafe { std::arch::x86_64::__cpuid_count(0xB, 0) }.ebx & 0xffff;
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        // (a pool that is there already is left as it is)
        let _ = rayon::ThreadPoolBuilder::new().num_threads(threads.div_ceil(per_core.clamp(1, 2) as usize)).build_global();
    }
}

/// Whether work on `n` items is shared out.
pub fn split(n: usize) -> bool {
    n >= PAR && rayon::current_num_threads() > 1
}

/// `f` of every item of `items`.
pub fn map<A: Sync, T: Send>(items: &[A], f: impl Fn(&A) -> T + Sync) -> Vec<T> {
    match split(items.len()) {
        false => items.iter().map(f).collect(),
        true => items.par_iter().with_min_len(PIECE).map(&f).collect(),
    }
}

/// `f` of every pair of items at the same place of `x` and `y`, which are equally long.
pub fn map2<A: Sync, B: Sync, T: Send>(x: &[A], y: &[B], f: impl Fn(&A, &B) -> T + Sync) -> Vec<T> {
    match split(x.len()) {
        false => x.iter().zip(y).map(|(x, y)| f(x, y)).collect(),
        true => x.par_iter().zip(y).with_min_len(PIECE).map(|(x, y)| f(x, y)).collect(),
    }
}

/// `piece` of consecutive pieces of `items`, in order. (The pieces do not depend on the number of threads.)
pub fn pieces<A: Sync, T: Send>(items: &[A], piece: impl Fn(&[A]) -> T + Sync) -> Vec<T> {
    match items.len() < PAR {
        true => vec![piece(items)],
        false => items.par_chunks(PIECE).map(&piece).collect(),
    }
}

/// The positions of the `true`s.
pub fn true_rows(bs: &[bool]) -> Vec<usize> {
    let count = |bs: &[bool]| bs.iter().map(|&b| usize::from(b)).sum::<usize>();
    if !split(bs.len()) {
        // every position is written and then kept or not: there is no branch to mispredict
        let count = count(bs);
        let mut rows = vec![0; count + 1];
        let mut next = 0;
        for (i, &b) in bs.iter().enumerate() {
            rows[next] = i;
            next += usize::from(b);
        }
        rows.truncate(count);
        return rows;
    }
    // each piece is given its place in the result
    let counts: Vec<usize> = bs.par_chunks(PIECE).map(count).collect();
    let mut rows = vec![0; counts.iter().sum()];
    let mut rest = rows.as_mut_slice();
    let places: Vec<&mut [usize]> = counts.iter().map(|&n| {
        let (place, after) = std::mem::take(&mut rest).split_at_mut(n);
        rest = after;
        place
    }).collect();
    places.into_par_iter().zip(bs.par_chunks(PIECE)).enumerate().for_each(|(p, (place, bs))| {
        let trues = bs.iter().enumerate().filter(|(_, &b)| b).map(|(i, _)| p * PIECE + i);
        place.iter_mut().zip(trues).for_each(|(slot, row)| *slot = row);
    });
    rows
}

/// Number the groups of rows with the keys `keys` (a column of 64-bit values each) into `ids`, in order of first
/// appearance, a piece at a time: `number` numbers the rows of one piece the same way, from 0. `false` (and nothing
/// to be relied on in `ids`) when there are so many groups that putting the pieces together would cost what it saves.
pub fn group<K: AsRef<[u64]> + Sync>(keys: &[K], ids: &mut [usize], number: impl Fn(Range<usize>, &mut [usize]) + Sync) -> bool {
    // a sample tells whether rows mostly share groups
    let sample = PIECE.min(ids.len());
    number(0..sample, &mut ids[..sample]);
    if ids[..sample].iter().max().map_or(0, |&m| m + 1) > sample / 8 {
        return false;
    }
    let size = ids.len().div_ceil(rayon::current_num_threads() * 2).max(PIECE);
    // the first row of each group of each piece
    let firsts: Vec<Vec<usize>> = ids
        .par_chunks_mut(size)
        .enumerate()
        .map(|(p, ids)| {
            number(p * size..p * size + ids.len(), ids);
            let mut found = vec![];
            for (r, &id) in ids.iter().enumerate() {
                if id == found.len() {
                    found.push(p * size + r);
                }
            }
            found
        })
        .collect();
    let mut seen: HashMap<Vec<u64>, usize> = HashMap::new();
    // for each piece, the number of each of its groups among all rows
    let numbers: Vec<Vec<usize>> = firsts
        .iter()
        .map(|rows| {
            let mut number_of = |row: usize| {
                let next = seen.len();
                *seen.entry(keys.iter().map(|k| k.as_ref()[row]).collect()).or_insert(next)
            };
            rows.iter().map(|&r| number_of(r)).collect()
        })
        .collect();
    ids.par_chunks_mut(size).zip(&numbers).for_each(|(ids, numbers)| ids.iter_mut().for_each(|id| *id = numbers[*id]));
    true
}
