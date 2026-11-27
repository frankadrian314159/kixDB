//! A list of strings with their text in one block (`ColVec::Strings`), which is how a table keeps a column of strings:
//! 8 bytes a string and the text, against some 80 and the text as a list of character vectors. To `q` it is that list.
//! Verbs that are not taught to read it are given the list (`ops::UNPACKING`).
use crate::types::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Strings {
    /// Where each string ends in `bytes`.
    ends: Vec<u64>,
    bytes: Vec<u8>,
}

impl Strings {
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    pub fn get(&self, i: usize) -> &[u8] {
        let from = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.bytes[from..self.ends[i] as usize]
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }

    /// The bytes of heap the strings take.
    pub fn bytes_held(&self) -> usize {
        self.ends.len() * 8 + self.bytes.len()
    }

    pub fn push(&mut self, s: &[u8]) {
        self.bytes.extend_from_slice(s);
        self.ends.push(self.bytes.len() as u64);
    }

    pub fn extend_from(&mut self, other: &Strings) {
        let base = self.bytes.len() as u64;
        self.bytes.extend_from_slice(&other.bytes);
        self.ends.extend(other.ends.iter().map(|end| base + end));
    }

    /// The strings of these lengths that `bytes` holds, one after another.
    pub fn from_parts(lengths: impl IntoIterator<Item = u32>, bytes: Vec<u8>) -> Option<Strings> {
        let mut end = 0;
        let ends: Vec<u64> = lengths.into_iter().map(|n| {
            end += u64::from(n);
            end
        }).collect();
        (end == bytes.len() as u64).then_some(Strings { ends, bytes })
    }

    pub fn lengths(&self) -> impl Iterator<Item = u32> + '_ {
        self.iter().map(|s| s.len() as u32)
    }

    pub fn text(&self) -> &[u8] {
        &self.bytes
    }
}

impl<'a> FromIterator<&'a [u8]> for Strings {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(items: I) -> Self {
        let mut out = Strings::default();
        items.into_iter().for_each(|s| out.push(s));
        // (a table keeps what it is given, so no room is left over)
        out.bytes.shrink_to_fit();
        out.ends.shrink_to_fit();
        out
    }
}

/// In files, logs and messages between the nodes of a cluster: the lengths and the text.
impl Serialize for Strings {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Parts<'a>(Vec<u32>, #[serde(with = "crate::bytes")] &'a Vec<u8>);
        Parts(self.lengths().collect(), &self.bytes).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Strings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Parts(Vec<u32>, #[serde(with = "crate::bytes")] Vec<u8>);
        let Parts(lengths, bytes) = Parts::deserialize(deserializer)?;
        Strings::from_parts(lengths, bytes).ok_or_else(|| serde::de::Error::custom("the strings' lengths do not fit their text"))
    }
}

/// A string as the value it is.
pub fn value(s: &[u8]) -> K {
    K::Vector(ColVec::Char(Attr::None, Arc::new(s.to_vec())))
}

/// The text of a value that is a string with no attribute: what a packed list can hold.
fn text(item: &K) -> Option<&[u8]> {
    match item {
        K::Vector(ColVec::Char(Attr::None, bytes)) => Some(bytes),
        _ => None,
    }
}

impl ColVec {
    /// A column of `strings`. (There is no empty one: that is an empty general list, as before.)
    pub fn of_strings(attr: Attr, strings: Strings) -> ColVec {
        match strings.is_empty() {
            true => ColVec::General(attr, Arc::new(vec![])),
            false => ColVec::Strings(attr, Arc::new(strings)),
        }
    }

    /// A general list, or one of strings, which is a general list to `q`.
    pub fn is_list(&self) -> bool {
        matches!(self, ColVec::General(..) | ColVec::Strings(..))
    }

    /// The strings of a column that holds nothing but strings, in the packed form.
    pub fn packed(&self) -> Option<Cow<'_, ColVec>> {
        match self {
            ColVec::Strings(..) => Some(Cow::Borrowed(self)),
            ColVec::General(attr, items) if !items.is_empty() && items.iter().all(|i| text(i).is_some()) => {
                Some(Cow::Owned(ColVec::Strings(*attr, Arc::new(items.iter().filter_map(text).collect()))))
            }
            _ => None,
        }
    }

    /// Keep the strings of a column that holds nothing else in the packed form.
    pub fn pack(&mut self) {
        if let ColVec::General(..) = self {
            if let Some(Cow::Owned(packed)) = self.packed() {
                *self = packed;
            }
        }
    }

    /// The column with packed strings made the general list that they stand for.
    pub fn listed(&self) -> Cow<'_, ColVec> {
        match self {
            ColVec::Strings(attr, s) => Cow::Owned(ColVec::General(*attr, Arc::new(crate::engine::par::map(&(0..s.len()).collect::<Vec<_>>(), |&i| value(s.get(i)))))),
            other => Cow::Borrowed(other),
        }
    }
}

impl K {
    /// Keep a table's columns of strings in the packed form.
    pub fn pack(&mut self) {
        let loose = |f: &Flip| f.vals.iter().any(|c| matches!(c, ColVec::General(..)));
        match self {
            K::Table(t) if loose(t) => Arc::make_mut(t).vals.iter_mut().for_each(ColVec::pack),
            K::KeyedTable(t) if loose(&t.keys) || loose(&t.vals) => {
                let t = Arc::make_mut(t);
                t.keys.vals.iter_mut().chain(&mut t.vals.vals).for_each(ColVec::pack);
            }
            _ => {}
        }
    }

    fn has_packed(&self) -> bool {
        let column = |c: &ColVec| match c {
            ColVec::Strings(..) => true,
            ColVec::General(_, items) => items.iter().any(K::has_packed),
            _ => false,
        };
        match self {
            K::Vector(c) => column(c),
            K::Table(t) => t.vals.iter().any(column),
            K::KeyedTable(t) => t.keys.vals.iter().chain(&t.vals.vals).any(column),
            K::Dict(d) => d.keys.has_packed() || d.vals.has_packed(),
            _ => false,
        }
    }

    fn listed(&self) -> K {
        let column = |c: &ColVec| match c {
            ColVec::General(attr, items) => ColVec::General(*attr, Arc::new(items.iter().map(K::listed).collect())),
            other => other.listed().into_owned(),
        };
        let flip = |f: &Flip| Flip { cols: f.cols.clone(), vals: f.vals.iter().map(column).collect() };
        match self {
            K::Vector(c) => K::Vector(column(c)),
            K::Table(t) => K::Table(Arc::new(flip(t))),
            K::KeyedTable(t) => K::KeyedTable(Arc::new(KeyedTable { keys: flip(&t.keys), vals: flip(&t.vals) })),
            K::Dict(d) => K::Dict(Arc::new(Dict { keys: Box::new(d.keys.listed()), vals: Box::new(d.vals.listed()) })),
            other => other.clone(),
        }
    }

    /// The value with no packed strings anywhere in it.
    pub fn unpacked(&self) -> Cow<'_, K> {
        match self.has_packed() {
            true => Cow::Owned(self.listed()),
            false => Cow::Borrowed(self),
        }
    }
}
