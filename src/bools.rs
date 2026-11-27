//! Bit-packed boolean column storage (`Bools`), storing 64 booleans per `u64` word (LSB-first).
//!
//! Invariant: `words.len() == len.div_ceil(64)`, and any unused high bits in `words.last()` are `0`.
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bools {
    len: usize,
    words: Vec<u64>,
}

#[inline]
fn mask_last(len: usize, words: &mut [u64]) {
    let rem = len & 63;
    if rem != 0 {
        if let Some(last) = words.last_mut() {
            *last &= (1u64 << rem) - 1;
        }
    }
}

impl Bools {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(bits: usize) -> Self {
        Self {
            len: 0,
            words: Vec::with_capacity(bits.div_ceil(64)),
        }
    }

    pub fn from_words(len: usize, mut words: Vec<u64>) -> Self {
        words.resize(len.div_ceil(64), 0);
        mask_last(len, &mut words);
        Self { len, words }
    }

    pub fn repeat(val: bool, len: usize) -> Self {
        let mut words = vec![if val { u64::MAX } else { 0 }; len.div_ceil(64)];
        mask_last(len, &mut words);
        Self { len, words }
    }

    /// Pack `len` booleans generated sequentially in 64-element blocks without an intermediate `Vec<bool>`.
    #[inline]
    pub fn from_fn(len: usize, mut f: impl FnMut(usize) -> bool) -> Self {
        let full = len / 64;
        let rem = len & 63;
        let mut words = Vec::with_capacity(len.div_ceil(64));
        for wi in 0..full {
            let base = wi * 64;
            let mut w = 0u64;
            for bit in 0..64 {
                w |= u64::from(f(base + bit)) << bit;
            }
            words.push(w);
        }
        if rem != 0 {
            let base = full * 64;
            let mut w = 0u64;
            for bit in 0..rem {
                w |= u64::from(f(base + bit)) << bit;
            }
            words.push(w);
        }
        Self { len, words }
    }

    pub fn from_slice(xs: &[bool]) -> Self {
        Self::from_fn(xs.len(), |i| xs[i])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    #[inline]
    pub fn into_words(self) -> Vec<u64> {
        self.words
    }

    #[inline]
    pub fn bytes_held(&self) -> usize {
        self.words.len() * 8
    }

    /// Raw little-endian packed bytes (matches Apache Arrow `BooleanBuffer` layout).
    #[cfg(target_endian = "little")]
    #[inline]
    pub fn as_raw_bytes(&self) -> &[u8] {
        // SAFETY: `u64` is plain bits with alignment >= `u8` and `words.len() * 8` initialized bytes.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.words.len() * 8) }
    }

    #[inline]
    pub fn get(&self, i: usize) -> bool {
        (self.words[i >> 6] >> (i & 63)) & 1 != 0
    }

    #[inline]
    pub fn set(&mut self, i: usize, val: bool) {
        let mask = 1u64 << (i & 63);
        if val {
            self.words[i >> 6] |= mask;
        } else {
            self.words[i >> 6] &= !mask;
        }
    }

    #[inline]
    pub fn push(&mut self, val: bool) {
        let bit = self.len & 63;
        if bit == 0 {
            self.words.push(u64::from(val));
        } else if val {
            *self.words.last_mut().unwrap() |= 1u64 << bit;
        }
        self.len += 1;
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + ExactSizeIterator + '_ {
        (0..self.len).map(move |i| self.get(i))
    }

    #[inline]
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.words.iter().any(|&w| w != 0)
    }

    #[inline]
    pub fn all(&self) -> bool {
        let full = self.len / 64;
        if self.words[..full].iter().any(|&w| w != u64::MAX) {
            return false;
        }
        let rem = self.len & 63;
        rem == 0 || self.words[full] == (1u64 << rem) - 1
    }

    pub fn not(&self) -> Self {
        let mut words: Vec<u64> = self.words.iter().map(|&w| !w).collect();
        mask_last(self.len, &mut words);
        Self { len: self.len, words }
    }

    pub fn bit_and(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        Self {
            len: self.len,
            words: self.words.iter().zip(&other.words).map(|(&a, &b)| a & b).collect(),
        }
    }

    pub fn bit_or(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        Self {
            len: self.len,
            words: self.words.iter().zip(&other.words).map(|(&a, &b)| a | b).collect(),
        }
    }

    pub fn bit_xor(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        Self {
            len: self.len,
            words: self.words.iter().zip(&other.words).map(|(&a, &b)| a ^ b).collect(),
        }
    }

    pub fn bit_eq(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        let mut words: Vec<u64> = self.words.iter().zip(&other.words).map(|(&a, &b)| !(a ^ b)).collect();
        mask_last(self.len, &mut words);
        Self { len: self.len, words }
    }

    /// First index holding `val`, scanned 64 bits at a time.
    pub fn first_pos(&self, val: bool) -> Option<usize> {
        if val {
            self.words
                .iter()
                .enumerate()
                .find(|(_, &w)| w != 0)
                .map(|(wi, &w)| wi * 64 + w.trailing_zeros() as usize)
        } else {
            let full = self.len / 64;
            if let Some((wi, &w)) = self.words[..full].iter().enumerate().find(|(_, &w)| w != u64::MAX) {
                return Some(wi * 64 + (!w).trailing_zeros() as usize);
            }
            let rem = self.len & 63;
            if rem != 0 {
                let z = (!self.words[full]).trailing_zeros() as usize;
                if z < rem {
                    return Some(full * 64 + z);
                }
            }
            None
        }
    }

    /// Whether bits are non-decreasing (`0...01...1`), checked 64 bits at a time.
    pub fn is_sorted(&self) -> bool {
        let Some(first_one) = self.first_pos(true) else {
            return true;
        };
        let wi = first_one >> 6;
        let rem = self.len & 63;
        let last_wi = self.words.len() - 1;
        let mut expected = !0u64 << (first_one & 63);
        if wi == last_wi && rem != 0 {
            expected &= (1u64 << rem) - 1;
        }
        if self.words[wi] != expected {
            return false;
        }
        let full = self.len / 64;
        if wi + 1 < full && self.words[wi + 1..full].iter().any(|&w| w != u64::MAX) {
            return false;
        }
        wi == last_wi || rem == 0 || self.words[last_wi] == (1u64 << rem) - 1
    }

    /// Whether `self` starts with `prefix`, checked 64 bits at a time.
    pub fn begins_with(&self, prefix: &Self) -> bool {
        if prefix.len > self.len {
            return false;
        }
        let full = prefix.len / 64;
        if self.words[..full] != prefix.words[..full] {
            return false;
        }
        let rem = prefix.len & 63;
        rem == 0 || (self.words[full] & ((1u64 << rem) - 1)) == prefix.words[full]
    }

    /// Contiguous bit-slice `[start .. start + len]` extracted at word granularity.
    pub fn slice(&self, start: usize, len: usize) -> Self {
        if len == 0 {
            return Self::new();
        }
        debug_assert!(start + len <= self.len);
        let word_off = start >> 6;
        let bit_off = start & 63;
        let nwords = len.div_ceil(64);
        let mut words = if bit_off == 0 {
            self.words[word_off..word_off + nwords].to_vec()
        } else {
            (0..nwords)
                .map(|i| {
                    let lo = self.words[word_off + i] >> bit_off;
                    let hi = self.words.get(word_off + i + 1).copied().unwrap_or(0) << (64 - bit_off);
                    lo | hi
                })
                .collect()
        };
        mask_last(len, &mut words);
        Self { len, words }
    }

    /// Append `other`'s bits at word granularity.
    pub fn extend_from(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        let new_len = self.len + other.len;
        let shift = self.len & 63;
        if shift == 0 {
            self.words.extend_from_slice(&other.words);
        } else {
            let needed = new_len.div_ceil(64);
            self.words.reserve(needed.saturating_sub(self.words.len()));
            for &w in &other.words {
                *self.words.last_mut().unwrap() |= w << shift;
                if self.words.len() < needed {
                    self.words.push(w >> (64 - shift));
                }
            }
            self.words.truncate(needed);
            mask_last(new_len, &mut self.words);
        }
        self.len = new_len;
    }
}

impl Serialize for Bools {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let nbytes = self.len.div_ceil(8);
        #[cfg(target_endian = "little")]
        {
            #[derive(Serialize)]
            struct Packed<'a>(usize, #[serde(with = "crate::bytes")] &'a [u8]);
            Packed(self.len, &self.as_raw_bytes()[..nbytes]).serialize(serializer)
        }
        #[cfg(not(target_endian = "little"))]
        {
            #[derive(Serialize)]
            struct Packed(usize, #[serde(with = "crate::bytes")] Vec<u8>);
            let bytes: Vec<u8> = self.words.iter().flat_map(|w| w.to_le_bytes()).take(nbytes).collect();
            Packed(self.len, bytes).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Bools {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Packed(usize, #[serde(with = "crate::bytes")] Vec<u8>);
        let Packed(len, bytes) = Packed::deserialize(deserializer)?;
        if bytes.len() != len.div_ceil(8) {
            return Err(serde::de::Error::custom("boolean byte count does not match bit length"));
        }
        let mut words: Vec<u64> = bytes
            .chunks(8)
            .map(|c| {
                let mut b = [0u8; 8];
                b[..c.len()].copy_from_slice(c);
                u64::from_le_bytes(b)
            })
            .collect();
        mask_last(len, &mut words);
        Ok(Self { len, words })
    }
}

impl FromIterator<bool> for Bools {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lo, _) = it.size_hint();
        let mut out = Self::with_capacity(lo);
        for b in it {
            out.push(b);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Bools;

    #[test]
    fn word_boundaries_slices_extends_and_bitwise_ops_match_vec_bool() {
        for n in [0, 1, 63, 64, 65, 127, 128, 129, 300] {
            let a_vec: Vec<bool> = (0..n).map(|i| (i * 7 + 3) % 5 < 2).collect();
            let b_vec: Vec<bool> = (0..n).map(|i| (i * 11 + 1) % 3 == 0).collect();
            let a = Bools::from_slice(&a_vec);
            let b = Bools::from_slice(&b_vec);

            assert_eq!(a.iter().collect::<Vec<_>>(), a_vec);
            assert_eq!(a.count_ones(), a_vec.iter().filter(|&&x| x).count());
            assert_eq!(a.any(), a_vec.iter().any(|&x| x));
            assert_eq!(a.all(), a_vec.iter().all(|&x| x));
            assert_eq!(Bools::repeat(true, n).all(), true);
            assert_eq!(a.not().iter().collect::<Vec<_>>(), a_vec.iter().map(|&x| !x).collect::<Vec<_>>());
            assert_eq!(a.bit_and(&b).iter().collect::<Vec<_>>(), a_vec.iter().zip(&b_vec).map(|(&x, &y)| x && y).collect::<Vec<_>>());
            assert_eq!(a.bit_or(&b).iter().collect::<Vec<_>>(), a_vec.iter().zip(&b_vec).map(|(&x, &y)| x || y).collect::<Vec<_>>());
            assert_eq!(a.bit_xor(&b).iter().collect::<Vec<_>>(), a_vec.iter().zip(&b_vec).map(|(&x, &y)| x ^ y).collect::<Vec<_>>());
            assert_eq!(a.bit_eq(&b).iter().collect::<Vec<_>>(), a_vec.iter().zip(&b_vec).map(|(&x, &y)| x == y).collect::<Vec<_>>());
            assert_eq!(a.first_pos(true), a_vec.iter().position(|&x| x));
            assert_eq!(a.first_pos(false), a_vec.iter().position(|&x| !x));

            for split in [0, n / 3, n.min(64), n] {
                let left = a.slice(0, split);
                let right = a.slice(split, n - split);
                assert_eq!(left.iter().collect::<Vec<_>>(), a_vec[..split]);
                assert_eq!(right.iter().collect::<Vec<_>>(), a_vec[split..]);
                assert!(a.begins_with(&left));
                let mut joined = left.clone();
                joined.extend_from(&right);
                assert_eq!(joined, a);
            }

            for k in [0, 1, n / 2, n] {
                let mut sorted_vec = vec![false; k];
                sorted_vec.resize(n, true);
                let s = Bools::from_slice(&sorted_vec);
                assert!(s.is_sorted());
                if n >= 2 {
                    let mut unsorted = s.clone();
                    unsorted.set(0, true);
                    unsorted.set(n - 1, false);
                    assert!(!unsorted.is_sorted());
                }
            }

            let encoded = postcard::to_allocvec(&a).unwrap();
            let decoded: Bools = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(decoded, a);
        }
    }
}
