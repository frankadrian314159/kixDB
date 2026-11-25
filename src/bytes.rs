//! `#[serde(with = "...")]` for byte buffers: (de)serialized as one run instead of byte by byte, many times faster and,
//! in `postcard`, encoded the same.
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserializer, Serializer};

pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(bytes)
}

struct Bytes;

impl<'de> Visitor<'de> for Bytes {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("bytes")
    }

    fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(v)
    }

    /// (formats without a byte type give a list)
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        let mut out = Vec::new();
        while let Some(b) = seq.next_element()? {
            out.push(b);
        }
        Ok(out)
    }
}

pub fn deserialize<'de, D: Deserializer<'de>, T: From<Vec<u8>>>(deserializer: D) -> Result<T, D::Error> {
    deserializer.deserialize_byte_buf(Bytes).map(T::from)
}
