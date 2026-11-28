//! Streaming tokenizer for `q` and `qSQL`.
//!
//! Handles `q`'s whitespace-sensitive rules in a single zero-copy pass over `&str`:
//! - Adjacent scalar literals (`1 2 3`, `1.5 2.5`, `0101b`, `0x01ff`, `` `a`b`c ``, `2026.11.24 2026.11.25`) coalesce into
//!   single typed vector tokens directly without an intermediate `Vec<K>` for numeric vectors.
//! - `/` at column 0 or after whitespace begins a line comment; immediately after an expression (`+/`, `sum/`) it is the
//!   `Over` adverb.
//! - `\` at column 0 is a system command (`\t`, `\l`, `\\`, or bare `\` abort); elsewhere it is the `Scan` adverb.
//! - `-` immediately before digits is part of a negative literal only when not immediately preceded by a value token
//!   (so `10-3` is `10 - 3`, `1 -2` is the vector `1 -2`, and `1 - 2` is `1 - 2`).
use crate::error::{KError, KResult};
use crate::sym::{intern, SymId};
use crate::types::*;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum Tok<'a> {
    Lit(K),
    Ident(&'a str),
    Op(&'a str),
    /// The `.` of `value.field`, written without spaces.
    Field,
    Adverb(AdverbKind),
    Colon,           // :
    DoubleColon,     // ::
    OpColon(&'a str), // +: ,: -: *: %: &: |:
    Semi,            // ;
    Comma,           // , (also binary concat operator outside qSQL clauses)
    LParen,
    RParen,
    LBrack,
    RBrack,
    LBrace(usize),
    RBrace(usize),
    CondOpen, // $[
    Quote,    // ' (signal when prefix, Each adverb when postfix)
    SysCmd { cmd: &'a str, arg: &'a str },
    Newline,
    Eof,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrevKind {
    Start,
    Value, // number, string, sym, ident, ), ], }
    Other, // (, [, ;, :, op, whitespace-separated
}

#[derive(Clone, Copy)]
pub struct Lexer<'a> {
    src: &'a str,
    pos: usize,
    at_line_start: bool,
    after_ws: bool,
    prev: PrevKind,
    emitted_any: bool,
    last_newline: bool,
    last_op_or_comma: bool,
    brace_depth: u32,
    done: bool,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self {
            src,
            pos: 0,
            at_line_start: true,
            after_ws: true,
            prev: PrevKind::Start,
            emitted_any: false,
            last_newline: false,
            last_op_or_comma: false,
            brace_depth: 0,
            done: false,
        }
    }

    #[inline]
    fn emit(&mut self, tok: Tok<'a>, after_ws: bool, prev: PrevKind) -> Tok<'a> {
        self.after_ws = after_ws;
        self.prev = prev;
        self.last_newline = matches!(tok, Tok::Newline);
        self.last_op_or_comma = matches!(tok, Tok::Op(_) | Tok::Comma);
        self.emitted_any = true;
        tok
    }

    pub fn next_tok(&mut self) -> KResult<Tok<'a>> {
        if self.done {
            return Ok(Tok::Eof);
        }
        let src = self.src;
        let bytes = src.as_bytes();
        let len = bytes.len();

        while self.pos < len {
            let b = bytes[self.pos];

            // Newlines
            if b == b'\r' || b == b'\n' {
                if b == b'\r' && self.pos + 1 < len && bytes[self.pos + 1] == b'\n' {
                    self.pos += 2;
                } else {
                    self.pos += 1;
                }
                self.at_line_start = true;
                self.after_ws = true;
                self.prev = PrevKind::Start;
                if self.emitted_any && !self.last_newline {
                    self.last_newline = true;
                    self.last_op_or_comma = false;
                    return Ok(Tok::Newline);
                }
                continue;
            }

            // Spaces / tabs
            if b == b' ' || b == b'\t' {
                while self.pos < len && (bytes[self.pos] == b' ' || bytes[self.pos] == b'\t') {
                    self.pos += 1;
                }
                self.after_ws = true;
                continue;
            }

            // Line-start backslash system command (`\t ...`, `\l ...`, `\\`, bare `\`)
            if self.at_line_start && b == b'\\' {
                self.pos += 1;
                let start = self.pos;
                while self.pos < len
                    && bytes[self.pos] != b'\r'
                    && bytes[self.pos] != b'\n'
                    && !(self.brace_depth > 0 && bytes[self.pos] == b'}')
                {
                    self.pos += 1;
                }
                let line = src[start..self.pos].trim();
                if line.is_empty() {
                    if self.brace_depth > 0 {
                        return Err(KError::syntax("unclosed '{' in lambda"));
                    }
                    // Bare `\` on its own line stops loading the rest of a script in kdb+.
                    self.done = true;
                    return Ok(Tok::Eof);
                }
                let tok = if line == "\\" {
                    Tok::SysCmd { cmd: "\\", arg: "" }
                } else {
                    let (cmd, arg) = match line.find(|c: char| c.is_whitespace()) {
                        Some(p) => (&line[..p], line[p..].trim()),
                        None => (line, ""),
                    };
                    Tok::SysCmd { cmd, arg }
                };
                self.at_line_start = false;
                return Ok(self.emit(tok, false, PrevKind::Other));
            }

            if b == b'/' && self.at_line_start && src[self.pos + 1..].split('\n').next().is_some_and(|rest| rest.trim().is_empty()) {
                let mut lines = src[self.pos..].split_inclusive('\n');
                self.pos += lines.next().map_or(0, str::len);
                for line in lines {
                    self.pos += line.len();
                    if line.trim_end() == "\\" {
                        break;
                    }
                }
                continue;
            }

            // Comments: `/` at line start or preceded by whitespace
            if b == b'/' && (self.at_line_start || self.after_ws) {
                if self.pos + 1 < len && bytes[self.pos + 1] == b':' && !self.at_line_start {
                    self.pos += 2;
                    self.at_line_start = false;
                    return Ok(self.emit(Tok::Adverb(AdverbKind::EachRight), false, PrevKind::Value));
                }
                while self.pos < len && bytes[self.pos] != b'\r' && bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }

            self.at_line_start = false;

            // Lambda braces `{ ... }` streamed directly without pre-scanning or nested token buffers
            if b == b'{' {
                let start = self.pos;
                self.pos += 1;
                self.brace_depth += 1;
                self.after_ws = true;
                self.prev = PrevKind::Start;
                self.last_newline = true;
                self.last_op_or_comma = false;
                self.emitted_any = true;
                return Ok(Tok::LBrace(start));
            }
            if b == b'}' {
                if self.brace_depth == 0 {
                    return Err(KError::syntax("unexpected character '}'"));
                }
                let end = self.pos;
                self.pos += 1;
                self.brace_depth -= 1;
                return Ok(self.emit(Tok::RBrace(end), false, PrevKind::Value));
            }

            // String / char literal `"..."`
            if b == b'"' {
                let (k, next_i) = lex_string(src, self.pos)?;
                self.pos = next_i;
                return Ok(self.emit(Tok::Lit(k), false, PrevKind::Value));
            }

            // Symbol literal(s) `` `a`b`c ``
            if b == b'`' {
                let (k, next_i) = lex_symbols(src, self.pos);
                self.pos = next_i;
                return Ok(self.emit(Tok::Lit(k), false, PrevKind::Value));
            }

            // Numeric / temporal / boolean / byte literal (or space-separated vector of them!)
            let i = self.pos;
            let number_at = |j: usize| j < len && (bytes[j].is_ascii_digit() || (bytes[j] == b'.' && j + 1 < len && bytes[j + 1].is_ascii_digit()));
            let neg_num_ok = b == b'-' && number_at(i + 1) && (self.after_ws || self.prev != PrevKind::Value);
            let point_ok = b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_digit() && (self.after_ws || self.prev != PrevKind::Value);
            if b.is_ascii_digit() || neg_num_ok || point_ok {
                if i + 1 < len && matches!(b, b'0' | b'1' | b'2') && bytes[i + 1] == b':' {
                    self.pos = i + 2;
                    return Ok(self.emit(Tok::Op(&src[i..i + 2]), false, PrevKind::Value));
                }
                let (k, next_i) = lex_numeric_sequence(src, i)?;
                self.pos = next_i;
                return Ok(self.emit(Tok::Lit(k), false, PrevKind::Value));
            }

            // `$[` conditional vs `$` cast operator
            if b == b'$' {
                if i + 1 < len && bytes[i + 1] == b'[' && (self.after_ws || self.prev != PrevKind::Value) {
                    self.pos += 2;
                    return Ok(self.emit(Tok::CondOpen, false, PrevKind::Other));
                }
                let tok = if i + 1 < len && bytes[i + 1] == b':' {
                    self.pos += 2;
                    Tok::OpColon("$")
                } else {
                    self.pos += 1;
                    Tok::Op("$")
                };
                return Ok(self.emit(tok, false, PrevKind::Other));
            }

            // Adverbs: `/:`, `\:`, `':`, `/`, `\`, `'`
            if b == b'/' {
                let tok = if i + 1 < len && bytes[i + 1] == b':' {
                    self.pos += 2;
                    Tok::Adverb(AdverbKind::EachRight)
                } else {
                    self.pos += 1;
                    Tok::Adverb(AdverbKind::Over)
                };
                return Ok(self.emit(tok, false, PrevKind::Value));
            }
            if b == b'\\' {
                let tok = if i + 1 < len && bytes[i + 1] == b':' {
                    self.pos += 2;
                    Tok::Adverb(AdverbKind::EachLeft)
                } else {
                    self.pos += 1;
                    Tok::Adverb(AdverbKind::Scan)
                };
                return Ok(self.emit(tok, false, PrevKind::Value));
            }
            if b == b'\'' {
                if i + 1 < len && bytes[i + 1] == b':' {
                    self.pos += 2;
                    return Ok(self.emit(Tok::Adverb(AdverbKind::EachPrior), false, PrevKind::Value));
                } else if !self.after_ws && (self.prev == PrevKind::Value || self.last_op_or_comma) {
                    self.pos += 1;
                    return Ok(self.emit(Tok::Adverb(AdverbKind::Each), false, PrevKind::Value));
                } else {
                    self.pos += 1;
                    return Ok(self.emit(Tok::Quote, false, PrevKind::Other));
                }
            }

            // Delimiters
            match b {
                b'(' => {
                    self.pos += 1;
                    return Ok(self.emit(Tok::LParen, false, PrevKind::Other));
                }
                b')' => {
                    self.pos += 1;
                    return Ok(self.emit(Tok::RParen, false, PrevKind::Value));
                }
                b'[' => {
                    self.pos += 1;
                    return Ok(self.emit(Tok::LBrack, false, PrevKind::Other));
                }
                b']' => {
                    self.pos += 1;
                    return Ok(self.emit(Tok::RBrack, false, PrevKind::Value));
                }
                b';' => {
                    self.pos += 1;
                    return Ok(self.emit(Tok::Semi, false, PrevKind::Other));
                }
                b',' => {
                    let tok = if i + 1 < len && bytes[i + 1] == b':' {
                        self.pos += 2;
                        Tok::OpColon(",")
                    } else {
                        self.pos += 1;
                        Tok::Comma
                    };
                    return Ok(self.emit(tok, false, PrevKind::Other));
                }
                b':' => {
                    let tok = if i + 1 < len && bytes[i + 1] == b':' {
                        self.pos += 2;
                        Tok::DoubleColon
                    } else {
                        self.pos += 1;
                        Tok::Colon
                    };
                    return Ok(self.emit(tok, false, PrevKind::Other));
                }
                _ => {}
            }

            // Identifiers & `.namespace.name`
            let own_name = || i > 0 && bytes[i - 1] == b']' && matches!(bytes.get(i + 1..i + 3), Some([b'z' | b'Q' | b'h' | b'j', b'.']));
            if b.is_ascii_alphabetic()
                || (b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() && (self.after_ws || self.prev != PrevKind::Value || own_name()))
            {
                let start = i;
                self.pos += 1;
                if b == b'.' {
                    while self.pos < len && (bytes[self.pos].is_ascii_alphanumeric() || bytes[self.pos] == b'_' || bytes[self.pos] == b'.') {
                        self.pos += 1;
                    }
                } else {
                    while self.pos < len && (bytes[self.pos].is_ascii_alphanumeric() || bytes[self.pos] == b'_') {
                        self.pos += 1;
                    }
                }
                return Ok(self.emit(Tok::Ident(&src[start..self.pos]), false, PrevKind::Value));
            }

            // Dot field access `.field` immediately after a value (`time.minute`, `t.price`) vs `.` apply operator
            if b == b'.' {
                if !self.after_ws && self.prev == PrevKind::Value && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
                    self.pos += 1;
                    return Ok(self.emit(Tok::Field, false, PrevKind::Other));
                }
                let tok = if i + 1 < len && bytes[i + 1] == b':' {
                    self.pos += 2;
                    Tok::OpColon(".")
                } else {
                    self.pos += 1;
                    Tok::Op(".")
                };
                return Ok(self.emit(tok, false, PrevKind::Other));
            }

            // Multi-char comparison operators (`<=`, `>=`, `<>`) and compound assignment (`+:`, `-:`, `*:`, `%:`, `&:`, `|:`, ...)
            if b == b'<' && i + 1 < len && (bytes[i + 1] == b'=' || bytes[i + 1] == b'>') {
                self.pos += 2;
                return Ok(self.emit(Tok::Op(&src[i..i + 2]), false, PrevKind::Other));
            }
            if b == b'>' && i + 1 < len && bytes[i + 1] == b'=' {
                self.pos += 2;
                return Ok(self.emit(Tok::Op(">="), false, PrevKind::Other));
            }

            if matches!(b, b'+' | b'-' | b'*' | b'%' | b'&' | b'|' | b'^' | b'=' | b'<' | b'>' | b'~' | b'!' | b'#' | b'_' | b'@' | b'?') {
                let op_slice = &src[i..i + 1];
                let tok = if i + 1 < len && bytes[i + 1] == b':' && matches!(b, b'+' | b'-' | b'*' | b'%' | b'&' | b'|' | b'^' | b'#' | b'_' | b'!' | b'@') {
                    self.pos += 2;
                    Tok::OpColon(op_slice)
                } else {
                    self.pos += 1;
                    Tok::Op(op_slice)
                };
                return Ok(self.emit(tok, false, PrevKind::Other));
            }

            return Err(KError::syntax(format!("unexpected character '{}'", b as char)));
        }

        if self.brace_depth > 0 {
            return Err(KError::syntax("unclosed '{' in lambda"));
        }
        self.done = true;
        Ok(Tok::Eof)
    }
}

pub fn tokenize(src: &str) -> KResult<Vec<Tok<'_>>> {
    let mut lex = Lexer::new(src);
    let mut out = Vec::new();
    loop {
        let t = lex.next_tok()?;
        let is_eof = matches!(t, Tok::Eof);
        out.push(t);
        if is_eof {
            return Ok(out);
        }
    }
}

fn lex_string(src: &str, start: usize) -> KResult<(K, usize)> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = start + 1;
    let mut buf = Vec::new();
    let mut closed = false;

    while i < len {
        let b = bytes[i];
        if b == b'"' {
            i += 1;
            closed = true;
            break;
        }
        if b == b'\\' {
            i += 1;
            if i >= len {
                return Err(KError::syntax("unterminated escape in string"));
            }
            match bytes[i] {
                b'n' => buf.push(b'\n'),
                b'r' => buf.push(b'\r'),
                b't' => buf.push(b'\t'),
                b'\\' => buf.push(b'\\'),
                b'"' => buf.push(b'"'),
                d if d.is_ascii_digit() => {
                    let s = i;
                    while i < len && i < s + 3 && bytes[i] >= b'0' && bytes[i] <= b'7' {
                        i += 1;
                    }
                    let v = u8::from_str_radix(&src[s..i], 8).unwrap_or(0);
                    buf.push(v);
                    continue;
                }
                other => buf.push(other),
            }
            i += 1;
        } else {
            buf.push(b);
            i += 1;
        }
    }
    if !closed {
        return Err(KError::syntax("unterminated string literal"));
    }
    let k = if buf.len() == 1 {
        K::Char(buf[0])
    } else {
        K::Vector(ColVec::Char(Attr::None, Arc::new(buf)))
    };
    Ok((k, i))
}

fn lex_symbols(src: &str, start: usize) -> (K, usize) {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = start;

    let lex_one = |mut i: usize| -> (SymId, usize) {
        i += 1;
        let s_start = i;
        if i < len && bytes[i] == b':' {
            i += 1;
            while i < len && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b';' | b')' | b']' | b'}' | b',' | b'`' | b'"') {
                i += 1;
            }
        } else {
            while i < len && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'.')) {
                i += 1;
            }
            if bytes[i..].starts_with(b"://") {
                while i < len && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b';' | b')' | b']' | b'}' | b',' | b'`' | b'"') {
                    i += 1;
                }
            }
        }
        (intern(&src[s_start..i]), i)
    };

    let (first, next_i) = lex_one(i);
    i = next_i;
    if i >= len || bytes[i] != b'`' {
        return (K::Sym(first), i);
    }
    let mut syms = vec![first];
    while i < len && bytes[i] == b'`' {
        let (s, next_i) = lex_one(i);
        syms.push(s);
        i = next_i;
    }
    (K::Vector(ColVec::Sym(Attr::None, Arc::new(syms))), i)
}

#[inline]
fn next_seq_item(src: &str, bytes: &[u8], pos: usize) -> Option<usize> {
    let len = bytes.len();
    let mut ws = pos;
    while ws < len && (bytes[ws] == b' ' || bytes[ws] == b'\t') {
        ws += 1;
    }
    if ws == pos || ws >= len {
        return None;
    }
    let next_b = bytes[ws];
    let digit_after = ws + 1 < len && bytes[ws + 1].is_ascii_digit();
    let negative_point = next_b == b'-' && ws + 2 < len && bytes[ws + 1] == b'.' && bytes[ws + 2].is_ascii_digit();
    let starts_num = next_b.is_ascii_digit() || (matches!(next_b, b'-' | b'.') && digit_after) || negative_point;
    if !starts_num {
        return None;
    }
    if ws + 1 < len && matches!(next_b, b'0' | b'1' | b'2') && bytes[ws + 1] == b':' {
        return None;
    }
    if ws + 2 <= len && (&src[ws..ws + 2] == "0x" || &src[ws..ws + 2] == "0X") {
        return None;
    }
    let mut bj = ws;
    while bj < len && (bytes[bj] == b'0' || bytes[bj] == b'1') {
        bj += 1;
    }
    if bj > ws && bj < len && bytes[bj] == b'b' && (bj + 1 == len || !bytes[bj + 1].is_ascii_alphanumeric()) {
        return None;
    }
    Some(ws)
}

#[inline]
fn i64_to_f64_lit(n: i64) -> f64 {
    match n {
        NULL_LONG => f64::NAN,
        INF_LONG => f64::INFINITY,
        NINF_LONG => f64::NEG_INFINITY,
        v => v as f64,
    }
}

/// Lex one scalar numeric/boolean/byte/temporal token and any space-separated peers that form a vector literal (`1 2 3`, `1.5 2.5`).
fn lex_numeric_sequence(src: &str, start: usize) -> KResult<(K, usize)> {
    let bytes = src.as_bytes();
    let len = bytes.len();

    // Hex byte literal `0x0102ff`
    if start + 2 <= len && (&src[start..start + 2] == "0x" || &src[start..start + 2] == "0X") {
        let mut i = start + 2;
        while i < len && bytes[i].is_ascii_hexdigit() {
            i += 1;
        }
        let hex = &src[start + 2..i];
        if hex.is_empty() {
            return Ok((K::Vector(ColVec::Byte(Attr::None, Arc::new(vec![]))), i));
        }
        let mut out = Vec::with_capacity((hex.len() + 1) / 2);
        let mut idx = 0;
        if hex.len() % 2 == 1 {
            out.push(u8::from_str_radix(&hex[..1], 16).map_err(|_| KError::syntax("bad hex byte"))?);
            idx = 1;
        }
        while idx < hex.len() {
            out.push(u8::from_str_radix(&hex[idx..idx + 2], 16).map_err(|_| KError::syntax("bad hex byte"))?);
            idx += 2;
        }
        let k = if out.len() == 1 && hex.len() <= 2 {
            K::Byte(out[0])
        } else {
            K::Vector(ColVec::Byte(Attr::None, Arc::new(out)))
        };
        return Ok((k, i));
    }

    // Bitstring boolean literal `0101b` / `1b` / `0b`
    {
        let mut j = start;
        while j < len && (bytes[j] == b'0' || bytes[j] == b'1') {
            j += 1;
        }
        if j > start && j < len && bytes[j] == b'b' && (j + 1 == len || !bytes[j + 1].is_ascii_alphanumeric()) {
            let blen = j - start;
            let k = if blen == 1 {
                K::Bool(bytes[start] == b'1')
            } else {
                K::Vector(ColVec::Bool(
                    Attr::None,
                    Arc::new(crate::bools::Bools::from_fn(blen, |i| bytes[start + i] == b'1')),
                ))
            };
            return Ok((k, j + 1));
        }
    }

    let (first_atom, mut pos, mut vec_suffix) = lex_one_number_or_temporal(src, start)?;
    if vec_suffix.is_some() {
        return Ok((apply_type_suffix_atom(first_atom, vec_suffix)?, pos));
    }
    let Some(mut ws) = next_seq_item(src, bytes, pos) else {
        return Ok((first_atom, pos));
    };

    // Fast single-pass typed vector accumulation for Long and Float vectors without `Vec<K>`
    enum Acc {
        Longs(Vec<i64>),
        Floats(Vec<f64>),
        Atoms(Vec<K>),
    }
    let mut time_kind;
    let mut acc = match first_atom {
        K::Long(n) => Acc::Longs(vec![n]),
        K::Float(f) => Acc::Floats(vec![f]),
        other => Acc::Atoms(vec![other]),
    };

    loop {
        let (atom, next_pos, suf) = lex_one_number_or_temporal(src, ws)?;
        time_kind = time_letter(&src[ws..next_pos]);
        pos = next_pos;
        acc = match (acc, atom) {
            (Acc::Longs(mut xs), K::Long(n)) if suf.is_none() => {
                xs.push(n);
                Acc::Longs(xs)
            }
            (Acc::Longs(xs), K::Float(f)) if suf.is_none() => {
                let mut fs: Vec<f64> = xs.into_iter().map(i64_to_f64_lit).collect();
                fs.push(f);
                Acc::Floats(fs)
            }
            (Acc::Floats(mut fs), K::Float(f)) if suf.is_none() => {
                fs.push(f);
                Acc::Floats(fs)
            }
            (Acc::Floats(mut fs), K::Long(n)) if suf.is_none() => {
                fs.push(i64_to_f64_lit(n));
                Acc::Floats(fs)
            }
            (Acc::Longs(xs), other) => {
                let mut atoms: Vec<K> = xs.into_iter().map(K::Long).collect();
                atoms.push(other);
                Acc::Atoms(atoms)
            }
            (Acc::Floats(fs), other) => {
                let mut atoms: Vec<K> = fs.into_iter().map(K::Float).collect();
                atoms.push(other);
                Acc::Atoms(atoms)
            }
            (Acc::Atoms(mut atoms), other) => {
                atoms.push(other);
                Acc::Atoms(atoms)
            }
        };
        if suf.is_some() {
            vec_suffix = suf;
            break;
        }
        let Some(next_ws) = next_seq_item(src, bytes, pos) else { break };
        ws = next_ws;
    }

    match acc {
        Acc::Longs(xs) => Ok((K::Vector(ColVec::Long(Attr::None, Arc::new(xs))), pos)),
        Acc::Floats(fs) => Ok((K::Vector(ColVec::Float(Attr::None, Arc::new(fs))), pos)),
        Acc::Atoms(mut atoms) => {
            let kinds: Vec<i16> = atoms.iter().map(|a| a.type_num()).collect();
            if let Some(kind) = time_kind.or_else(|| crate::temporal::finest(&kinds)) {
                atoms.iter_mut().filter(|a| a.type_num() != -kind).for_each(|a| *a = crate::temporal::convert(a, kind));
            }
            Ok((coalesce_numeric_vector(atoms, vec_suffix)?, pos))
        }
    }
}

/// The type a time ends by naming (`12:34u` is a minute).
fn time_letter(raw: &str) -> Option<i16> {
    let kind = "uvtn".find(*raw.as_bytes().last()? as char)?;
    raw.contains(':').then_some([17, 18, 19, 16][kind])
}

/// Lexes a single numeric or temporal scalar token starting at `start`.
fn lex_one_number_or_temporal(src: &str, start: usize) -> KResult<(K, usize, Option<char>)> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = start;
    if bytes[i] == b'-' {
        i += 1;
    }
    while i < len {
        let b = bytes[i];
        if b.is_ascii_alphanumeric() || b == b'.' || b == b':' {
            if b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
                break;
            }
            if b == b':' {
                let next_is_digit = i + 1 < len && bytes[i + 1].is_ascii_digit();
                if !next_is_digit {
                    break;
                }
            }
            if (b == b'e' || b == b'E') && i + 1 < len && (bytes[i + 1] == b'+' || bytes[i + 1] == b'-') && i + 2 < len && bytes[i + 2].is_ascii_digit() {
                i += 2;
                continue;
            }
            i += 1;
        } else if b == b'-' {
            if i - start == 8 && src[start..i].bytes().all(|c| c.is_ascii_hexdigit()) {
                let end_guid = start + 36;
                if end_guid <= len {
                    if let Some(g) = parse_guid(&src[start..end_guid]) {
                        return Ok((K::Guid(g), end_guid, None));
                    }
                }
            }
            break;
        } else {
            break;
        }
    }

    let raw = &src[start..i];
    parse_scalar_token(raw).map(|(k, suf)| (k, i, suf))
}

pub(crate) fn parse_scalar_token(raw: &str) -> KResult<(K, Option<char>)> {
    if raw.is_empty() {
        return Err(KError::syntax("empty numeric token"));
    }
    match raw {
        "0N" | "0Nj" => return Ok((K::Long(NULL_LONG), if raw.ends_with('j') { Some('j') } else { None })),
        "0W" | "0Wj" => return Ok((K::Long(INF_LONG), if raw.ends_with('j') { Some('j') } else { None })),
        "-0W" | "-0Wj" => return Ok((K::Long(NINF_LONG), if raw.ends_with('j') { Some('j') } else { None })),
        "0n" | "0Nf" => return Ok((K::Float(f64::NAN), if raw.ends_with('f') { Some('f') } else { None })),
        "0w" | "0Wf" => return Ok((K::Float(f64::INFINITY), if raw.ends_with('f') { Some('f') } else { None })),
        "-0w" | "-0Wf" => return Ok((K::Float(f64::NEG_INFINITY), if raw.ends_with('f') { Some('f') } else { None })),
        "0Nh" => return Ok((K::Short(NULL_SHORT), Some('h'))),
        "0Wh" => return Ok((K::Short(INF_SHORT), Some('h'))),
        "-0Wh" => return Ok((K::Short(NINF_SHORT), Some('h'))),
        "0Ni" => return Ok((K::Int(NULL_INT), Some('i'))),
        "0Wi" => return Ok((K::Int(INF_INT), Some('i'))),
        "-0Wi" => return Ok((K::Int(NINF_INT), Some('i'))),
        "0Ne" => return Ok((K::Real(f32::NAN), Some('e'))),
        "0We" => return Ok((K::Real(f32::INFINITY), Some('e'))),
        "-0We" => return Ok((K::Real(f32::NEG_INFINITY), Some('e'))),
        "0Ng" => return Ok((K::Guid(NULL_GUID), None)),
        _ => {}
    }
    if let Some(k) = temporal_null_or_infinity(raw) {
        return Ok((k, None));
    }

    // Datetime (`YYYY.MM.DDThh:mm:ss.mmm`)
    if let Some((date, time)) = raw.split_once('T').filter(|(date, _)| date.contains('.')) {
        let day = if time.is_empty() { 0 } else { parse_hms_nanos(time)? };
        return Ok((K::Datetime(f64::from(parse_date_str(date)?) + day as f64 / NANOS_PER_DAY as f64), None));
    }

    // A time followed by the letter of the type it is to have (`11:59:59.999u`)
    if let Some((body, kind)) = raw.char_indices().last().and_then(|(i, c)| Some((&raw[..i], "uvtn".find(c)?))) {
        if body.contains(':') {
            let (value, _) = parse_scalar_token(body)?;
            let kind = [17, 18, 19, 16][kind];
            return Ok((crate::temporal::convert(&value, kind), None));
        }
    }

    // Timestamp (`YYYY.MM.DDDhh:mm:ss[.nnnnnnnnn]`) or Timespan (`[+-][d]Dhh:mm:ss[.nnnnnnnnn]`)
    if let Some((left, right)) = raw.split_once('D') {
        if left.contains('.') {
            let days = parse_date_str(left)?;
            let ns_in_day = if right.is_empty() { 0 } else { parse_hms_nanos(right)? };
            let total_ns = (days as i64)
                .checked_mul(NANOS_PER_DAY)
                .and_then(|d| d.checked_add(ns_in_day))
                .ok_or_else(|| KError::syntax("timestamp overflow"))?;
            return Ok((K::Timestamp(total_ns), None));
        } else {
            let neg = left.starts_with('-');
            let days_abs: i64 = left.trim_start_matches('-').parse().map_err(|_| KError::syntax(format!("bad timespan '{raw}'")))?;
            let ns_part = if right.is_empty() { 0 } else { parse_hms_nanos(right)? };
            let mag = days_abs * NANOS_PER_DAY + ns_part;
            return Ok((K::Timespan(if neg { -mag } else { mag }), None));
        }
    }

    // Time / Second / Minute (`hh:mm`, `hh:mm:ss`, `hh:mm:ss.mmm`, `hh:mm:ss.nnnnnnnnn` -> Timespan if >3 fractional digits)
    if raw.contains(':') {
        let neg = raw.starts_with('-');
        let s = raw.trim_start_matches('-');
        let Some((hh_s, rest)) = s.split_once(':') else { unreachable!() };
        match rest.split_once(':') {
            None => {
                let hh: i32 = hh_s.parse().map_err(|_| KError::syntax(format!("bad minute '{raw}'")))?;
                let mm: i32 = rest.parse().map_err(|_| KError::syntax(format!("bad minute '{raw}'")))?;
                let mins = hh * 60 + mm;
                return Ok((K::Minute(if neg { -mins } else { mins }), None));
            }
            Some((mm_s, p2)) if !p2.contains(':') => {
                let hh: i64 = hh_s.parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                let mm: i64 = mm_s.parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                if let Some((ss_s, frac_s)) = p2.split_once('.') {
                    let ss: i64 = ss_s.parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                    if frac_s.len() <= 3 {
                        let mut ms_frac = 0i64;
                        for idx in 0..3 {
                            let b = frac_s.as_bytes().get(idx).copied().unwrap_or(b'0');
                            if !b.is_ascii_digit() {
                                return Err(KError::syntax(format!("bad time '{raw}'")));
                            }
                            ms_frac = ms_frac * 10 + i64::from(b - b'0');
                        }
                        let total_ms = ((hh * 3600 + mm * 60 + ss) * 1000 + ms_frac) as i32;
                        return Ok((K::Time(if neg { -total_ms } else { total_ms }), None));
                    } else {
                        let ns = parse_hms_nanos(s)?;
                        return Ok((K::Timespan(if neg { -ns } else { ns }), None));
                    }
                } else {
                    let ss: i32 = p2.parse().map_err(|_| KError::syntax(format!("bad second '{raw}'")))?;
                    let total_s = (hh as i32) * 3600 + (mm as i32) * 60 + ss;
                    return Ok((K::Second(if neg { -total_s } else { total_s }), None));
                }
            }
            _ => return Err(KError::syntax(format!("bad temporal literal '{raw}'"))),
        }
    }

    // Month (`YYYY.MMm`)
    if raw.ends_with('m') && raw.contains('.') {
        let body = &raw[..raw.len() - 1];
        if let Some((ys, ms)) = body.split_once('.').filter(|(ys, ms)| ys.len() == 4 && !ms.contains('.')) {
            let y: i32 = ys.parse().map_err(|_| KError::syntax(format!("bad month '{raw}'")))?;
            let m: i32 = ms.parse().map_err(|_| KError::syntax(format!("bad month '{raw}'")))?;
            if (1..=12).contains(&m) {
                return Ok((K::Month((y - 2000) * 12 + (m - 1)), None));
            }
        }
    }

    // Date (`YYYY.MM.DD`)
    if let Some((ys, rest)) = raw.split_once('.') {
        if let Some((ms, ds)) = rest.split_once('.').filter(|(_, ds)| !ds.contains('.')) {
            let y: i32 = ys.parse().map_err(|_| KError::syntax(format!("bad date '{raw}'")))?;
            let m: u32 = ms.parse().map_err(|_| KError::syntax(format!("bad date '{raw}'")))?;
            let d: u32 = ds.parse().map_err(|_| KError::syntax(format!("bad date '{raw}'")))?;
            let days = date_from_ymd(y, m, d).ok_or_else(|| KError::syntax(format!("invalid calendar date '{raw}'")))?;
            return Ok((K::Date(days), None));
        }
    }

    // Check trailing numeric type suffix (`h`, `i`, `j`, `e`, `f`)
    let last = raw.as_bytes()[raw.len() - 1] as char;
    if matches!(last, 'h' | 'i' | 'j' | 'e' | 'f') {
        let num_part = &raw[..raw.len() - 1];
        let atom = if matches!(last, 'e' | 'f') || num_part.contains('.') {
            let v: f64 = num_part.parse().map_err(|_| KError::syntax(format!("bad number '{raw}'")))?;
            K::Float(v)
        } else {
            let v: i64 = num_part.parse().map_err(|_| KError::syntax(format!("bad integer '{raw}'")))?;
            K::Long(v)
        };
        return Ok((atom, Some(last)));
    }

    // Plain float vs plain long
    if raw.contains('.') || raw.contains('e') || raw.contains('E') {
        let v: f64 = raw.parse().map_err(|_| KError::syntax(format!("bad float '{raw}'")))?;
        Ok((K::Float(v), None))
    } else {
        let v: i64 = raw.parse().map_err(|_| KError::syntax(format!("bad integer '{raw}'")))?;
        Ok((K::Long(v), None))
    }
}

/// `0N`, `0W` or `-0W` followed by the letter of a temporal type (`0Nz`, `0Wp`, `-0Wu`).
fn temporal_null_or_infinity(raw: &str) -> Option<K> {
    let (body, letter) = raw.split_at_checked(raw.len() - 1)?;
    let (int, long, float) = match body {
        "0N" => (NULL_INT, NULL_LONG, f64::NAN),
        "0W" => (INF_INT, INF_LONG, f64::INFINITY),
        "-0W" => (NINF_INT, NINF_LONG, f64::NEG_INFINITY),
        _ => return None,
    };
    Some(match letter {
        "p" => K::Timestamp(long),
        "m" => K::Month(int),
        "d" => K::Date(int),
        "z" => K::Datetime(float),
        "n" => K::Timespan(long),
        "u" => K::Minute(int),
        "v" => K::Second(int),
        "t" => K::Time(int),
        _ => return None,
    })
}

fn parse_date_str(s: &str) -> KResult<i32> {
    let Some((ys, rest)) = s.split_once('.') else { return Err(KError::syntax(format!("bad date '{s}'"))) };
    let Some((ms, ds)) = rest.split_once('.').filter(|(_, ds)| !ds.contains('.')) else {
        return Err(KError::syntax(format!("bad date '{s}'")));
    };
    let y: i32 = ys.parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    let m: u32 = ms.parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    let d: u32 = ds.parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    date_from_ymd(y, m, d).ok_or_else(|| KError::syntax(format!("invalid calendar date '{s}'")))
}

fn parse_hms_nanos(s: &str) -> KResult<i64> {
    // (an hour may stand alone: `2026.01.01D12`, `0D0`)
    let hour_only = || (s.len() <= 2 && s.bytes().all(|b| b.is_ascii_digit())).then_some((s, "0"));
    let Some((hh_s, rest)) = s.split_once(':').or_else(hour_only) else { return Err(KError::syntax(format!("bad time '{s}'"))) };
    let hh: i64 = hh_s.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
    let (mm_s, ss_opt) = match rest.split_once(':') {
        None => (rest, None),
        Some((m, p2)) if !p2.contains(':') => (m, Some(p2)),
        _ => return Err(KError::syntax(format!("bad time '{s}'"))),
    };
    let mm: i64 = mm_s.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
    let (ss, nanos) = match ss_opt {
        Some(p2) => {
            if let Some((ss_s, frac_s)) = p2.split_once('.') {
                let ss: i64 = ss_s.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
                let mut ns = 0i64;
                for idx in 0..9 {
                    let b = frac_s.as_bytes().get(idx).copied().unwrap_or(b'0');
                    if !b.is_ascii_digit() {
                        return Err(KError::syntax(format!("bad time '{s}'")));
                    }
                    ns = ns * 10 + i64::from(b - b'0');
                }
                if frac_s.bytes().skip(9).any(|b| !b.is_ascii_digit()) {
                    return Err(KError::syntax(format!("bad time '{s}'")));
                }
                (ss, ns)
            } else {
                let ss: i64 = p2.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
                (ss, 0)
            }
        }
        None => (0, 0),
    };
    Ok((hh * 3600 + mm * 60 + ss) * NANOS_PER_SEC + nanos)
}

fn apply_type_suffix_atom(atom: K, suf: Option<char>) -> KResult<K> {
    let Some(ch) = suf else { return Ok(atom) };
    match ch {
        'h' => match atom {
            K::Short(_) => Ok(atom),
            K::Long(NULL_LONG) => Ok(K::Short(NULL_SHORT)),
            K::Long(INF_LONG) => Ok(K::Short(INF_SHORT)),
            K::Long(NINF_LONG) => Ok(K::Short(NINF_SHORT)),
            K::Long(n) => Ok(K::Short(n as i16)),
            _ => Err(KError::syntax("bad short literal")),
        },
        'i' => match atom {
            K::Int(_) => Ok(atom),
            K::Long(NULL_LONG) => Ok(K::Int(NULL_INT)),
            K::Long(INF_LONG) => Ok(K::Int(INF_INT)),
            K::Long(NINF_LONG) => Ok(K::Int(NINF_INT)),
            K::Long(n) => Ok(K::Int(n as i32)),
            _ => Err(KError::syntax("bad int literal")),
        },
        'j' => match atom {
            K::Long(_) => Ok(atom),
            K::Int(n) => Ok(K::Long(if n == NULL_INT { NULL_LONG } else { n as i64 })),
            _ => Err(KError::syntax("bad long literal")),
        },
        'e' => match atom {
            K::Real(_) => Ok(atom),
            K::Float(f) => Ok(K::Real(f as f32)),
            K::Long(NULL_LONG) => Ok(K::Real(f32::NAN)),
            K::Long(n) => Ok(K::Real(n as f32)),
            _ => Err(KError::syntax("bad real literal")),
        },
        'f' => match atom {
            K::Float(_) => Ok(atom),
            K::Long(NULL_LONG) => Ok(K::Float(f64::NAN)),
            K::Long(INF_LONG) => Ok(K::Float(f64::INFINITY)),
            K::Long(NINF_LONG) => Ok(K::Float(f64::NEG_INFINITY)),
            K::Long(n) => Ok(K::Float(n as f64)),
            _ => Err(KError::syntax("bad float literal")),
        },
        _ => Ok(atom),
    }
}

fn coalesce_numeric_vector(atoms: Vec<K>, suf: Option<char>) -> KResult<K> {
    if let Some(ch) = suf {
        let converted: KResult<Vec<K>> = atoms.into_iter().map(|a| apply_type_suffix_atom(a, Some(ch))).collect();
        return Ok(K::Vector(ColVec::from_atoms(converted?)));
    }
    let any_float = atoms.iter().any(|a| matches!(a, K::Float(_)));
    if any_float && atoms.iter().all(|a| matches!(a, K::Float(_) | K::Long(_))) {
        let out: Vec<f64> = atoms
            .into_iter()
            .map(|a| match a {
                K::Float(f) => f,
                K::Long(n) => i64_to_f64_lit(n),
                _ => unreachable!(),
            })
            .collect();
        return Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))));
    }
    let dom = atoms.iter().find(|a| !matches!(a, K::Long(NULL_LONG))).map(|a| a.type_num());
    if let Some(dt) = dom {
        if matches!(dt, -12 | -13 | -14 | -15 | -16 | -17 | -18 | -19 | -6 | -5) {
            let coerced: Vec<K> = atoms
                .into_iter()
                .map(|a| match (dt, a) {
                    (-12, K::Long(NULL_LONG)) => K::Timestamp(NULL_LONG),
                    (-15, K::Long(NULL_LONG)) => K::Datetime(f64::NAN),
                    (-13, K::Long(NULL_LONG)) => K::Month(NULL_INT),
                    (-14, K::Long(NULL_LONG)) => K::Date(NULL_INT),
                    (-16, K::Long(NULL_LONG)) => K::Timespan(NULL_LONG),
                    (-17, K::Long(NULL_LONG)) => K::Minute(NULL_INT),
                    (-18, K::Long(NULL_LONG)) => K::Second(NULL_INT),
                    (-19, K::Long(NULL_LONG)) => K::Time(NULL_INT),
                    (-6, K::Long(NULL_LONG)) => K::Int(NULL_INT),
                    (-5, K::Long(NULL_LONG)) => K::Short(NULL_SHORT),
                    (_, other) => other,
                })
                .collect();
            return Ok(K::Vector(ColVec::from_atoms(coerced)));
        }
    }
    Ok(K::Vector(ColVec::from_atoms(atoms)))
}
