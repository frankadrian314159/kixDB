//! Tokenizer for `q` and `qSQL`.
//!
//! Handles `q`'s whitespace-sensitive rules:
//! - Adjacent scalar literals (`1 2 3`, `1.5 2.5`, `0101b`, `0x01ff`, `` `a`b`c ``, `2026.11.24 2026.11.25`) coalesce into
//!   single typed vector tokens.
//! - `/` at column 0 or after whitespace begins a line comment; immediately after an expression (`+/`, `sum/`) it is the
//!   `Over` adverb.
//! - `\` at column 0 is a system command (`\t`, `\l`, `\\`, or bare `\` abort); elsewhere it is the `Scan` adverb.
//! - `-` immediately before digits is part of a negative literal only when not immediately preceded by a value token
//!   (so `10-3` is `10 - 3`, `1 -2` is the vector `1 -2`, and `1 - 2` is `1 - 2`).
use crate::error::{KError, KResult};
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum Tok {
    Lit(K),
    Ident(String),
    Op(String),
    /// The `.` of `value.field`, written without spaces.
    Field,
    Adverb(AdverbKind),
    Colon,       // :
    DoubleColon, // ::
    OpColon(String), // +: ,: -: *: %: &: |:
    Semi,        // ;
    Comma,       // , (also binary concat operator outside qSQL clauses)
    LParen,
    RParen,
    LBrack,
    RBrack,
    LBrace { source: String, body_tokens: Vec<Tok> },
    CondOpen, // $[
    Quote,    // ' (signal when prefix, Each adverb when postfix)
    SysCmd { cmd: String, arg: String },
    Newline,
    Eof,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrevKind {
    Start,
    Value, // number, string, sym, ident, ), ], }
    Other, // (, [, ;, :, op, whitespace-separated
}

pub fn tokenize(src: &str) -> KResult<Vec<Tok>> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;
    let mut at_line_start = true;
    let mut after_ws = true;
    let mut prev = PrevKind::Start;
    let mut out = Vec::new();

    while i < len {
        let b = bytes[i];

        // Newlines
        if b == b'\r' || b == b'\n' {
            if b == b'\r' && i + 1 < len && bytes[i + 1] == b'\n' {
                i += 2;
            } else {
                i += 1;
            }
            if !matches!(out.last(), Some(Tok::Newline) | None) {
                out.push(Tok::Newline);
            }
            at_line_start = true;
            after_ws = true;
            prev = PrevKind::Start;
            continue;
        }

        // Spaces / tabs
        if b == b' ' || b == b'\t' {
            while i < len && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            after_ws = true;
            continue;
        }

        // Line-start backslash system command (`\t ...`, `\l ...`, `\\`, bare `\`)
        if at_line_start && b == b'\\' {
            i += 1;
            let start = i;
            while i < len && bytes[i] != b'\r' && bytes[i] != b'\n' {
                i += 1;
            }
            let line = src[start..i].trim();
            if line.is_empty() {
                // Bare `\` on its own line stops loading the rest of a script in kdb+.
                break;
            }
            if line == "\\" {
                out.push(Tok::SysCmd { cmd: "\\".into(), arg: String::new() });
            } else {
                let (cmd, arg) = match line.find(|c: char| c.is_whitespace()) {
                    Some(pos) => (line[..pos].to_string(), line[pos..].trim().to_string()),
                    None => (line.to_string(), String::new()),
                };
                out.push(Tok::SysCmd { cmd, arg });
            }
            at_line_start = false;
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }

        if b == b'/' && at_line_start && src[i + 1..].split('\n').next().is_some_and(|rest| rest.trim().is_empty()) {
            let mut lines = src[i..].split_inclusive('\n');
            i += lines.next().map_or(0, str::len);
            for line in lines {
                i += line.len();
                if line.trim_end() == "\\" {
                    break;
                }
            }
            continue;
        }

        // Comments: `/` at line start or preceded by whitespace
        if b == b'/' && (at_line_start || after_ws) {
            // Check if this is `/:` — even after space `x /: y` is each-right!
            if i + 1 < len && bytes[i + 1] == b':' && !at_line_start {
                out.push(Tok::Adverb(AdverbKind::EachRight));
                i += 2;
                at_line_start = false;
                after_ws = false;
                prev = PrevKind::Value;
                continue;
            }
            while i < len && bytes[i] != b'\r' && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        at_line_start = false;

        // Lambda `{ ... }` — capture exact source text and tokenize interior recursively
        if b == b'{' {
            let brace_start = i;
            let brace_end = find_matching_brace(src, i)?;
            let full_src = src[brace_start..=brace_end].to_string();
            let inner_src = &src[brace_start + 1..brace_end];
            let inner_toks = tokenize(inner_src)?;
            out.push(Tok::LBrace { source: full_src, body_tokens: inner_toks });
            i = brace_end + 1;
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }

        // String / char literal `"..."`
        if b == b'"' {
            let (k, next_i) = lex_string(src, i)?;
            out.push(Tok::Lit(k));
            i = next_i;
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }

        // Symbol literal(s) `` `a`b`c ``
        if b == b'`' {
            let (k, next_i) = lex_symbols(src, i);
            out.push(Tok::Lit(k));
            i = next_i;
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }

        // Numeric / temporal / boolean / byte literal (or space-separated vector of them!)
        let number_at = |j: usize| j < len && (bytes[j].is_ascii_digit() || (bytes[j] == b'.' && j + 1 < len && bytes[j + 1].is_ascii_digit()));
        let neg_num_ok = b == b'-'
            && number_at(i + 1)
            && (after_ws || prev != PrevKind::Value);
        let point_ok = b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_digit() && (after_ws || prev != PrevKind::Value);
        if b.is_ascii_digit() || neg_num_ok || point_ok {
            let (k, next_i) = lex_numeric_sequence(src, i)?;
            out.push(match k {
                K::Func(Func::Prim(s)) => Tok::Op(resolve(s).into()),
                k => Tok::Lit(k),
            });
            i = next_i;
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }

        // `$[` conditional vs `$` cast operator
        if b == b'$' {
            if i + 1 < len && bytes[i + 1] == b'[' && (after_ws || prev != PrevKind::Value) {
                out.push(Tok::CondOpen);
                i += 2;
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            if i + 1 < len && bytes[i + 1] == b':' {
                out.push(Tok::OpColon("$".into()));
                i += 2;
            } else {
                out.push(Tok::Op("$".into()));
                i += 1;
            }
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }

        // Adverbs: `/:`, `\:`, `':`, `/`, `\`, `'`
        if b == b'/' {
            if i + 1 < len && bytes[i + 1] == b':' {
                out.push(Tok::Adverb(AdverbKind::EachRight));
                i += 2;
            } else {
                out.push(Tok::Adverb(AdverbKind::Over));
                i += 1;
            }
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }
        if b == b'\\' {
            if i + 1 < len && bytes[i + 1] == b':' {
                out.push(Tok::Adverb(AdverbKind::EachLeft));
                i += 2;
            } else {
                out.push(Tok::Adverb(AdverbKind::Scan));
                i += 1;
            }
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }
        if b == b'\'' {
            if i + 1 < len && bytes[i + 1] == b':' {
                out.push(Tok::Adverb(AdverbKind::EachPrior));
                i += 2;
                after_ws = false;
                prev = PrevKind::Value;
            } else if !after_ws && (prev == PrevKind::Value || matches!(out.last(), Some(Tok::Op(_) | Tok::Comma))) {
                out.push(Tok::Adverb(AdverbKind::Each));
                i += 1;
                after_ws = false;
                prev = PrevKind::Value;
            } else {
                out.push(Tok::Quote);
                i += 1;
                after_ws = false;
                prev = PrevKind::Other;
            }
            continue;
        }

        // Delimiters
        match b {
            b'(' => {
                out.push(Tok::LParen);
                i += 1;
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            b')' => {
                out.push(Tok::RParen);
                i += 1;
                after_ws = false;
                prev = PrevKind::Value;
                continue;
            }
            b'[' => {
                out.push(Tok::LBrack);
                i += 1;
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            b']' => {
                out.push(Tok::RBrack);
                i += 1;
                after_ws = false;
                prev = PrevKind::Value;
                continue;
            }
            b';' => {
                out.push(Tok::Semi);
                i += 1;
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            b',' => {
                if i + 1 < len && bytes[i + 1] == b':' {
                    out.push(Tok::OpColon(",".into()));
                    i += 2;
                } else {
                    out.push(Tok::Comma);
                    i += 1;
                }
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            b':' => {
                if i + 1 < len && bytes[i + 1] == b':' {
                    out.push(Tok::DoubleColon);
                    i += 2;
                } else {
                    out.push(Tok::Colon);
                    i += 1;
                }
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            _ => {}
        }

        // Identifiers & `.namespace.name` (note: `.` followed by letter starts a namespace ident when not directly on a value)
        // (after `]`, as in `del[x].z.w`, a name of one of q's own namespaces is what is applied to, not a field)
        let own_name = || i > 0 && bytes[i - 1] == b']' && matches!(bytes.get(i + 1..i + 3), Some([b'z' | b'Q' | b'h' | b'j', b'.']));
        if b.is_ascii_alphabetic()
            || (b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() && (after_ws || prev != PrevKind::Value || own_name()))
        {
            let start = i;
            if b == b'.' {
                i += 1;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.') {
                    i += 1;
                }
            } else {
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
            }
            out.push(Tok::Ident(src[start..i].to_string()));
            after_ws = false;
            prev = PrevKind::Value;
            continue;
        }

        // Dot field access `.field` immediately after a value (`time.minute`, `t.price`) vs `.` apply operator
        if b == b'.' {
            if !after_ws && prev == PrevKind::Value && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
                out.push(Tok::Field);
                i += 1;
                after_ws = false;
                prev = PrevKind::Other;
                continue;
            }
            if i + 1 < len && bytes[i + 1] == b':' {
                out.push(Tok::OpColon(".".into()));
                i += 2;
            } else {
                out.push(Tok::Op(".".into()));
                i += 1;
            }
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }

        // Multi-char comparison operators (`<=`, `>=`, `<>`) and compound assignment (`+:`, `-:`, `*:`, `%:`, `&:`, `|:`, ...)
        if b == b'<' && i + 1 < len && (bytes[i + 1] == b'=' || bytes[i + 1] == b'>') {
            out.push(Tok::Op(src[i..i + 2].to_string()));
            i += 2;
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }
        if b == b'>' && i + 1 < len && bytes[i + 1] == b'=' {
            out.push(Tok::Op(">=".into()));
            i += 2;
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }

        if matches!(b, b'+' | b'-' | b'*' | b'%' | b'&' | b'|' | b'^' | b'=' | b'<' | b'>' | b'~' | b'!' | b'#' | b'_' | b'@' | b'?') {
            let op_ch = (b as char).to_string();
            // Check compound assignment `+:`, `&:`, etc. (note: `0:` / `1:` / `2:` are lexed in numeric sequence if preceded by digit)
            if i + 1 < len && bytes[i + 1] == b':' && matches!(b, b'+' | b'-' | b'*' | b'%' | b'&' | b'|' | b'^' | b'#' | b'_' | b'!' | b'@') {
                out.push(Tok::OpColon(op_ch));
                i += 2;
            } else {
                out.push(Tok::Op(op_ch));
                i += 1;
            }
            after_ws = false;
            prev = PrevKind::Other;
            continue;
        }

        return Err(KError::syntax(format!("unexpected character '{}'", b as char)));
    }

    out.push(Tok::Eof);
    Ok(out)
}

fn find_matching_brace(src: &str, start: usize) -> KResult<usize> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut depth = 0usize;
    let mut i = start;
    let mut at_line_start = false;
    let mut after_ws = false;

    while i < len {
        let b = bytes[i];
        if b == b'\r' || b == b'\n' {
            at_line_start = true;
            after_ws = true;
            i += 1;
            continue;
        }
        if b == b'/' && (at_line_start || after_ws) && !(i + 1 < len && bytes[i + 1] == b':') {
            while i < len && bytes[i] != b'\r' && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'"' {
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            at_line_start = false;
            after_ws = false;
            continue;
        }
        after_ws = b == b' ' || b == b'\t';
        if !after_ws {
            at_line_start = false;
        }
        if b == b'{' {
            depth += 1;
        } else if b == b'}' {
            depth -= 1;
            if depth == 0 {
                return Ok(i);
            }
        }
        i += 1;
    }
    Err(KError::syntax("unclosed '{' in lambda"))
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
    // In kdb+: `"a"` (length 1) is a Char atom (`-10h`), while `""` and `"ab..."` are Char vectors (`10h`).
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
    let mut syms: Vec<SymId> = Vec::new();

    while i < len && bytes[i] == b'`' {
        i += 1;
        let s_start = i;
        // File/handle symbols like `:path/to/dir/ or `:localhost:5000 can contain `:`, `/`, `.`, `-`, `_`
        let is_handle = i < len && bytes[i] == b':';
        if is_handle {
            i += 1;
            while i < len && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b';' | b')' | b']' | b'}' | b',' | b'`' | b'"') {
                i += 1;
            }
        } else {
            while i < len && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'.')) {
                i += 1;
            }
            // a web address such as `http://host/page
            if bytes[i..].starts_with(b"://") {
                while i < len && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b';' | b')' | b']' | b'}' | b',' | b'`' | b'"') {
                    i += 1;
                }
            }
        }
        syms.push(intern(&src[s_start..i]));
    }

    if syms.len() == 1 {
        (K::Sym(syms[0]), i)
    } else {
        (K::Vector(ColVec::Sym(Attr::None, Arc::new(syms))), i)
    }
}

/// Lex one scalar numeric/boolean/byte/temporal token and any space-separated peers that form a vector literal (`1 2 3`, `1.5 2.5`).
fn lex_numeric_sequence(src: &str, start: usize) -> KResult<(K, usize)> {
    let bytes = src.as_bytes();
    let len = bytes.len();

    // Special kdb+ file/IPC verbs `0:`, `1:`, `2:`
    if start + 1 < len && matches!(bytes[start], b'0' | b'1' | b'2') && bytes[start + 1] == b':' {
        // Check not `00:00` time literal!
        return Ok((K::Func(Func::Prim(intern(&src[start..start + 2]))), start + 2));
    }

    // Hex byte literal `0x0102ff`
    if start + 2 <= len && &src[start..start + 2] == "0x" || (start + 2 <= len && &src[start..start + 2] == "0X") {
        let mut i = start + 2;
        while i < len && bytes[i].is_ascii_hexdigit() {
            i += 1;
        }
        let hex = &src[start + 2..i];
        if hex.is_empty() {
            return Ok((K::Vector(ColVec::Byte(Attr::None, Arc::new(vec![]))), i));
        }
        let mut out = Vec::with_capacity((hex.len() + 1) / 2);
        let padded = if hex.len() % 2 == 1 { format!("0{hex}") } else { hex.to_string() };
        for c in (0..padded.len()).step_by(2) {
            out.push(u8::from_str_radix(&padded[c..c + 2], 16).map_err(|_| KError::syntax("bad hex byte"))?);
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
            let bits: Vec<bool> = bytes[start..j].iter().map(|&c| c == b'1').collect();
            let k = if bits.len() == 1 {
                K::Bool(bits[0])
            } else {
                K::Vector(ColVec::Bool(Attr::None, Arc::new(bits)))
            };
            return Ok((k, j + 1));
        }
    }

    let (first_atom, mut pos, mut vec_suffix) = lex_one_number_or_temporal(src, start)?;
    // the type a letter on the last time of a list gives to all of them
    let mut time_kind = time_letter(&src[start..pos]);
    if vec_suffix.is_some() {
        // e.g. `42i`, `3.5e`, `10h` — single atom with explicit suffix
        return Ok((apply_type_suffix_atom(first_atom, vec_suffix)?, pos));
    }

    let mut atoms = vec![first_atom];

    // Consume space-separated adjacent numeric/temporal atoms (`1 2 3`, `1 0N 3i`, `2026.11.24 2026.11.25`)
    loop {
        let mut ws = pos;
        while ws < len && (bytes[ws] == b' ' || bytes[ws] == b'\t') {
            ws += 1;
        }
        if ws == pos || ws >= len {
            break;
        }
        // Next item is part of the vector if it starts with a digit, OR `-` immediately followed by a digit!
        let next_b = bytes[ws];
        let digit_after = ws + 1 < len && bytes[ws + 1].is_ascii_digit();
        let negative_point = next_b == b'-' && ws + 2 < len && bytes[ws + 1] == b'.' && bytes[ws + 2].is_ascii_digit();
        let starts_num = next_b.is_ascii_digit() || (matches!(next_b, b'-' | b'.') && digit_after) || negative_point;
        if !starts_num {
            break;
        }
        // Make sure it is not `0:` / `1:` / `2:` or `0x...` or `01b` of a different incompatible family unless compatible
        if ws + 1 < len && matches!(next_b, b'0' | b'1' | b'2') && bytes[ws + 1] == b':' {
            break;
        }
        if ws + 2 <= len && (&src[ws..ws + 2] == "0x" || &src[ws..ws + 2] == "0X") {
            break;
        }
        // Check bitstring `01b` — do not merge with integer vector unless whole thing was bitstring
        let mut bj = ws;
        while bj < len && (bytes[bj] == b'0' || bytes[bj] == b'1') {
            bj += 1;
        }
        if bj > ws && bj < len && bytes[bj] == b'b' && (bj + 1 == len || !bytes[bj + 1].is_ascii_alphanumeric()) {
            break;
        }

        let (atom, next_pos, suf) = lex_one_number_or_temporal(src, ws)?;
        atoms.push(atom);
        time_kind = time_letter(&src[ws..next_pos]);
        pos = next_pos;
        if suf.is_some() {
            vec_suffix = suf;
            break;
        }
    }

    // times of different types become the type written last, or the finest of them
    let kinds: Vec<i16> = atoms.iter().map(|a| a.type_num()).collect();
    if let Some(kind) = time_kind.or_else(|| crate::temporal::finest(&kinds)) {
        atoms.iter_mut().filter(|a| a.type_num() != -kind).for_each(|a| *a = crate::temporal::convert(a, kind));
    }

    if atoms.len() == 1 {
        Ok((atoms.into_iter().next().unwrap(), pos))
    } else {
        Ok((coalesce_numeric_vector(atoms, vec_suffix)?, pos))
    }
}

/// The type a time ends by naming (`12:34u` is a minute).
fn time_letter(raw: &str) -> Option<i16> {
    let kind = "uvtn".find(raw.chars().last()?)?;
    raw.contains(':').then_some([17, 18, 19, 16][kind])
}

/// Lexes a single numeric or temporal scalar token starting at `start`.
/// Returns `(atom, next_offset, optional_trailing_type_suffix)`.
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
            // Stop before `.` if `.` is followed by an identifier that isn't a temporal/float part
            // (e.g. `2026.11.24` has digits after `.`; `time.minute` starts with ident, not digit; what about `5.year`? stop if alpha after `.`!)
            if b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
                break;
            }
            // Stop before `:` unless part of time/timestamp (`12:30`, `2026.11.24D12:30:00`)!
            if b == b':' {
                let next_is_digit = i + 1 < len && bytes[i + 1].is_ascii_digit();
                if !next_is_digit {
                    break;
                }
            }
            // Scientific notation `1e-5` / `1.5e+3`
            if (b == b'e' || b == b'E') && i + 1 < len && (bytes[i + 1] == b'+' || bytes[i + 1] == b'-') && i + 2 < len && bytes[i + 2].is_ascii_digit() {
                i += 2;
                continue;
            }
            i += 1;
        } else if b == b'-' {
            // GUID literal `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (only if 8 hex chars before first `-`)
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

fn parse_scalar_token(raw: &str) -> KResult<(K, Option<char>)> {
    // Nulls and infinities: 0N, 0n, 0W, -0W, 0w, -0w, plus type-suffixed 0Ni, 0Nh, 0Nj, 0Nf, 0Ne, 0Nd, 0Nm, 0Np, 0Nn, 0Nu, 0Nv, 0Nt, 0Ng
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
        "0Nd" => return Ok((K::Date(NULL_INT), None)),
        "0Wd" => return Ok((K::Date(INF_INT), None)),
        "-0Wd" => return Ok((K::Date(NINF_INT), None)),
        "0Nm" => return Ok((K::Month(NULL_INT), None)),
        "0Np" => return Ok((K::Timestamp(NULL_LONG), None)),
        "0Nn" => return Ok((K::Timespan(NULL_LONG), None)),
        "0Nu" => return Ok((K::Minute(NULL_INT), None)),
        "0Nv" => return Ok((K::Second(NULL_INT), None)),
        "0Nt" => return Ok((K::Time(NULL_INT), None)),
        "0Ng" => return Ok((K::Guid(NULL_GUID), None)),
        _ => {}
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
    if let Some(d_pos) = raw.find('D') {
        let left = &raw[..d_pos];
        let right = &raw[d_pos + 1..];
        if left.contains('.') {
            // Timestamp: left is YYYY.MM.DD
            let days = parse_date_str(left)?;
            let ns_in_day = if right.is_empty() { 0 } else { parse_hms_nanos(right)? };
            let total_ns = (days as i64)
                .checked_mul(NANOS_PER_DAY)
                .and_then(|d| d.checked_add(ns_in_day))
                .ok_or_else(|| KError::syntax("timestamp overflow"))?;
            return Ok((K::Timestamp(total_ns), None));
        } else {
            // Timespan: left is integer days (e.g. `0D12:00:00` or `-1D02:00:00`)
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
        let parts: Vec<&str> = s.split(':').collect();
        match parts.len() {
            2 => {
                let hh: i32 = parts[0].parse().map_err(|_| KError::syntax(format!("bad minute '{raw}'")))?;
                let mm: i32 = parts[1].parse().map_err(|_| KError::syntax(format!("bad minute '{raw}'")))?;
                let mins = hh * 60 + mm;
                return Ok((K::Minute(if neg { -mins } else { mins }), None));
            }
            3 => {
                let hh: i64 = parts[0].parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                let mm: i64 = parts[1].parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                if let Some((ss_s, frac_s)) = parts[2].split_once('.') {
                    let ss: i64 = ss_s.parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                    if frac_s.len() <= 3 {
                        let mut p = frac_s.to_string();
                        while p.len() < 3 {
                            p.push('0');
                        }
                        let ms_frac: i64 = p.parse().map_err(|_| KError::syntax(format!("bad time '{raw}'")))?;
                        let total_ms = ((hh * 3600 + mm * 60 + ss) * 1000 + ms_frac) as i32;
                        return Ok((K::Time(if neg { -total_ms } else { total_ms }), None));
                    } else {
                        let ns = parse_hms_nanos(s)?;
                        return Ok((K::Timespan(if neg { -ns } else { ns }), None));
                    }
                } else {
                    let ss: i32 = parts[2].parse().map_err(|_| KError::syntax(format!("bad second '{raw}'")))?;
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
        let parts: Vec<&str> = body.split('.').collect();
        if parts.len() == 2 && parts[0].len() == 4 {
            let y: i32 = parts[0].parse().map_err(|_| KError::syntax(format!("bad month '{raw}'")))?;
            let m: i32 = parts[1].parse().map_err(|_| KError::syntax(format!("bad month '{raw}'")))?;
            if (1..=12).contains(&m) {
                return Ok((K::Month((y - 2000) * 12 + (m - 1)), None));
            }
        }
    }

    // Date (`YYYY.MM.DD`)
    if raw.matches('.').count() == 2 {
        let d = parse_date_str(raw)?;
        return Ok((K::Date(d), None));
    }

    // Check trailing numeric type suffix (`h`, `i`, `j`, `e`, `f`)
    let last = raw.as_bytes()[raw.len() - 1] as char;
    if matches!(last, 'h' | 'i' | 'j' | 'e' | 'f') {
        let num_part = &raw[..raw.len() - 1];
        // Make sure `e` isn't incomplete scientific notation (already handled above)
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

fn parse_date_str(s: &str) -> KResult<i32> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return Err(KError::syntax(format!("bad date '{s}'")));
    }
    let y: i32 = parts[0].parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    let m: u32 = parts[1].parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    let d: u32 = parts[2].parse().map_err(|_| KError::syntax(format!("bad date '{s}'")))?;
    date_from_ymd(y, m, d).ok_or_else(|| KError::syntax(format!("invalid calendar date '{s}'")))
}

fn parse_hms_nanos(s: &str) -> KResult<i64> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(KError::syntax(format!("bad time '{s}'")));
    }
    let hh: i64 = parts[0].parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
    let mm: i64 = parts[1].parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
    let (ss, nanos) = if parts.len() == 3 {
        if let Some((ss_s, frac_s)) = parts[2].split_once('.') {
            let ss: i64 = ss_s.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
            let mut p: String = frac_s.chars().take(9).collect();
            while p.len() < 9 {
                p.push('0');
            }
            let ns: i64 = p.parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
            (ss, ns)
        } else {
            let ss: i64 = parts[2].parse().map_err(|_| KError::syntax(format!("bad time '{s}'")))?;
            (ss, 0)
        }
    } else {
        (0, 0)
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
    // Check if any element is Float while others are Long (`1 2.5 3` -> Float vector; `1 0N 2.5` -> `0N` becomes `NaN`)
    let any_float = atoms.iter().any(|a| matches!(a, K::Float(_)));
    if any_float && atoms.iter().all(|a| matches!(a, K::Float(_) | K::Long(_))) {
        let mut out = Vec::with_capacity(atoms.len());
        for a in atoms {
            match a {
                K::Float(f) => out.push(f),
                K::Long(NULL_LONG) => out.push(f64::NAN),
                K::Long(INF_LONG) => out.push(f64::INFINITY),
                K::Long(NINF_LONG) => out.push(f64::NEG_INFINITY),
                K::Long(n) => out.push(n as f64),
                _ => unreachable!(),
            }
        }
        return Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))));
    }
    // Handle `0N` inside temporal vectors (e.g. `2026.11.24 0Nd` or `2026.11.24 0N`)
    let dom = atoms.iter().find(|a| !matches!(a, K::Long(NULL_LONG))).map(|a| a.type_num());
    if let Some(dt) = dom {
        if matches!(dt, -12 | -13 | -14 | -16 | -17 | -18 | -19 | -6 | -5) {
            let coerced: Vec<K> = atoms
                .into_iter()
                .map(|a| match (dt, a) {
                    (-12, K::Long(NULL_LONG)) => K::Timestamp(NULL_LONG),
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
