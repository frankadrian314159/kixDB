//! Shared formatting, CSV import/export, streaming bulk loader, and `q` script helpers for CLI and GUI.
use crate::engine::ResultSet;
use crate::error::{KError, KResult};
use crate::inspect::TableInfo;
use crate::sym::resolve;
use crate::types::{ColVec, Flip, KeyedTable, K};
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

// ----- Authentic kdb+ Console Formatting ------------------------------------

fn truncate_cell(s: String, max_width: usize) -> String {
    let clean = s.replace(['\r', '\n'], " ");
    if clean.chars().count() > max_width {
        format!("{}..", clean.chars().take(max_width.saturating_sub(2)).collect::<String>())
    } else {
        clean
    }
}

pub fn format_flip(f: &Flip, max_rows: usize) -> String {
    if f.cols.is_empty() {
        return "()".into();
    }
    let show_rows = f.nrows().min(max_rows);
    let headers: Vec<String> = f.cols.iter().map(|&c| resolve(c).into()).collect();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count().max(1)).collect();
    let mut grid: Vec<Vec<String>> = Vec::with_capacity(show_rows);

    for r in 0..show_rows {
        let mut row = Vec::with_capacity(f.ncols());
        for (ci, col) in f.vals.iter().enumerate() {
            let txt = truncate_cell(crate::display::column_cell(col, r), 50);
            widths[ci] = widths[ci].max(txt.chars().count());
            row.push(txt);
        }
        grid.push(row);
    }

    let pad = |cells: &[String]| -> String {
        cells
            .iter()
            .zip(&widths)
            .map(|(c, &w)| format!("{c}{}", " ".repeat(w - c.chars().count())))
            .collect::<Vec<_>>()
            .join(" ")
            .trim_end()
            .to_string()
    };

    let mut out = String::new();
    let hdr_line = pad(&headers);
    let rule_len = widths.iter().sum::<usize>() + widths.len().saturating_sub(1);
    out.push_str(&hdr_line);
    out.push('\n');
    out.push_str(&"-".repeat(rule_len.max(1)));
    for row in &grid {
        out.push('\n');
        out.push_str(&pad(row));
    }
    if f.nrows() > show_rows {
        out.push_str("\n..");
    }
    out
}

pub fn format_keyed_table(kt: &KeyedTable, max_rows: usize) -> String {
    let total = kt.nrows();
    let show_rows = total.min(max_rows);
    let k_headers: Vec<String> = kt.keys.cols.iter().map(|&c| resolve(c).into()).collect();
    let v_headers: Vec<String> = kt.vals.cols.iter().map(|&c| resolve(c).into()).collect();

    let mut kw: Vec<usize> = k_headers.iter().map(|h| h.chars().count().max(1)).collect();
    let mut vw: Vec<usize> = v_headers.iter().map(|h| h.chars().count().max(1)).collect();
    let mut k_grid = Vec::with_capacity(show_rows);
    let mut v_grid = Vec::with_capacity(show_rows);

    for r in 0..show_rows {
        let mut kr = Vec::with_capacity(kt.keys.ncols());
        for (ci, col) in kt.keys.vals.iter().enumerate() {
            let txt = truncate_cell(crate::display::column_cell(col, r), 40);
            kw[ci] = kw[ci].max(txt.chars().count());
            kr.push(txt);
        }
        k_grid.push(kr);

        let mut vr = Vec::with_capacity(kt.vals.ncols());
        for (ci, col) in kt.vals.vals.iter().enumerate() {
            let txt = truncate_cell(crate::display::column_cell(col, r), 40);
            vw[ci] = vw[ci].max(txt.chars().count());
            vr.push(txt);
        }
        v_grid.push(vr);
    }

    let pad_exact = |cells: &[String], widths: &[usize]| -> String {
        cells
            .iter()
            .zip(widths)
            .map(|(c, &w)| format!("{c}{}", " ".repeat(w - c.chars().count())))
            .collect::<Vec<_>>()
            .join(" ")
    };

    let kh = pad_exact(&k_headers, &kw);
    let vh = pad_exact(&v_headers, &vw).trim_end().to_string();
    let k_rule = "-".repeat(kw.iter().sum::<usize>() + kw.len().saturating_sub(1));
    let v_rule = "-".repeat((vw.iter().sum::<usize>() + vw.len().saturating_sub(1)).max(1));

    let mut out = format!("{kh}| {vh}\n{k_rule}| {v_rule}");
    for (kr, vr) in k_grid.iter().zip(&v_grid) {
        out.push('\n');
        out.push_str(&pad_exact(kr, &kw));
        out.push_str("| ");
        out.push_str(pad_exact(vr, &vw).trim_end());
    }
    if total > show_rows {
        out.push_str("\n..");
    }
    out
}

pub fn table_script(info: &TableInfo) -> String {
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    for c in &info.columns {
        let attr_pfx = c.attr.map(|a| format!("`{a}#")).unwrap_or_default();
        let empty_expr = if c.type_char == 'C' || c.type_char == ' ' {
            "()".to_string()
        } else {
            format!("{attr_pfx}`{}$()", c.type_name)
        };
        let def = format!("{}: {empty_expr}", c.name);
        if c.is_key {
            keys.push(def);
        } else {
            vals.push(def);
        }
    }
    format!("{}: ([{}] {})", info.name, keys.join("; "), vals.join("; "))
}

/// Convert user-typed text in Datasheet into a valid `q` literal expression for a column of `type_char`.
pub fn q_literal_for_column(text: &str, type_char: char) -> KResult<String> {
    let t = text.trim();
    if t.is_empty() {
        return Ok(match type_char {
            'b' => "0b".into(),
            'g' => "0Ng".into(),
            'x' => "0x00".into(),
            'h' => "0Nh".into(),
            'i' => "0Ni".into(),
            'j' => "0N".into(),
            'e' => "0Ne".into(),
            'f' => "0n".into(),
            'c' => "\" \"".into(),
            's' => "`".into(),
            'p' => "0Np".into(),
            'm' => "0Nm".into(),
            'd' => "0Nd".into(),
            'z' => "0Nz".into(),
            'n' => "0Nn".into(),
            'u' => "0Nu".into(),
            'v' => "0Nv".into(),
            't' => "0Nt".into(),
            _ => "\"\"".into(),
        });
    }
    let atom = crate::engine::ops::parse_atom_from_str(type_char.to_ascii_lowercase(), t);
    if type_char == 'C' {
        let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
        if escaped.len() == 1 {
            Ok(format!("enlist \"{escaped}\""))
        } else {
            Ok(format!("\"{escaped}\""))
        }
    } else {
        Ok(atom.q_repr())
    }
}

pub fn q_literal(v: &K) -> String {
    match v {
        K::Vector(ColVec::Char(_, bs)) if bs.len() == 1 => {
            format!("enlist \"{}\"", (bs[0] as char))
        }
        other => other.q_repr(),
    }
}

// ----- CSV Parsing, Export, Inference & Streaming Bulk Import ---------------

pub fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) || s.starts_with(' ') || s.ends_with(' ') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

pub fn to_csv(rs: &ResultSet) -> String {
    let mut out = String::new();
    out.push_str(&rs.columns.iter().map(|c| csv_field(c)).collect::<Vec<_>>().join(","));
    out.push_str("\r\n");
    for r in &rs.rows {
        let cells: Vec<String> = r.iter().map(|v| if v.is_null() { String::new() } else { csv_field(&v.data_text()) }).collect();
        out.push_str(&cells.join(","));
        out.push_str("\r\n");
    }
    out
}

pub fn parse_csv(text: &str) -> KResult<Vec<Vec<String>>> {
    parse_delimited(text, ',')
}

pub fn parse_delimited(text: &str, delim: char) -> KResult<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    scan_delimited_rows(text, delim, |fields| {
        rows.push(fields.iter().map(str::to_string).collect());
        Ok(())
    })?;
    Ok(rows)
}

pub(crate) struct RowFields<'a> {
    text: &'a str,
    unescaped: &'a str,
    ranges: &'a [(usize, usize, bool)],
}

impl<'a> RowFields<'a> {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ranges.len()
    }

    #[inline]
    pub(crate) fn get(&self, i: usize) -> Option<&'a str> {
        self.ranges.get(i).map(|&(s, e, u)| if u { &self.unescaped[s..e] } else { &self.text[s..e] })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &'a str> + '_ {
        (0..self.len()).map(|i| self.get(i).unwrap())
    }
}

/// Reusable zero-copy row scanner for delimited text: unquoted fields borrow straight from `text`,
/// while quoted fields unescape into a single reused scratch buffer.
pub(crate) struct DelimScanner {
    ranges: Vec<(usize, usize, bool)>,
    unescaped: String,
}

impl DelimScanner {
    pub(crate) fn new() -> Self {
        Self {
            ranges: Vec::with_capacity(16),
            unescaped: String::new(),
        }
    }

    pub(crate) fn scan<F>(&mut self, text: &str, delim: char, mut on_row: F) -> KResult<()>
    where
        F: FnMut(RowFields<'_>) -> KResult<()>,
    {
        let text = text.strip_prefix('\u{feff}').unwrap_or(text);
        let dlen = delim.len_utf8();
        self.ranges.clear();
        self.unescaped.clear();

        let mut i = 0usize;
        let len = text.len();
        let bytes = text.as_bytes();
        let mut field_start = 0usize;
        let mut quoted = false;
        let mut in_unescaped = false;
        let mut unesc_start = 0usize;

        let emit_row = |ranges: &mut Vec<(usize, usize, bool)>, unescaped: &mut String, on_row: &mut F| -> KResult<()> {
            if !(ranges.len() == 1 && !ranges[0].2 && ranges[0].0 == ranges[0].1) {
                on_row(RowFields { text, unescaped, ranges })?;
            }
            ranges.clear();
            unescaped.clear();
            Ok(())
        };

        while i < len {
            let c = if bytes[i].is_ascii() {
                bytes[i] as char
            } else {
                text[i..].chars().next().unwrap()
            };
            let clen = c.len_utf8();

            if quoted {
                if c == '"' {
                    if i + 1 < len && bytes[i + 1] == b'"' {
                        self.unescaped.push('"');
                        i += 2;
                    } else {
                        quoted = false;
                        i += 1;
                    }
                } else {
                    self.unescaped.push(c);
                    i += clen;
                }
                continue;
            }

            if c == '"' && !in_unescaped && i == field_start {
                quoted = true;
                in_unescaped = true;
                unesc_start = self.unescaped.len();
                i += 1;
            } else if c == delim {
                if in_unescaped {
                    self.ranges.push((unesc_start, self.unescaped.len(), true));
                } else {
                    self.ranges.push((field_start, i, false));
                }
                i += dlen;
                field_start = i;
                in_unescaped = false;
            } else if c == '\r' || c == '\n' {
                if in_unescaped {
                    self.ranges.push((unesc_start, self.unescaped.len(), true));
                } else {
                    self.ranges.push((field_start, i, false));
                }
                if c == '\r' && i + 1 < len && bytes[i + 1] == b'\n' {
                    i += 2;
                } else {
                    i += 1;
                }
                emit_row(&mut self.ranges, &mut self.unescaped, &mut on_row)?;
                field_start = i;
                in_unescaped = false;
            } else {
                if in_unescaped {
                    self.unescaped.push(c);
                }
                i += clen;
            }
        }

        if quoted {
            return Err(KError::syntax("unterminated quoted CSV field"));
        }
        if in_unescaped || field_start < len || !self.ranges.is_empty() {
            if in_unescaped {
                self.ranges.push((unesc_start, self.unescaped.len(), true));
            } else {
                self.ranges.push((field_start, len, false));
            }
            emit_row(&mut self.ranges, &mut self.unescaped, &mut on_row)?;
        }
        Ok(())
    }
}

pub(crate) fn scan_delimited_rows<F>(text: &str, delim: char, on_row: F) -> KResult<()>
where
    F: FnMut(RowFields<'_>) -> KResult<()>,
{
    DelimScanner::new().scan(text, delim, on_row)
}

/// Infer a kdb+ column type character (`j`, `f`, `b`, `d`, `p`, `t`, `s`, `C`) from sample strings.
pub fn infer_column_type(samples: &[&str]) -> char {
    let non_empty: Vec<&str> = samples.iter().copied().map(str::trim).filter(|s| !s.is_empty()).collect();
    if non_empty.is_empty() {
        return 's';
    }
    if non_empty.iter().all(|s| matches!(*s, "0" | "1" | "0b" | "1b" | "true" | "false" | "TRUE" | "FALSE")) {
        return 'b';
    }
    if non_empty.iter().all(|s| s.parse::<i64>().is_ok()) {
        return 'j';
    }
    if non_empty.iter().all(|s| s.parse::<f64>().is_ok()) {
        return 'f';
    }
    if non_empty.iter().all(|s| {
        let n = s.replace('-', ".");
        let p: Vec<&str> = n.split('.').collect();
        p.len() == 3 && p[0].len() == 4 && p.iter().all(|x| x.bytes().all(|b| b.is_ascii_digit()))
    }) {
        return 'd';
    }
    if non_empty.iter().all(|s| !s.contains(' ') && s.len() <= 32) {
        return 's';
    }
    'C'
}

pub fn sanitize_ident(raw: &str, fallback_idx: usize) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if out.is_empty() || out.as_bytes()[0].is_ascii_digit() {
        out = format!("c{fallback_idx}_{out}");
    }
    out
}

/// Whether `{...}`, `(...)`, `[...]` and strings are closed, so that a shell can run what has been typed so far.
pub fn delimiters_balanced(src: &str) -> bool {
    let mut depth = 0i32;
    let mut in_str = false;
    let mut bytes = src.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'\\' if in_str => {
                bytes.next();
            }
            b'"' => in_str = !in_str,
            b'(' | b'[' | b'{' if !in_str => depth += 1,
            b')' | b']' | b'}' if !in_str => depth -= 1,
            _ => {}
        }
    }
    !in_str && depth <= 0
}

pub trait QSession {
    fn run_q(&mut self, code: &str) -> KResult<()>;
    fn describe_table(&mut self, name: &str) -> Option<TableInfo>;
    fn upsert_batch(&mut self, table_name: &str, batch: Flip) -> KResult<()>;
}

fn create_empty_table<S: QSession + ?Sized>(sess: &mut S, table_name: &str, headers: &[String], tcs: &[char]) -> KResult<()> {
    let col_defs: Vec<String> = headers
        .iter()
        .zip(tcs)
        .map(|(h, &tc)| {
            let empty = ColVec::empty_of_type_char(tc)
                .map(|cv| format!("`{}$()", cv.type_name()))
                .unwrap_or_else(|| "()".into());
            format!("{h}: {empty}")
        })
        .collect();
    sess.run_q(&format!("{table_name}: ([] {})", col_defs.join("; ")))
}

fn flush_builders<S: QSession + ?Sized>(
    sess: &mut S,
    table_name: &str,
    col_syms: &[crate::sym::SymId],
    type_chars: &[char],
    cap: usize,
    builders: &mut Vec<crate::storage::ColBuilder>,
) -> KResult<()> {
    let finished = std::mem::replace(
        builders,
        type_chars.iter().map(|&tc| crate::storage::ColBuilder::with_capacity(tc, cap, true)).collect(),
    );
    let vals: Vec<ColVec> = finished.into_iter().filter_map(crate::storage::ColBuilder::finish).collect();
    sess.upsert_batch(table_name, Flip::new(col_syms.to_vec(), vals)?)
}

/// Import CSV text into `table_name` in one columnar pass without intermediate `Vec<Vec<String>>` or `q` formatting.
pub fn import_csv<S: QSession + ?Sized>(sess: &mut S, csv_text: &str, table_name: &str, create_new: bool) -> KResult<usize> {
    let mut headers: Vec<String> = Vec::new();
    let mut col_syms: Vec<crate::sym::SymId> = Vec::new();
    let mut type_chars: Vec<char> = Vec::new();
    let mut builders: Vec<crate::storage::ColBuilder> = Vec::new();
    let mut sample_buf = String::new();
    let mut sample_spans: Vec<Vec<(usize, usize)>> = Vec::new();
    let mut sample_rows = 0usize;
    let mut total_rows = 0usize;
    let mut initialized = false;
    let mut saw_header = false;

    let init_schema = |sess: &mut S,
                       headers: &[String],
                       sample_buf: &str,
                       sample_spans: &[Vec<(usize, usize)>]|
     -> KResult<(Vec<char>, Vec<crate::storage::ColBuilder>)> {
        let tcs: Vec<char> = if create_new {
            let inferred: Vec<char> = (0..headers.len())
                .map(|ci| {
                    let samples: Vec<&str> = sample_spans[ci].iter().map(|&(s, e)| &sample_buf[s..e]).collect();
                    infer_column_type(&samples)
                })
                .collect();
            create_empty_table(sess, table_name, headers, &inferred)?;
            inferred
        } else {
            let info = sess
                .describe_table(table_name)
                .ok_or_else(|| KError::value(table_name.to_string()))?;
            info.columns.iter().map(|c| c.type_char).collect()
        };
        let mut bs: Vec<crate::storage::ColBuilder> =
            tcs.iter().map(|&tc| crate::storage::ColBuilder::with_capacity(tc, 500, true)).collect();
        if !sample_spans.is_empty() {
            let nrows = sample_spans[0].len();
            for r in 0..nrows {
                for (ci, b) in bs.iter_mut().enumerate() {
                    let (s, e) = sample_spans.get(ci).and_then(|v| v.get(r)).copied().unwrap_or((0, 0));
                    b.push(&sample_buf[s..e]);
                }
            }
        }
        Ok((tcs, bs))
    };

    scan_delimited_rows(csv_text, ',', |fields| {
        if !saw_header {
            saw_header = true;
            headers = fields.iter().enumerate().map(|(i, h)| sanitize_ident(h, i)).collect();
            col_syms = headers.iter().map(|h| crate::sym::intern(h)).collect();
            sample_spans = vec![Vec::new(); headers.len()];
            if !create_new {
                let (tcs, bs) = init_schema(sess, &headers, "", &[])?;
                type_chars = tcs;
                builders = bs;
                initialized = true;
            }
            return Ok(());
        }
        total_rows += 1;
        if !initialized {
            for (ci, sp) in sample_spans.iter_mut().enumerate() {
                let cell = fields.get(ci).unwrap_or("");
                let s = sample_buf.len();
                sample_buf.push_str(cell);
                sp.push((s, sample_buf.len()));
            }
            sample_rows += 1;
            if sample_rows == 500 {
                let (tcs, mut bs) = init_schema(sess, &headers, &sample_buf, &sample_spans)?;
                sample_buf.clear();
                sample_spans.clear();
                flush_builders(sess, table_name, &col_syms, &tcs, 500, &mut bs)?;
                type_chars = tcs;
                builders = bs;
                initialized = true;
            }
            return Ok(());
        }
        for (ci, b) in builders.iter_mut().enumerate() {
            b.push(fields.get(ci).unwrap_or(""));
        }
        if total_rows % 500 == 0 {
            flush_builders(sess, table_name, &col_syms, &type_chars, 500, &mut builders)?;
        }
        Ok(())
    })?;

    if !saw_header {
        return Ok(0);
    }
    if !initialized {
        let (tcs, mut bs) = init_schema(sess, &headers, &sample_buf, &sample_spans)?;
        if total_rows > 0 {
            flush_builders(sess, table_name, &col_syms, &tcs, 0, &mut bs)?;
        }
    } else if total_rows % 500 != 0 {
        flush_builders(sess, table_name, &col_syms, &type_chars, 0, &mut builders)?;
    }
    Ok(total_rows)
}

/// Read one logical CSV record from `reader` into `buf` (continuing across embedded newlines inside quotes).
fn read_csv_record(reader: &mut impl BufRead, buf: &mut String) -> std::io::Result<bool> {
    buf.clear();
    let mut in_quotes = false;
    loop {
        let start = buf.len();
        if reader.read_line(buf)? == 0 {
            return Ok(!buf.is_empty());
        }
        for &b in &buf.as_bytes()[start..] {
            if b == b'"' {
                in_quotes = !in_quotes;
            }
        }
        if !in_quotes {
            return Ok(true);
        }
    }
}

/// Stream a delimited file of arbitrary size into `table_name` in columnar batches without `Vec<Vec<String>>` or `q` formatting.
pub fn bulk_import<S: QSession + ?Sized>(
    sess: &mut S,
    path: &std::path::Path,
    table_name: &str,
    delimiter: char,
    has_header: bool,
    create_new: bool,
    batch_size: usize,
    progress: Option<&Arc<AtomicU64>>,
    cancel: Option<&Arc<AtomicBool>>,
) -> KResult<u64> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut scanner = DelimScanner::new();
    let mut line = String::new();
    let bsize = batch_size.max(1);

    // Read first non-empty record
    let mut headers: Vec<String> = Vec::new();
    let mut sample_buf = String::new();
    let mut sample_spans: Vec<Vec<(usize, usize)>> = Vec::new();
    let mut sample_rows = 0usize;

    loop {
        if !read_csv_record(&mut reader, &mut line)? {
            return Ok(0);
        }
        let mut got_first = false;
        scanner.scan(&line, delimiter, |fields| {
            got_first = true;
            if has_header {
                headers = fields.iter().enumerate().map(|(i, h)| sanitize_ident(h, i)).collect();
                sample_spans = vec![Vec::new(); headers.len()];
            } else {
                headers = (0..fields.len()).map(|i| format!("c{i}")).collect();
                sample_spans = vec![Vec::new(); headers.len()];
                for (ci, sp) in sample_spans.iter_mut().enumerate() {
                    let cell = fields.get(ci).unwrap_or("");
                    let s = sample_buf.len();
                    sample_buf.push_str(cell);
                    sp.push((s, sample_buf.len()));
                }
                sample_rows = 1;
            }
            Ok(())
        })?;
        if got_first {
            break;
        }
    }

    let col_syms: Vec<crate::sym::SymId> = headers.iter().map(|h| crate::sym::intern(h)).collect();
    let sample_target = if create_new { 500 } else { 0 };

    while sample_rows < sample_target && read_csv_record(&mut reader, &mut line)? {
        scanner.scan(&line, delimiter, |fields| {
            for (ci, sp) in sample_spans.iter_mut().enumerate() {
                let cell = fields.get(ci).unwrap_or("");
                let s = sample_buf.len();
                sample_buf.push_str(cell);
                sp.push((s, sample_buf.len()));
            }
            sample_rows += 1;
            Ok(())
        })?;
    }

    let type_chars: Vec<char> = if create_new {
        let tcs: Vec<char> = (0..headers.len())
            .map(|ci| {
                let samples: Vec<&str> = sample_spans[ci].iter().map(|&(s, e)| &sample_buf[s..e]).collect();
                infer_column_type(&samples)
            })
            .collect();
        create_empty_table(sess, table_name, &headers, &tcs)?;
        tcs
    } else {
        let info = sess
            .describe_table(table_name)
            .ok_or_else(|| KError::value(table_name.to_string()))?;
        info.columns.iter().map(|c| c.type_char).collect()
    };

    let mut total = 0u64;
    let mut in_batch = 0usize;
    let mut builders: Vec<crate::storage::ColBuilder> =
        type_chars.iter().map(|&tc| crate::storage::ColBuilder::with_capacity(tc, bsize, true)).collect();
    for r in 0..sample_rows {
        for (ci, b) in builders.iter_mut().enumerate() {
            let (s, e) = sample_spans.get(ci).and_then(|v| v.get(r)).copied().unwrap_or((0, 0));
            b.push(&sample_buf[s..e]);
        }
        in_batch += 1;
        if in_batch == bsize {
            if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
                return Err(KError::new("cancelled", "bulk import cancelled"));
            }
            flush_builders(sess, table_name, &col_syms, &type_chars, bsize, &mut builders)?;
            total += in_batch as u64;
            if let Some(p) = progress {
                p.store(total, Ordering::Relaxed);
            }
            in_batch = 0;
        }
    }
    drop(sample_spans);
    drop(sample_buf);

    loop {
        while in_batch < bsize && read_csv_record(&mut reader, &mut line)? {
            scanner.scan(&line, delimiter, |fields| {
                for (ci, b) in builders.iter_mut().enumerate() {
                    b.push(fields.get(ci).unwrap_or(""));
                }
                in_batch += 1;
                Ok(())
            })?;
        }
        if in_batch == 0 {
            break;
        }
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return Err(KError::new("cancelled", "bulk import cancelled"));
        }
        flush_builders(sess, table_name, &col_syms, &type_chars, bsize, &mut builders)?;
        total += in_batch as u64;
        if let Some(p) = progress {
            p.store(total, Ordering::Relaxed);
        }
        in_batch = 0;
    }

    Ok(total)
}
