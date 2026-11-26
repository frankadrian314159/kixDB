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
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();

    while let Some(c) = chars.next() {
        if quoted {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        if c == '"' && field.is_empty() {
            quoted = true;
        } else if c == delim {
            row.push(std::mem::take(&mut field));
        } else if c == '\r' || c == '\n' {
            if c == '\r' && chars.peek() == Some(&'\n') {
                chars.next();
            }
            row.push(std::mem::take(&mut field));
            if !(row.len() == 1 && row[0].is_empty()) {
                rows.push(std::mem::take(&mut row));
            } else {
                row.clear();
            }
        } else {
            field.push(c);
        }
    }
    if quoted {
        return Err(KError::syntax("unterminated quoted CSV field"));
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
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
}

/// Import CSV text into `table_name` (creating it with inferred types when `create_new` is true).
pub fn import_csv<S: QSession + ?Sized>(sess: &mut S, csv_text: &str, table_name: &str, create_new: bool) -> KResult<usize> {
    let rows = parse_csv(csv_text)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let headers: Vec<String> = rows[0].iter().enumerate().map(|(i, h)| sanitize_ident(h, i)).collect();
    let data = &rows[1..];

    let type_chars: Vec<char> = if create_new {
        let tcs: Vec<char> = (0..headers.len())
            .map(|ci| {
                let col_samples: Vec<&str> = data.iter().take(500).filter_map(|r| r.get(ci).map(String::as_str)).collect();
                infer_column_type(&col_samples)
            })
            .collect();
        let col_defs: Vec<String> = headers
            .iter()
            .zip(&tcs)
            .map(|(h, &tc)| {
                let empty = ColVec::empty_of_type_char(tc)
                    .map(|cv| format!("`{}$()", cv.type_name()))
                    .unwrap_or_else(|| "()".into());
                format!("{h}: {empty}")
            })
            .collect();
        sess.run_q(&format!("{table_name}: ([] {})", col_defs.join("; ")))?;
        tcs
    } else {
        let info = sess
            .describe_table(table_name)
            .ok_or_else(|| KError::value(table_name.to_string()))?;
        info.columns.iter().map(|c| c.type_char).collect()
    };

    for chunk in data.chunks(500) {
        let batch_q = build_batch_insert_q(table_name, &headers, &type_chars, chunk)?;
        sess.run_q(&batch_q)?;
    }
    Ok(data.len())
}

/// Stream a delimited file of arbitrary size into `table_name` in batches without loading the file into RAM.
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
    let mut line = String::new();

    let first_len = reader.read_line(&mut line)?;
    if first_len == 0 {
        return Ok(0);
    }
    let first_parsed = parse_delimited(&line, delimiter)?;
    let Some(first_row) = first_parsed.into_iter().next() else {
        return Ok(0);
    };

    let mut pending_rows: Vec<Vec<String>> = Vec::new();
    let headers: Vec<String> = if has_header {
        first_row.iter().enumerate().map(|(i, h)| sanitize_ident(h, i)).collect()
    } else {
        let ncols = first_row.len();
        pending_rows.push(first_row);
        (0..ncols).map(|i| format!("c{i}")).collect()
    };

    let bsize = batch_size.max(1);
    while pending_rows.len() < bsize {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if let Some(r) = parse_delimited(&line, delimiter)?.into_iter().next() {
            pending_rows.push(r);
        }
    }

    let type_chars: Vec<char> = if create_new {
        let tcs: Vec<char> = (0..headers.len())
            .map(|ci| {
                let samples: Vec<&str> = pending_rows.iter().filter_map(|r| r.get(ci).map(String::as_str)).collect();
                infer_column_type(&samples)
            })
            .collect();
        let col_defs: Vec<String> = headers
            .iter()
            .zip(&tcs)
            .map(|(h, &tc)| {
                let empty = ColVec::empty_of_type_char(tc)
                    .map(|cv| format!("`{}$()", cv.type_name()))
                    .unwrap_or_else(|| "()".into());
                format!("{h}: {empty}")
            })
            .collect();
        sess.run_q(&format!("{table_name}: ([] {})", col_defs.join("; ")))?;
        tcs
    } else {
        let info = sess
            .describe_table(table_name)
            .ok_or_else(|| KError::value(table_name.to_string()))?;
        info.columns.iter().map(|c| c.type_char).collect()
    };

    let mut total = 0u64;
    while !pending_rows.is_empty() {
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return Err(KError::new("cancelled", "bulk import cancelled"));
        }
        let q = build_batch_insert_q(table_name, &headers, &type_chars, &pending_rows)?;
        sess.run_q(&q)?;
        total += pending_rows.len() as u64;
        if let Some(p) = progress {
            p.store(total, Ordering::Relaxed);
        }
        pending_rows.clear();
        while pending_rows.len() < bsize {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            if let Some(r) = parse_delimited(&line, delimiter)?.into_iter().next() {
                pending_rows.push(r);
            }
        }
    }

    Ok(total)
}

fn build_batch_insert_q(
    table_name: &str,
    headers: &[String],
    type_chars: &[char],
    rows: &[Vec<String>],
) -> KResult<String> {
    if rows.is_empty() {
        return Ok(String::new());
    }
    let mut col_exprs = Vec::with_capacity(headers.len());
    for (ci, (h, &tc)) in headers.iter().zip(type_chars).enumerate() {
        if tc == 'C' {
            let items: Vec<String> = rows
                .iter()
                .map(|r| q_literal_for_column(r.get(ci).map(String::as_str).unwrap_or(""), 'C'))
                .collect::<KResult<_>>()?;
            let rhs = if items.len() == 1 {
                format!("enlist {}", items[0])
            } else {
                format!("({})", items.join("; "))
            };
            col_exprs.push(format!("{h}: {rhs}"));
        } else {
            let atoms: Vec<K> = rows
                .iter()
                .map(|r| crate::engine::ops::parse_atom_from_str(tc.to_ascii_lowercase(), r.get(ci).map(String::as_str).unwrap_or("")))
                .collect();
            let cv = ColVec::from_atoms(atoms);
            col_exprs.push(format!("{h}: {}", K::Vector(cv).q_repr()));
        }
    }
    Ok(format!("`{table_name} upsert ([] {})", col_exprs.join("; ")))
}
