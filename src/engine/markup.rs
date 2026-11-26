//! The text helpers of the `.h` namespace: delimited text, XML, spreadsheet XML, HTML elements and escapes. (Those
//! that serve web pages need a web server, which kixdb does not have.)
use crate::engine::ops::index_k;
use crate::engine::{json, Engine};
use crate::error::{KError, KResult};
use crate::sym::{intern, resolve};
use crate::types::*;
use std::sync::Arc;

/// Each function and the number of its arguments. (`.h.tx.json` is the JSON lines of `.h.tx`.)
pub const FUNCTIONS: [(&str, usize); 26] = [
    (".h.cd", 1), (".h.code", 1), (".h.ed", 1), (".h.edsn", 1), (".h.ha", 2), (".h.hb", 2), (".h.hc", 1), (".h.hr", 1),
    (".h.hta", 2), (".h.htac", 3), (".h.htc", 2), (".h.html", 1), (".h.http", 1), (".h.hu", 1), (".h.hug", 1),
    (".h.iso8601", 1), (".h.nbr", 1), (".h.pre", 1), (".h.td", 1), (".h.text", 1), (".h.uh", 1), (".h.xd", 1),
    (".h.xmp", 1), (".h.xs", 1), (".h.xt", 2), (".h.tx.json", 1),
];

const ANCHOR_STYLE: &str = "a{text-decoration:none}a:link{color:024C7E}a:visited{color:024C7E}a:active{color:958600}";
const BODY_STYLE: &str = "body{font:10pt verdana;text-align:justify}";
/// The characters that a URI need not escape.
const URI_SAFE: &str = "$-.+!*'(),abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789";
const SPREADSHEET: &str = "urn:schemas-microsoft-com:office:spreadsheet";

fn strings(lines: Vec<String>) -> K {
    K::Vector(ColVec::General(Attr::None, Arc::new(lines.iter().map(|l| K::string(l)).collect())))
}

fn symbols(names: &[&str]) -> Box<K> {
    Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(names.iter().map(|n| intern(n)).collect()))))
}

fn dictionary(names: &[&str], values: Vec<K>) -> K {
    K::Dict(Arc::new(Dict { keys: symbols(names), vals: Box::new(K::Vector(ColVec::from_atoms(values))) }))
}

pub fn constant(name: &str) -> Option<K> {
    Some(match name {
        ".h.br" => K::string("<br>"),
        ".h.c0" => K::sym("024C7E"),
        ".h.c1" => K::sym("958600"),
        ".h.d" => K::string(" "),
        ".h.sa" => K::string(ANCHOR_STYLE),
        ".h.sb" => K::string(BODY_STYLE),
        ".h.sc" => K::string(URI_SAFE),
        ".h.tx" => {
            let verbs = ["enlist", ".h.tx.json", ".h.cd", ".h.td", ".h.xd", ".h.ed"];
            dictionary(&["raw", "json", "csv", "txt", "xml", "xls"], verbs.map(|v| K::Func(Func::Prim(intern(v)))).to_vec())
        }
        ".h.ty" => {
            let types = [
                ("htm", "text/html"), ("html", "text/html"), ("csv", "text/comma-separated-values"), ("txt", "text/plain"),
                ("xml", "text/plain"), ("xls", "application/msexcel"), ("gif", "image/gif"), ("png", "image/png"),
                ("jpg", "image/jpeg"), ("css", "text/css"), ("js", "application/javascript"), ("json", "application/json"),
            ];
            dictionary(&types.map(|t| t.0), types.map(|t| K::string(t.1)).to_vec())
        }
        _ => return None,
    })
}

/// The strings of `x`: one, or a list of them.
fn lines(x: &K) -> Vec<String> {
    match x {
        K::Vector(ColVec::General(_, items)) => items.iter().map(|i| i.as_str().unwrap_or_else(|| i.cell_text())).collect(),
        K::Vector(ColVec::Sym(_, names)) => names.iter().map(|&n| resolve(n)).collect(),
        one => vec![one.as_str().unwrap_or_else(|| one.cell_text())],
    }
}

fn text(x: &K) -> String {
    x.as_str().unwrap_or_else(|| x.cell_text())
}

fn table(x: &K) -> KResult<Flip> {
    match x {
        K::Table(t) => Ok((**t).clone()),
        K::KeyedTable(t) => Ok(t.unkey()),
        _ => Err(KError::ty("expected a table")),
    }
}

/// XML's special characters, as their entities.
fn escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

fn uri_escape(c: u8) -> String {
    match URI_SAFE.as_bytes().contains(&c) {
        true => (c as char).to_string(),
        false => format!("%{c:02x}"),
    }
}

fn uri_unescape(s: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    let mut i = 0;
    while i < s.len() {
        let code = s.get(i + 1..i + 3).filter(|h| s[i] == b'%' && h.iter().all(u8::is_ascii_hexdigit));
        match code.and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok()) {
            Some(c) => (out.push(c), i += 3),
            None => (out.push(s[i]), i += 1),
        };
    }
    out
}

/// `x` with the web addresses in it made links.
fn link_addresses(mut rest: &str) -> String {
    let mut out = String::new();
    while let Some(at) = rest.find("http://") {
        let end = rest[at..].find(|c: char| c.is_whitespace() || c == '<' || c == '"').map_or(rest.len(), |n| at + n);
        let url = &rest[at..end];
        out += &format!("{}<a href=\"{url}\">{url}</a>", &rest[..at]);
        rest = &rest[end..];
    }
    out + rest
}

/// An opening tag with the attributes of a dictionary.
fn open_tag(element: &K, attributes: &K) -> KResult<String> {
    let K::Dict(d) = attributes else { return Err(KError::ty("attributes are a dictionary")) };
    let mut tag = format!("<{}", text(element));
    for i in 0..d.keys.count() {
        let at = [Some(K::Long(i as i64))];
        tag += &format!(" {}=\"{}\"", text(&index_k(&d.keys, &at)?), text(&index_k(&d.vals, &at)?));
    }
    Ok(tag + ">")
}

/// The sheets of a spreadsheet as XML, a line a row.
fn spreadsheet(sheets: &[(String, Flip)]) -> K {
    let cell = |value: &K| {
        let number = value.as_f64().is_ok_and(f64::is_finite) && matches!(value.type_num(), -9..=-4);
        let kind = if number { "Number" } else { "String" };
        format!("<Cell><Data ss:Type=\"{kind}\">{}</Data></Cell>", escape(&value.data_text()))
    };
    let row = |cells: Vec<String>| format!("<Row>{}</Row>", cells.concat());
    let mut out = vec![
        "<?xml version=\"1.0\"?><?mso-application progid=\"Excel.Sheet\"?>".to_string(),
        format!("<Workbook xmlns=\"{SPREADSHEET}\" xmlns:ss=\"{SPREADSHEET}\">"),
    ];
    for (name, t) in sheets {
        out.push(format!("<Worksheet ss:Name=\"{}\"><Table>", escape(name)));
        out.push(row(t.cols.iter().map(|&c| cell(&K::Sym(c))).collect()));
        out.extend((0..t.nrows()).map(|r| row(t.vals.iter().map(|c| cell(&c.get_atom(r))).collect())));
        out.push("</Table></Worksheet>".into());
    }
    out.push("</Workbook>".into());
    strings(out)
}

impl Engine<'_> {
    /// `.h.cd` and `.h.td`: a table (or a list of columns, which has no header) as lines of delimited text. The items of
    /// a nested column are joined by `.h.d`.
    fn delimited(&mut self, x: &K, delimiter: u8) -> KResult<K> {
        let between = self.ws.vars.get(&intern(".h.d")).map_or_else(|| " ".to_string(), text);
        let (flip, header) = match x {
            K::Vector(ColVec::General(_, columns)) => {
                let column = |c: &K| match c {
                    K::Vector(v) => v.clone(),
                    atom => ColVec::from_atoms(vec![atom.clone()]),
                };
                (Flip::new(vec![intern("x"); columns.len()], columns.iter().map(column).collect())?, false)
            }
            other => (table(other)?, true),
        };
        let joined = |item: &K| match item {
            K::Vector(v) if !matches!(v, ColVec::Char(..)) => {
                K::string(&(0..v.len()).map(|i| v.get_atom(i).data_text()).collect::<Vec<_>>().join(&between))
            }
            other => other.clone(),
        };
        let vals = flip.vals.iter().map(|c| match c {
            ColVec::General(_, items) => ColVec::General(Attr::None, Arc::new(items.iter().map(joined).collect())),
            simple => simple.clone(),
        });
        let flip = Flip { cols: flip.cols.clone(), vals: vals.collect() };
        let made = crate::storage::delimited_lines(char::from(delimiter), &flip);
        Ok(match made {
            K::Vector(v) if !header => K::Vector(v.drop_n(1)),
            all => all,
        })
    }

    /// Call a function of `FUNCTIONS`, given as many arguments as it has.
    pub(crate) fn markup_call(&mut self, name: &str, args: &[K]) -> KResult<K> {
        let x = &args[0];
        let y = args.get(1).unwrap_or(&K::Null);
        let wrap = |tag: &str, content: &str| K::string(&format!("<{tag}>{content}</{tag}>"));
        let block = |tag: &str| wrap(tag, &lines(x).iter().map(|l| format!("{l}\n")).collect::<String>());
        let anchor = |target: &str| {
            let href = match x {
                K::Sym(s) => resolve(*s),
                other => format!("\"{}\"", text(other)),
            };
            K::string(&format!("<a{target} href={href}>{}</a>", text(y)))
        };
        Ok(match name {
            ".h.cd" => self.delimited(x, b',')?,
            ".h.td" => self.delimited(x, b'\t')?,
            ".h.tx.json" => {
                let rows = match x {
                    K::Table(_) | K::KeyedTable(_) => {
                        let t = table(x)?;
                        (0..t.nrows()).map(|r| K::Dict(Arc::new(t.row_dict(r)))).collect()
                    }
                    K::Vector(v) => (0..v.len()).map(|i| v.get_atom(i)).collect(),
                    one => vec![one.clone()],
                };
                strings(rows.iter().map(|r| json::write(r, false)).collect())
            }
            ".h.xd" => {
                let t = table(x)?;
                let field = |r, (c, v): (&crate::sym::SymId, &ColVec)| format!("<{0}>{1}</{0}>", resolve(*c), escape(&text(&v.get_atom(r))));
                let row = |r| format!("<r>{}</r>", t.cols.iter().zip(&t.vals).map(|column| field(r, column)).collect::<String>());
                let rows = (0..t.nrows()).map(row);
                strings(std::iter::once("<R>".to_string()).chain(rows).chain(["</R>".to_string()]).collect())
            }
            ".h.ed" => spreadsheet(&[("Sheet1".into(), table(x)?)]),
            ".h.edsn" => {
                let K::Dict(d) = x else { return Err(KError::ty(".h.edsn takes names and their tables")) };
                let sheet = |i: usize| {
                    let at = [Some(K::Long(i as i64))];
                    Ok((text(&index_k(&d.keys, &at)?), table(&index_k(&d.vals, &at)?)?))
                };
                spreadsheet(&(0..d.keys.count()).map(sheet).collect::<KResult<Vec<_>>>()?)
            }
            ".h.xt" => {
                if !matches!(x, K::Sym(s) if resolve(*s) == "json") {
                    return Err(KError::domain(".h.xt reads `json"));
                }
                crate::engine::rows::collapse(lines(y).iter().map(|l| json::read(l)).collect::<KResult<_>>()?)
            }
            ".h.xs" => K::string(&escape(&text(x))),
            ".h.hc" => K::string(&text(x).replace('<', "&lt;")),
            ".h.hr" => K::string(&"-".repeat(text(x).chars().count())),
            ".h.hu" => K::string(&text(x).bytes().map(uri_escape).collect::<String>()),
            ".h.uh" => K::string(&String::from_utf8_lossy(&uri_unescape(text(x).as_bytes()))),
            ".h.hug" => {
                let kept = text(x);
                let escaped = |c: u8| K::string(&if kept.as_bytes().contains(&c) { (c as char).to_string() } else { format!("%{c:02x}") });
                let keys = K::Vector(ColVec::Char(Attr::None, Arc::new((0..=255).collect())));
                let vals = K::Vector(ColVec::General(Attr::None, Arc::new((0..=255).map(escaped).collect())));
                K::Dict(Arc::new(Dict { keys: Box::new(keys), vals: Box::new(vals) }))
            }
            ".h.iso8601" => K::string(&format_timestamp(x.as_i64()?).replacen('.', "-", 2).replace('D', "T")),
            ".h.code" => {
                let marked = text(x).split('\t').enumerate().map(|(i, s)| match i % 2 {
                    0 => s.to_string(),
                    _ => format!("<code><nobr>{s}</nobr></code>"),
                }).collect::<Vec<_>>();
                K::string(&marked.join(" "))
            }
            ".h.ha" => anchor(""),
            ".h.hb" => anchor(" target=v"),
            ".h.hta" => K::string(&open_tag(x, y)?),
            ".h.htac" => K::string(&format!("{}{}</{}>", open_tag(x, y)?, text(&args[2]), text(x))),
            ".h.htc" => wrap(&text(x), &text(y)),
            ".h.html" => K::string(&format!("<html><head><style>{ANCHOR_STYLE}{BODY_STYLE}</style></head><body>{}</body></html>", text(x))),
            ".h.http" => K::string(&link_addresses(&text(x))),
            ".h.nbr" => wrap("nobr", &text(x)),
            ".h.pre" => block("pre"),
            ".h.xmp" => block("xmp"),
            ".h.text" => K::string(&lines(x).iter().map(|l| format!("<p>{l}</p>\n")).collect::<String>()),
            _ => return Err(KError::nyi(name.to_string())),
        })
    }
}
