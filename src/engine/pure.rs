//! Which statements only read, so that they can run on a copy of the workspace while others run too.
//!
//! The answer errs towards "no": anything not known to be harmless runs as before, alone. Should a statement taken to
//! read try to write after all, the copy refuses (`'snapshot`) and it is run again alone.
use crate::ast::{Expr, QSqlKind};
use crate::engine::{namespaces, Workspace};
use crate::parser::{is_builtin_verb, parse_script};
use crate::sym::{intern, resolve, SymId};
use crate::types::{ColVec, Func, K};
use std::collections::HashSet;
use std::sync::Arc;

/// Verbs that write, reach outside the engine, or run code that is only known when they run.
const EFFECTS: [&str; 23] = [
    "setenv", "insert", "upsert", "set", " set", "system", "value", "get", "eval", "reval", "parse", "hopen", "hclose",
    "show", "exit", "0:", "1:", "2:", "hdel", "save", "load", "rsave", "rload",
];

#[derive(Default)]
struct Seen {
    syms: HashSet<SymId>,
    lambdas: HashSet<usize>,
}

struct Reader<'a> {
    ws: &'a Workspace,
    namespace: &'a str,
}

impl<'a> Reader<'a> {
    /// The qualified global symbol `s` resolves to in `ws` (`views` or `vars`), if any.
    fn resolve_global(&self, s: SymId) -> Option<SymId> {
        let text = resolve(s);
        let qualified = (!self.namespace.is_empty() && !text.starts_with('.')).then(|| intern(&format!("{}.{text}", self.namespace)));
        let mut names = qualified.into_iter().chain([s]);
        names.find(|n| self.ws.views.contains_key(n) || self.ws.vars.contains_key(n))
    }

    /// Whether a value (including lambdas, projections, compositions, adverbs, dicts, and general lists) is pure when used/applied.
    fn value_reads(&self, v: &K, seen: &mut Seen) -> bool {
        match v {
            K::Func(f) => self.func_reads(f, seen),
            K::Dict(d) => self.value_reads(&d.keys, seen) && self.value_reads(&d.vals, seen),
            K::Vector(ColVec::General(_, xs)) => xs.iter().all(|x| self.value_reads(x, seen)),
            _ => true,
        }
    }

    fn func_reads(&self, f: &Func, seen: &mut Seen) -> bool {
        match f {
            Func::Prim(s) => {
                let text = resolve(*s);
                !EFFECTS.contains(&text) && (!text.starts_with('.') || namespaces::reads(text))
            }
            Func::Lambda(lam) => {
                let ptr = Arc::as_ptr(lam) as usize;
                if !seen.lambdas.insert(ptr) {
                    return true;
                }
                Reader { ws: self.ws, namespace: &lam.ns }.lambda_reads(&lam.params, &lam.body, seen)
            }
            Func::Proj { func, args, .. } => self.func_reads(func, seen) && args.iter().flatten().all(|a| self.value_reads(a, seen)),
            Func::Comp(a, b) => self.func_reads(a, seen) && self.func_reads(b, seen),
            Func::Adverb { inner, .. } => self.func_reads(inner, seen),
        }
    }

    fn lambda_reads(&self, params: &[SymId], body: &[Expr], seen: &mut Seen) -> bool {
        let mut locals: HashSet<SymId> = params.iter().copied().collect();
        for stmt in body {
            if let Expr::Assign { name, global: false, op: None, val } = stmt {
                if !resolve(*name).starts_with('.') {
                    if !self.reads(val, Some(&locals), seen) {
                        return false;
                    }
                    locals.insert(*name);
                    continue;
                }
            }
            if !self.reads(stmt, Some(&locals), seen) {
                return false;
            }
        }
        true
    }

    /// Whether the verb or name `s` can be used by a reader.
    fn name(&self, s: SymId, locals: Option<&HashSet<SymId>>, seen: &mut Seen) -> bool {
        let text = resolve(s);
        if EFFECTS.contains(&text) {
            return false;
        }
        if locals.is_some_and(|l| l.contains(&s)) {
            return true;
        }
        match self.resolve_global(s) {
            Some(qname) => {
                if !seen.syms.insert(qname) {
                    return true;
                }
                if let Some(vdef) = self.ws.views.get(&qname) {
                    if vdef.cached.get().is_some() {
                        return true;
                    }
                    let vname = resolve(vdef.name);
                    let vns = vname.rfind('.').filter(|_| vname.starts_with('.')).map_or("", |d| &vname[..d]);
                    return Reader { ws: self.ws, namespace: vns }.reads(&vdef.expr, None, seen);
                }
                self.ws.vars.get(&qname).is_none_or(|val| self.value_reads(val, seen))
            }
            None => !text.starts_with('.') || text.starts_with(".z.") || is_builtin_verb(text) || namespaces::reads(text),
        }
    }

    /// Whether applying `target` may send a message: it is a whole number, as a connection handle is, or a `:host:port` symbol.
    fn handle(&self, target: &Expr, locals: Option<&HashSet<SymId>>) -> bool {
        let number = |v: &K| matches!(v, K::Int(_) | K::Long(_) | K::Short(_));
        let server_sym = |v: &K| matches!(v, K::Sym(s) if resolve(*s).starts_with(':'));
        match target {
            Expr::Lit(v) => number(v) || server_sym(v),
            Expr::Var(s) if !locals.is_some_and(|l| l.contains(s)) => {
                self.resolve_global(*s)
                    .and_then(|q| self.ws.vars.get(&q))
                    .is_some_and(|v| number(v) || server_sym(v))
            }
            // (`neg h`, whatever `h` is, sends or appends)
            Expr::Unary { op, .. } => resolve(*op) == "neg",
            Expr::Apply { target, .. } => matches!(&**target, Expr::Lit(K::Func(Func::Prim(s))) | Expr::Var(s) if resolve(*s) == "neg"),
            _ => false,
        }
    }

    fn reads(&self, e: &Expr, locals: Option<&HashSet<SymId>>, seen: &mut Seen) -> bool {
        let here = match e {
            Expr::Lit(v) => return self.value_reads(v, seen),
            Expr::Lambda { params, body, .. } => return self.lambda_reads(params, body, seen),
            Expr::List(_) | Expr::TableLit { .. } | Expr::Cond(_) | Expr::Nop | Expr::Adverb { .. } | Expr::Compose(..) | Expr::Return(_) => true,
            Expr::If { .. } | Expr::Do { .. } | Expr::While { .. } => true,
            Expr::Assign { name, global: false, op: None, .. } => locals.is_some() && !resolve(*name).starts_with('.'),
            Expr::Assign { name, global: false, op: Some(op), .. } => {
                locals.is_some_and(|l| l.contains(name)) && self.name(*op, locals, seen)
            }
            Expr::IndexAssign { name, op, .. } => {
                locals.is_some_and(|l| l.contains(name)) && op.is_none_or(|o| self.name(o, locals, seen))
            }
            Expr::Field(base, field) => {
                if let Expr::Var(b) = &**base {
                    if !locals.is_some_and(|l| l.contains(b)) {
                        let full = intern(&format!("{}.{}", resolve(*b), resolve(*field)));
                        if self.ws.vars.contains_key(&full) || self.ws.views.contains_key(&full) {
                            return self.name(full, None, seen);
                        }
                    }
                }
                true
            }
            Expr::Var(s) => self.name(*s, locals, seen),
            Expr::Unary { op, .. } => self.name(*op, locals, seen),
            Expr::Binary { op, lhs, rhs } => match resolve(*op) {
                // (`-11!` replays a log, `-25!` broadcasts, `-30!` defers, `0N!` prints)
                "!" if matches!(&**lhs, Expr::Lit(K::Int(_) | K::Long(_))) => false,
                // (`-1 each lines`)
                "each" | "peach" if self.handle(lhs, locals) => false,
                // in-place sort/key/rename on a symbol target `` `a xasc `t ``
                "xasc" | "xdesc" | "xkey" | "xcol" | "xcols" if matches!(&**rhs, Expr::Lit(K::Sym(_))) => false,
                _ => self.name(*op, locals, seen),
            },
            Expr::Apply { target, args } => match &**target {
                // protected evaluation and amends
                Expr::Lit(K::Func(Func::Prim(s))) if args.len() >= 3 && matches!(resolve(*s), "@" | ".") => false,
                Expr::Var(s) if args.len() >= 3 && matches!(resolve(*s), "@" | ".") => false,
                // a number applied to something writes to it
                _ => !self.handle(target, locals),
            },
            Expr::Juxt { func, .. } => !self.handle(func, locals),
            // `update` and `delete` of a table named by a symbol change it
            Expr::Query(q) => matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) || !matches!(&*q.from, Expr::Lit(K::Sym(_))),
            _ => false,
        };
        here && e.children().into_iter().all(|c| self.reads(c, locals, seen))
    }
}

/// Whether `statements`, run in `namespace`, only read.
pub fn reads_only(ws: &Workspace, namespace: &str, statements: &[Expr]) -> bool {
    let reader = Reader { ws, namespace };
    statements.iter().all(|s| reader.reads(s, None, &mut Seen::default()))
}

/// Whether calling `func` only reads the workspace.
pub fn func_reads_only(ws: &Workspace, func: &Func) -> bool {
    Reader { ws, namespace: "" }.func_reads(func, &mut Seen::default())
}

/// Whether a functional IPC message ``(`fn; arg1; arg2)`` only reads.
pub fn message_reads_only(ws: &Workspace, namespace: &str, msg: &K) -> bool {
    let K::Vector(ColVec::General(_, items)) = msg else { return true };
    let Some(head) = items.first() else { return true };
    let reader = Reader { ws, namespace };
    let mut seen = Seen::default();
    let head_ok = match head {
        K::Sym(s) => !resolve(*s).starts_with(':') && reader.name(*s, None, &mut seen),
        K::Func(f) => reader.func_reads(f, &mut seen),
        K::Vector(ColVec::Char(_, code)) => parse_script(&String::from_utf8_lossy(code))
            .ok()
            .is_some_and(|stmts| stmts.iter().all(|s| reader.reads(s, None, &mut seen))),
        _ => false,
    };
    head_ok && items[1..].iter().all(|a| reader.value_reads(a, &mut seen))
}
