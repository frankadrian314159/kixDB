//! Which statements only read, so that they can run on a copy of the workspace while others run too.
//!
//! The answer errs towards "no": anything not known to be harmless runs as before, alone. Should a statement taken to
//! read try to write after all, the copy refuses (`'snapshot`) and it is run again alone.
use crate::ast::{Expr, QSqlKind};
use crate::engine::{namespaces, Workspace};
use crate::parser::is_builtin_verb;
use crate::sym::{intern, resolve, SymId};
use crate::types::{Func, K};

/// Verbs that write, reach outside the engine, or run code that is only known when they run.
const EFFECTS: [&str; 22] = ["setenv", "insert", "upsert", "set", " set", "system", "value", "get", "eval", "reval", "parse", "hopen", "hclose", "show", "exit", "0:", "1:", "2:", "hdel", "save", "load", "rsave"];

struct Reader<'a> {
    ws: &'a Workspace,
    namespace: &'a str,
}

impl<'a> Reader<'a> {
    /// The global `s` stands for: `None` inside for a view.
    fn global(&self, s: SymId) -> Option<Option<&'a K>> {
        let text = resolve(s);
        let qualified = (!self.namespace.is_empty() && !text.starts_with('.')).then(|| intern(&format!("{}.{text}", self.namespace)));
        let mut names = qualified.into_iter().chain([s]);
        names.find_map(|n| self.ws.views.get(&n).map(|_| None).or_else(|| self.ws.vars.get(&n).map(Some)))
    }

    /// Whether the verb or name `s` can be used by a reader.
    fn name(&self, s: SymId) -> bool {
        let text = resolve(s);
        if EFFECTS.contains(&text.as_str()) {
            return false;
        }
        match self.global(s) {
            // a view keeps what it computes
            Some(None) => false,
            // a function may do anything
            Some(Some(value)) => !matches!(value, K::Func(_)),
            None => !text.starts_with('.') || text.starts_with(".z.") || is_builtin_verb(&text) || namespaces::reads(&text),
        }
    }

    /// Whether applying `target` may send a message: it is a whole number, as a connection handle is.
    fn handle(&self, target: &Expr) -> bool {
        let number = |v: &K| matches!(v, K::Int(_) | K::Long(_) | K::Short(_));
        match target {
            Expr::Lit(v) => number(v),
            Expr::Var(s) => self.global(*s).flatten().is_some_and(number),
            // (`neg h`, whatever `h` is, sends or appends)
            Expr::Unary { op, .. } => resolve(*op) == "neg",
            Expr::Apply { target, .. } => matches!(&**target, Expr::Lit(K::Func(Func::Prim(s))) | Expr::Var(s) if resolve(*s) == "neg"),
            _ => false,
        }
    }

    fn reads(&self, e: &Expr) -> bool {
        let here = match e {
            Expr::Lit(K::Func(Func::Prim(s))) => !EFFECTS.contains(&resolve(*s).as_str()),
            Expr::Lit(K::Func(_)) => false,
            Expr::Lit(_) | Expr::List(_) | Expr::TableLit { .. } | Expr::Field(..) | Expr::Cond(_) | Expr::Nop | Expr::Adverb { .. } => true,
            Expr::Var(s) => self.name(*s),
            Expr::Unary { op, .. } => self.name(*op),
            // (`-11!` replays a log)
            Expr::Binary { op, lhs, .. } => self.name(*op) && !(resolve(*op) == "!" && matches!(&**lhs, Expr::Lit(K::Int(_) | K::Long(_)))),
            Expr::Apply { target, args } => match &**target {
                // protected evaluation and amends
                Expr::Lit(K::Func(Func::Prim(s))) if args.len() >= 3 && matches!(resolve(*s).as_str(), "@" | ".") => false,
                Expr::Var(s) if args.len() >= 3 && matches!(resolve(*s).as_str(), "@" | ".") => false,
                // a number applied to something writes to it
                _ => !self.handle(target),
            },
            Expr::Juxt { func, .. } => !self.handle(func),
            // `update` and `delete` of a table named by a symbol change it
            Expr::Query(q) => matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) || !matches!(&*q.from, Expr::Lit(K::Sym(_))),
            _ => false,
        };
        here && e.children().into_iter().all(|c| self.reads(c))
    }
}

/// Whether `statements`, run in `namespace`, only read.
pub fn reads_only(ws: &Workspace, namespace: &str, statements: &[Expr]) -> bool {
    let reader = Reader { ws, namespace };
    statements.iter().all(|s| reader.reads(s))
}
