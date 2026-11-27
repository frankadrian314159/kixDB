//! Recursive-descent parser for `q` and `qSQL`.
//!
//! `q` has **no operator precedence hierarchy**: binary operators and keyword verbs all associate strictly
//! **right-to-left** (`2 * 3 + 4` parses as `2 * (3 + 4)`), while unary verbs consume the entire expression to their right
//! (`sum 1 + til 5` parses as `sum (1 + (til 5))`). Postfix indexing (`x[i;j]`) and adverbs (`+/`, `f'`) bind tightly to
//! the term on their left.
use crate::ast::{ColSpec, Expr, QSql, QSqlKind};
use crate::error::{KError, KResult};
use crate::lexer::{tokenize, Tok};
use crate::sym::{intern, resolve, SymId};
use crate::types::{Attr, ColVec, Func, K};
use std::sync::Arc;

/// Unary keyword verbs in `q` (when used without brackets in prefix position `verb expr`, they take the whole RHS).
pub fn is_unary_keyword(s: &str) -> bool {
    matches!(
        s,
        "neg"
            | "not"
            | "null"
            | "string"
            | "reciprocal"
            | "floor"
            | "ceiling"
            | "signum"
            | "abs"
            | "sqrt"
            | "exp"
            | "log"
            | "sin"
            | "cos"
            | "tan"
            | "asin"
            | "acos"
            | "atan"
            | "enlist"
            | "count"
            | "first"
            | "last"
            | "sorts"
            | "iasc"
            | "idesc"
            | "asc"
            | "desc"
            | "reverse"
            | "distinct"
            | "group"
            | "where"
            | "flip"
            | "type"
            | "key"
            | "keys"
            | "cols"
            | "meta"
            | "fkeys"
            | "tables"
            | "views"
            | "value"
            | "get"
            | "raze"
            | "til"
            | "sum"
            | "prd"
            | "min"
            | "max"
            | "avg"
            | "med"
            | "var"
            | "dev"
            | "svar"
            | "sdev"
            | "sums"
            | "prds"
            | "mins"
            | "maxs"
            | "avgs"
            | "deltas"
            | "ratios"
            | "differ"
            | "prev"
            | "next"
            | "fills"
            | "upper"
            | "lower"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "md5"
            | "all"
            | "any"
            | "rank"
            | "attr"
            | "inv"
            | "ltime"
            | "gtime"
            | "getenv"
            | "hopen"
            | "hclose"
            | "hcount"
            | "hsym"
            | "hdel"
            | "read0"
            | "read1"
            | "save"
            | "load"
            | "rsave"
            | "rload"
            | "system"
            | "show"
            | "ungroup"
            | "parse"
            | "eval"
    )
}

/// Binary keyword verbs in `q` (can be written infix `lhs verb rhs` OR bracketed `verb[lhs; rhs]`).
pub fn is_binary_keyword(s: &str) -> bool {
    matches!(
        s,
        "and"
            | "or"
            | "each"
            | "over"
            | "scan"
            | "prior"
            | "in"
            | "within"
            | "like"
            | "ss"
            | "vs"
            | "sv"
            | "div"
            | "mod"
            | "xexp"
            | "xlog"
            | "xbar"
            | "xasc"
            | "xdesc"
            | "xkey"
            | "xcol"
            | "xcols"
            | "xgroup"
            | "sublist"
            | "mavg"
            | "msum"
            | "mcount"
            | "mmin"
            | "mmax"
            | "mdev"
            | "ema"
            | "wavg"
            | "wsum"
            | "cov"
            | "cor"
            | "bin"
            | "binr"
            | "ij"
            | "lj"
            | "uj"
            | "ej"
            | "pj"
            | "asof"
            | "inter"
            | "union"
            | "except"
            | "cross"
            | "cut"
            | "rotate"
            | "xprev"
            | "xrank"
            | "scov"
            | "mmu"
            | "lsq"
            | "setenv"
            | "peach"
            | " set"
            | "set"
            | "upsert"
            | "insert"
            | "fby"
    )
}

/// Multi-argument (>2) built-in keywords called as `aj[cols; t1; t2]`, `wj[...]`, `ssr[s; pat; rep]`.
pub fn is_nary_keyword(s: &str) -> bool {
    matches!(s, "aj" | "aj0" | "wj" | "wj1" | "ssr")
}

pub fn is_builtin_verb(s: &str) -> bool {
    is_unary_keyword(s) || is_binary_keyword(s) || is_nary_keyword(s)
}

pub fn parse_script(src: &str) -> KResult<Vec<Expr>> {
    let toks = tokenize(src)?;
    let mut p = Parser::new(&toks);
    p.parse_seq(false)
}

struct Parser<'a> {
    toks: &'a [Tok],
    pos: usize,
    /// When true, `,` separates qSQL clause items rather than acting as the binary concat operator!
    stop_on_comma: bool,
    /// When true, top-level keywords `by`, `from`, `where` terminate an expression inside a qSQL statement.
    in_qsql: bool,
}

impl<'a> Parser<'a> {
    fn new(toks: &'a [Tok]) -> Self {
        Self { toks, pos: 0, stop_on_comma: false, in_qsql: false }
    }

    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::Eof)
    }

    fn peek_n(&self, n: usize) -> &Tok {
        self.toks.get(self.pos + n).unwrap_or(&Tok::Eof)
    }

    fn next(&mut self) -> Tok {
        let t = self.peek().clone();
        if self.pos < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn skip_newlines(&mut self) {
        while matches!(self.peek(), Tok::Newline) {
            self.pos += 1;
        }
    }

    fn is_expr_end(&self) -> bool {
        match self.peek() {
            Tok::Eof | Tok::Semi | Tok::Newline | Tok::RParen | Tok::RBrack => true,
            Tok::Comma if self.stop_on_comma => true,
            Tok::Ident(s) if self.in_qsql && matches!(s.as_str(), "by" | "from" | "where") => true,
            _ => false,
        }
    }

    fn parse_seq(&mut self, inside_lambda: bool) -> KResult<Vec<Expr>> {
        let mut stmts = Vec::new();
        loop {
            while matches!(self.peek(), Tok::Newline | Tok::Semi) {
                self.pos += 1;
            }
            if matches!(self.peek(), Tok::Eof | Tok::RParen | Tok::RBrack) {
                break;
            }
            if let Tok::SysCmd { cmd, arg } = self.peek().clone() {
                self.next();
                stmts.push(Expr::SysCmd { cmd, arg });
                continue;
            }
            stmts.push(self.parse_expr()?);
            // If a semicolon terminates the last statement at top-level, kdb+ suppresses printing (returns `::` Null)!
            if !inside_lambda && matches!(self.peek(), Tok::Semi) {
                while matches!(self.peek(), Tok::Semi) {
                    self.pos += 1;
                }
                if matches!(self.peek(), Tok::Eof | Tok::Newline) {
                    stmts.push(Expr::Lit(K::Null));
                }
            }
        }
        Ok(stmts)
    }

    fn parse_expr(&mut self) -> KResult<Expr> {
        if self.is_expr_end() {
            return Ok(Expr::Lit(K::Null));
        }

        // Control structures: `if[...]`, `do[...]`, `while[...]`, `$[...]`, qSQL `select`/`exec`/`update`/`delete`
        if matches!(self.peek(), Tok::CondOpen) {
            return self.parse_cond();
        }
        if let Tok::Ident(kw) = self.peek() {
            match kw.as_str() {
                "if" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("if"),
                "do" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("do"),
                "while" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("while"),
                "select" | "exec" | "update" | "delete" => return self.parse_qsql(),
                _ => {}
            }
        }

        // `:expr` returns early from a lambda (also from inside `if[...]` and friends)
        if matches!(self.peek(), Tok::Colon) {
            self.next();
            return Ok(Expr::Return(Box::new(self.parse_expr()?)));
        }

        // Prefix signal `'err`
        if matches!(self.peek(), Tok::Quote) {
            self.next();
            let arg = self.parse_expr()?;
            return Ok(Expr::Signal(Box::new(arg)));
        }

        // Assignment forms:
        //   `ident : rhs`
        //   `ident :: rhs`
        //   `ident +: rhs` / `ident +:: rhs`
        //   `ident[idx...] : rhs` / `ident[idx...] +: rhs` (`::` and `+::` too, though these reach a global as they are)
        if let Tok::Ident(name) = self.peek().clone() {
            if !is_builtin_verb(&name) || matches!(self.peek_n(1), Tok::Colon | Tok::DoubleColon) {
                match self.peek_n(1) {
                    Tok::Colon => {
                        self.next();
                        self.next();
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(&name), global: false, op: None, val: Box::new(val) });
                    }
                    Tok::DoubleColon => {
                        self.next();
                        self.next();
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(&name), global: true, op: None, val: Box::new(val) });
                    }
                    Tok::OpColon(op) => {
                        let op = intern(op);
                        self.next();
                        self.next();
                        // (a second colon, as in `x,::y`, says that it is the global that is changed)
                        let global = matches!(self.peek(), Tok::Colon);
                        if global {
                            self.next();
                        }
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(&name), global, op: Some(op), val: Box::new(val) });
                    }
                    Tok::LBrack => {
                        // Look ahead past matching `]` to see if `: ` or `op:` follows!
                        if let Some((indices, after_pos, assign_op)) = self.try_parse_indexed_assign(&name)? {
                            self.pos = after_pos;
                            let val = self.parse_expr()?;
                            return Ok(Expr::IndexAssign { name: intern(&name), indices, op: assign_op, val: Box::new(val) });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Prefix unary operator (`-x`, `+x` flip, `*x` first, `!x` til/key, `&x` where, `|x` reverse, `<x` iasc, `>x` idesc, `=x` group, `~x` not, `,x` enlist, `#x` count, `_x` floor, `$x` string, `?x` distinct, `@x` type, `.x` value)
        // Note: only if NOT followed immediately by an adverb (`+/`, `,/`, etc.) or `]` / `)` / `;` (bare verb value!)
        if let Some(op_str) = self.peek_prefix_op() {
            let adverb_word = matches!(self.peek_n(1), Tok::Ident(s) if matches!(s.as_str(), "each" | "over" | "scan" | "prior"));
            if !adverb_word && !matches!(self.peek_n(1), Tok::Adverb(_) | Tok::LBrack) {
                self.next();
                if self.is_expr_end() {
                    return Ok(Expr::Lit(K::Func(Func::Prim(intern(&op_str)))));
                }
                let rhs = self.parse_expr()?;
                return Ok(Expr::Unary { op: intern(&op_str), arg: Box::new(rhs) });
            }
        }

        // Parse left-hand term (atom, vector, ident, list/table, lambda, plus tight postfix `[...]` / adverbs / `.field`)
        let lhs = self.parse_term()?;

        // If `lhs` is a bare unary keyword (`sum`, `til`, `neg`, `not`, ...) and more expression follows without a binary op:
        if let Expr::Var(sym) = &lhs {
            let name = resolve(*sym).to_string();
            // (`value +[2]`: an operator with brackets after it begins the argument)
            let called = matches!(self.peek(), Tok::Op(_)) && matches!(self.peek_n(1), Tok::LBrack);
            if is_unary_keyword(&name) && !self.is_expr_end() && (called || self.peek_binary_op().is_none()) {
                let rhs = self.parse_expr()?;
                return Ok(Expr::Unary { op: *sym, arg: Box::new(rhs) });
            }
        }

        // Check if followed by an adverb-modified infix verb/lambda (`10 +/: 1 2 3`, `0 +/ 1 2 3`, `a {x+y}' b`)
        if !self.is_expr_end()
            && matches!(self.peek(), Tok::Op(_) | Tok::Comma | Tok::Ident(_) | Tok::LBrace { .. })
            && matches!(self.peek_n(1), Tok::Adverb(_))
        {
            let adv_fn = self.parse_term()?;
            let rhs = self.parse_expr()?;
            return Ok(Expr::Apply {
                target: Box::new(adv_fn),
                args: vec![Some(lhs), Some(rhs)],
            });
        }

        // Check if followed by a binary operator (`+`, `-`, `*`, `%`, `,`, `!`, `#`, `_`, `$`, `?`, `@`, `.`, or binary keyword `in`, `xbar`, `lj`, ...)
        if !self.is_expr_end() {
            if let Some(bin_op) = self.peek_binary_op() {
                self.next();
                if self.is_expr_end() {
                    let target = Box::new(Expr::Lit(K::Func(Func::Prim(intern(&bin_op)))));
                    return Ok(Expr::Apply { target, args: vec![Some(lhs), None] });
                }
                // Special case: `0N!x` (debug print and return `x`) vs dict `!`
                let rhs = self.parse_expr()?;
                return Ok(Expr::Binary { op: intern(&bin_op), lhs: Box::new(lhs), rhs: Box::new(rhs) });
            }

            // Juxtaposition application (`f x`, e.g. `{x+1} 5`, `(+/) 1 2 3`, `myfn 10`, `t 0`, `` `s#1 2 3 `` already handled by `#`)!
            let rhs = self.parse_expr()?;
            return Ok(Expr::Juxt { func: Box::new(lhs), arg: Box::new(rhs) });
        }

        Ok(lhs)
    }

    fn peek_prefix_op(&self) -> Option<String> {
        match self.peek() {
            Tok::Op(s) => Some(s.clone()),
            Tok::Comma => Some(",".into()),
            _ => None,
        }
    }

    fn peek_binary_op(&self) -> Option<String> {
        if self.is_expr_end() {
            return None;
        }
        match self.peek() {
            Tok::Op(s) => Some(s.clone()),
            Tok::Comma if !self.stop_on_comma => Some(",".into()),
            Tok::Ident(s) if is_binary_keyword(s) => {
                // Only treat as infix binary operator if NOT followed by `[` with no rhs, wait: `t lj kt[0]` -> `lj` is infix, `kt[0]` is rhs!
                // What if `x mavg[3; y]`? Nobody writes `lhs binverb[a;b]`; if `[` immediately follows `binverb`, wait: `1 + mavg[2; 1 2 3]` has `+` before `mavg`!
                // Here `lhs` was ALREADY parsed, so `lhs binverb ...` is ALWAYS infix `binverb`!
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn try_parse_indexed_assign(&mut self, _name: &str) -> KResult<Option<(Vec<Option<Expr>>, usize, Option<SymId>)>> {
        let saved_pos = self.pos;
        let saved_comma = self.stop_on_comma;
        let saved_qsql = self.in_qsql;
        self.pos += 1; // skip ident
        if !matches!(self.peek(), Tok::LBrack) {
            self.pos = saved_pos;
            return Ok(None);
        }
        self.stop_on_comma = false;
        self.in_qsql = false;
        let args_res = self.parse_bracket_args();
        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        let Ok(indices) = args_res else {
            self.pos = saved_pos;
            return Ok(None);
        };
        let assign = match self.peek() {
            Tok::Colon | Tok::DoubleColon => {
                let after = self.pos + 1;
                self.pos = saved_pos;
                Some((indices, after, None))
            }
            Tok::OpColon(op) => {
                let op_sym = intern(op);
                let after = self.pos + 1 + usize::from(matches!(self.peek_n(1), Tok::Colon));
                self.pos = saved_pos;
                Some((indices, after, Some(op_sym)))
            }
            _ => {
                self.pos = saved_pos;
                None
            }
        };
        Ok(assign)
    }

    fn parse_term(&mut self) -> KResult<Expr> {
        let mut base = self.parse_atom()?;

        // Tight postfix loop: adverbs (`+/`, `f'`), bracket call/indexing (`f[a;b]`), dot field (`time.minute`, `t.col`)
        loop {
            match self.peek() {
                Tok::Adverb(kind) => {
                    let k = *kind;
                    self.next();
                    base = Expr::Adverb { kind: k, func: Box::new(base) };
                }
                Tok::LBrack => {
                    let saved_comma = self.stop_on_comma;
                    let saved_qsql = self.in_qsql;
                    self.stop_on_comma = false;
                    self.in_qsql = false;
                    let mut args = self.parse_bracket_args()?;
                    self.stop_on_comma = saved_comma;
                    self.in_qsql = saved_qsql;
                    // `+[2]`: one argument in brackets leaves an operator waiting for its other
                    let operator = matches!(&base, Expr::Lit(K::Func(Func::Prim(s))) if !resolve(*s).starts_with(|c: char| c.is_ascii_alphanumeric()));
                    if operator && matches!(args.as_slice(), [Some(_)]) {
                        args.push(None);
                    }
                    base = Expr::Apply { target: Box::new(base), args };
                }
                Tok::Field if matches!(self.peek_n(1), Tok::Ident(_)) => {
                    // Check that `.ident` is field/temporal access (`time.minute`, `t.price`)
                    self.next(); // `.`
                    if let Tok::Ident(field) = self.next() {
                        base = Expr::Field(Box::new(base), intern(&field));
                    }
                }
                _ => break,
            }
        }

        Ok(base)
    }

    fn parse_atom(&mut self) -> KResult<Expr> {
        match self.peek().clone() {
            Tok::DoubleColon => {
                self.next();
                Ok(Expr::Lit(K::Null))
            }
            Tok::Lit(k) => {
                self.next();
                Ok(Expr::Lit(k))
            }
            Tok::Ident(s) => {
                self.next();
                Ok(Expr::Var(intern(&s)))
            }
            Tok::Op(op) => {
                // Operator used as a verb value before an adverb or bracket (`+/`, `+[1;2]`, `(+)`)
                self.next();
                Ok(Expr::Lit(K::Func(Func::Prim(intern(&op)))))
            }
            Tok::Comma if !self.stop_on_comma => {
                self.next();
                Ok(Expr::Lit(K::Func(Func::Prim(intern(",")))))
            }
            Tok::LBrace { source, body_tokens } => {
                self.next();
                parse_lambda(&source, &body_tokens)
            }
            Tok::LParen => self.parse_paren_or_table(),
            Tok::LBrack => {
                let saved = (std::mem::replace(&mut self.stop_on_comma, false), std::mem::replace(&mut self.in_qsql, false));
                let args = self.parse_bracket_args();
                (self.stop_on_comma, self.in_qsql) = saved;
                let target = Box::new(Expr::Lit(K::Func(Func::Prim(intern(";")))));
                Ok(Expr::Apply { target, args: args? })
            }
            other => Err(KError::syntax(format!("unexpected token {other:?}"))),
        }
    }

    fn parse_bracket_args(&mut self) -> KResult<Vec<Option<Expr>>> {
        if !matches!(self.next(), Tok::LBrack) {
            return Err(KError::syntax("expected '['"));
        }
        self.skip_newlines();
        if matches!(self.peek(), Tok::RBrack) {
            self.next();
            return Ok(vec![None]);
        }
        let mut args = Vec::new();
        loop {
            self.skip_newlines();
            if matches!(self.peek(), Tok::Semi | Tok::RBrack) {
                args.push(None);
            } else if matches!(self.peek(), Tok::Colon) && matches!(self.peek_n(1), Tok::Semi | Tok::RBrack) {
                self.next();
                args.push(Some(Expr::Lit(K::Func(Func::Prim(intern(":"))))));
            } else {
                args.push(Some(self.parse_expr()?));
            }
            self.skip_newlines();
            match self.peek() {
                Tok::Semi => {
                    self.next();
                }
                Tok::RBrack => {
                    self.next();
                    break;
                }
                other => return Err(KError::syntax(format!("expected ';' or ']', got {other:?}"))),
            }
        }
        Ok(args)
    }

    fn parse_paren_or_table(&mut self) -> KResult<Expr> {
        self.next(); // consume `(`
        let saved_comma = self.stop_on_comma;
        let saved_qsql = self.in_qsql;
        self.stop_on_comma = false;
        self.in_qsql = false;

        let res = (|| -> KResult<Expr> {
            self.skip_newlines();
            // `()` empty general list
            if matches!(self.peek(), Tok::RParen) {
                self.next();
                return Ok(Expr::Lit(K::Vector(ColVec::General(Attr::None, Arc::new(vec![])))));
            }
            // `(::)` generic null
            if matches!(self.peek(), Tok::DoubleColon) && matches!(self.peek_n(1), Tok::RParen) {
                self.next();
                self.next();
                return Ok(Expr::Lit(K::Null));
            }
            // Table literal `([keys...] cols...)`!
            if matches!(self.peek(), Tok::LBrack) {
                return self.parse_table_lit();
            }

            // Parenthesized expression `(expr)` vs general list `(e1; e2; ...)`
            let mut items = Vec::new();
            let mut saw_semi = false;
            loop {
                self.skip_newlines();
                if matches!(self.peek(), Tok::Semi | Tok::RParen) {
                    items.push(Expr::Lit(K::Null));
                } else {
                    items.push(self.parse_expr()?);
                }
                self.skip_newlines();
                match self.peek() {
                    Tok::Semi => {
                        saw_semi = true;
                        self.next();
                    }
                    Tok::RParen => {
                        self.next();
                        break;
                    }
                    other => return Err(KError::syntax(format!("expected ';' or ')', got {other:?}"))),
                }
            }
            if !saw_semi && items.len() == 1 {
                Ok(items.into_iter().next().unwrap())
            } else {
                Ok(Expr::List(items))
            }
        })();

        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        res
    }

    fn parse_table_lit(&mut self) -> KResult<Expr> {
        self.next(); // `[`
        let keys = self.parse_table_col_defs(Tok::RBrack)?;
        self.skip_newlines();
        if matches!(self.peek(), Tok::Semi) {
            self.next();
        }
        let vals = self.parse_table_col_defs(Tok::RParen)?;
        Ok(Expr::TableLit { keys, vals })
    }

    fn parse_table_col_defs(&mut self, end_tok: Tok) -> KResult<Vec<(SymId, Expr)>> {
        let mut cols = Vec::new();
        self.skip_newlines();
        if *self.peek() == end_tok {
            self.next();
            return Ok(cols);
        }
        loop {
            self.skip_newlines();
            if *self.peek() == end_tok {
                self.next();
                break;
            }
            let (name, expr) = if let Tok::Ident(id) = self.peek().clone() {
                if matches!(self.peek_n(1), Tok::Colon) {
                    self.next();
                    self.next();
                    (intern(&id), self.parse_expr()?)
                } else {
                    let e = self.parse_expr()?;
                    let inferred = infer_col_name(&e).unwrap_or_else(|| unnamed(&cols));
                    (inferred, e)
                }
            } else {
                let e = self.parse_expr()?;
                let inferred = infer_col_name(&e).unwrap_or_else(|| unnamed(&cols));
                (inferred, e)
            };
            cols.push((name, expr));
            self.skip_newlines();
            if matches!(self.peek(), Tok::Semi) {
                self.next();
            } else if *self.peek() == end_tok {
                self.next();
                break;
            } else {
                return Err(KError::syntax(format!("expected ';' or closing delimiter in table literal, got {:?}", self.peek())));
            }
        }
        Ok(cols)
    }

    fn parse_cond(&mut self) -> KResult<Expr> {
        self.next(); // `$[`
        let saved_comma = self.stop_on_comma;
        let saved_qsql = self.in_qsql;
        self.stop_on_comma = false;
        self.in_qsql = false;

        let mut parts = Vec::new();
        loop {
            self.skip_newlines();
            parts.push(self.parse_expr()?);
            self.skip_newlines();
            match self.peek().clone() {
                Tok::Semi => {
                    self.next();
                }
                Tok::RBrack => {
                    self.next();
                    break;
                }
                other => {
                    self.stop_on_comma = saved_comma;
                    self.in_qsql = saved_qsql;
                    return Err(KError::syntax(format!("expected ';' or ']' in $[...], got {other:?}")));
                }
            }
        }
        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        if parts.len() < 3 {
            return Err(KError::syntax("$[cond; true; false] requires at least 3 arguments"));
        }
        Ok(Expr::Cond(parts))
    }

    fn parse_if_do_while(&mut self, kw: &str) -> KResult<Expr> {
        self.next(); // keyword
        self.next(); // `[`
        let saved_comma = self.stop_on_comma;
        let saved_qsql = self.in_qsql;
        self.stop_on_comma = false;
        self.in_qsql = false;

        self.skip_newlines();
        let head = self.parse_expr()?;
        let mut body = Vec::new();
        while matches!(self.peek(), Tok::Semi) {
            self.next();
            self.skip_newlines();
            if matches!(self.peek(), Tok::RBrack) {
                break;
            }
            body.push(self.parse_expr()?);
            self.skip_newlines();
        }
        if !matches!(self.next(), Tok::RBrack) {
            self.stop_on_comma = saved_comma;
            self.in_qsql = saved_qsql;
            return Err(KError::syntax(format!("expected ']' closing {kw}[...]")));
        }
        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        Ok(match kw {
            "if" => Expr::If { cond: Box::new(head), body },
            "do" => Expr::Do { count: Box::new(head), body },
            _ => Expr::While { cond: Box::new(head), body },
        })
    }

    fn parse_qsql(&mut self) -> KResult<Expr> {
        let Tok::Ident(kw) = self.next() else { unreachable!() };
        let saved_comma = self.stop_on_comma;
        let saved_qsql = self.in_qsql;

        let res = (|| -> KResult<Expr> {
            let mut limit = None;
            if matches!(self.peek(), Tok::LBrack) {
                self.stop_on_comma = false;
                self.in_qsql = false;
                let args = self.parse_bracket_args()?;
                if let Some(Some(lim)) = args.into_iter().next() {
                    limit = Some(Box::new(lim));
                }
            }

            let mut distinct = false;
            if kw == "select" {
                if let Tok::Ident(s) = self.peek() {
                    if s == "distinct" {
                        distinct = true;
                        self.next();
                    }
                }
            }

            self.in_qsql = true;
            self.stop_on_comma = true;

            // Parse column specifications up to `by` or `from`
            let mut cols = Vec::new();
            if !matches!(self.peek(), Tok::Ident(s) if s == "by" || s == "from") {
                cols = self.parse_qsql_col_list()?;
            }

            // Optional `by` clause
            let by = if matches!(self.peek(), Tok::Ident(s) if s == "by") {
                self.next();
                Some(self.parse_qsql_col_list()?)
            } else {
                None
            };

            // Mandatory `from`
            if !matches!(self.peek(), Tok::Ident(s) if s == "from") {
                return Err(KError::syntax(format!("expected 'from' in {kw} query")));
            }
            self.next();

            // `from` target expression (do not treat `,` as separator unless `where` follows; keep `in_qsql = true` so `where` stops it!)
            self.stop_on_comma = false;
            let from = self.parse_expr()?;

            // Optional `where` clause (comma-separated sequential predicates!)
            let mut where_clauses = Vec::new();
            if matches!(self.peek(), Tok::Ident(s) if s == "where") {
                self.next();
                self.stop_on_comma = true;
                loop {
                    where_clauses.push(self.parse_expr()?);
                    if matches!(self.peek(), Tok::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
            }

            let kind = match kw.as_str() {
                "select" => QSqlKind::Select,
                "exec" => QSqlKind::Exec,
                "update" => QSqlKind::Update,
                "delete" => {
                    if !cols.is_empty() && where_clauses.is_empty() && by.is_none() {
                        QSqlKind::DeleteCols
                    } else {
                        QSqlKind::DeleteRows
                    }
                }
                _ => unreachable!(),
            };

            Ok(Expr::Query(Box::new(QSql {
                kind,
                distinct,
                limit,
                cols,
                by,
                from: Box::new(from),
                where_clauses,
            })))
        })();

        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        res
    }

    fn parse_qsql_col_list(&mut self) -> KResult<Vec<ColSpec>> {
        let mut out = Vec::new();
        loop {
            if matches!(self.peek(), Tok::Ident(s) if s == "by" || s == "from" || s == "where") || self.is_expr_end_ignoring_comma() {
                break;
            }
            let spec = if let Tok::Ident(id) = self.peek().clone() {
                if matches!(self.peek_n(1), Tok::Colon) {
                    self.next();
                    self.next();
                    ColSpec { name: Some(intern(&id)), expr: self.parse_expr()? }
                } else {
                    ColSpec { name: None, expr: self.parse_expr()? }
                }
            } else {
                ColSpec { name: None, expr: self.parse_expr()? }
            };
            out.push(spec);
            if matches!(self.peek(), Tok::Comma) {
                self.next();
            } else {
                break;
            }
        }
        Ok(out)
    }

    fn is_expr_end_ignoring_comma(&self) -> bool {
        matches!(self.peek(), Tok::Eof | Tok::Semi | Tok::Newline | Tok::RParen | Tok::RBrack)
    }
}

/// Infer a default column name from a `qSQL` expression (`price` -> `price`, `sum price` -> `price`, `5 xbar time.minute` -> `minute`).
pub fn infer_col_name(expr: &Expr) -> Option<SymId> {
    if matches!(expr, Expr::Var(s) if resolve(*s) == "i") {
        return Some(intern("x"));
    }
    match expr {
        Expr::Var(s) => Some(*s),
        Expr::Field(_, f) => Some(*f),
        Expr::Unary { arg, .. } => infer_col_name(arg),
        Expr::Binary { op, lhs, rhs } => {
            let word = resolve(*op).starts_with(|c: char| c.is_ascii_alphabetic());
            infer_col_name(if word { rhs } else { lhs })
        }
        Expr::Juxt { arg, .. } => infer_col_name(arg),
        Expr::Apply { args, .. } => args.first().and_then(|a| a.as_ref()).and_then(infer_col_name),
        _ => None,
    }
}

/// The name of the next column without one.
fn unnamed(cols: &[(SymId, Expr)]) -> SymId {
    let names = (0..).map(|n| intern(&if n == 0 { "x".to_string() } else { format!("x{n}") }));
    names.into_iter().find(|name| cols.iter().all(|c| c.0 != *name)).unwrap_or_else(|| intern("x"))
}

fn parse_lambda(source: &str, toks: &[Tok]) -> KResult<Expr> {
    let mut p = Parser::new(toks);
    p.skip_newlines();

    let (params, implicit) = if matches!(p.peek(), Tok::LBrack) {
        p.next();
        p.skip_newlines();
        let mut ps = Vec::new();
        if matches!(p.peek(), Tok::RBrack) {
            p.next();
        } else {
            loop {
                p.skip_newlines();
                match p.next() {
                    Tok::Ident(name) => ps.push(intern(&name)),
                    other => return Err(KError::syntax(format!("expected parameter name in lambda, got {other:?}"))),
                }
                p.skip_newlines();
                match p.next() {
                    Tok::Semi => continue,
                    Tok::RBrack => break,
                    other => return Err(KError::syntax(format!("expected ';' or ']' in lambda parameter list, got {other:?}"))),
                }
            }
        }
        (ps, false)
    } else {
        // Determine implicit parameters `x`, `y`, `z` referenced inside the body (matching kdb+ arity rules!)
        let mut max_imp = 0usize;
        for t in toks {
            if let Tok::Ident(s) = t {
                match s.as_str() {
                    "x" => max_imp = max_imp.max(1),
                    "y" => max_imp = max_imp.max(2),
                    "z" => max_imp = max_imp.max(3),
                    _ => {}
                }
            }
        }
        let imp = match max_imp {
            0 | 1 => vec![intern("x")],
            2 => vec![intern("x"), intern("y")],
            _ => vec![intern("x"), intern("y"), intern("z")],
        };
        (imp, true)
    };

    let body = p.parse_seq(true)?;
    let ends_open = toks.iter().rev().find(|t| !matches!(t, Tok::Newline | Tok::Eof)) == Some(&Tok::Semi);
    let body = if ends_open { [body, vec![Expr::Lit(K::Null)]].concat() } else { body };
    Ok(Expr::Lambda {
        params,
        implicit,
        body,
        source: source.to_string(),
    })
}
