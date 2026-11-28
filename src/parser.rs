//! Single-pass recursive-descent parser for `q` and `qSQL`, pulling tokens directly from [`Lexer`].
//!
//! `q` has **no operator precedence hierarchy**: binary operators and keyword verbs all associate strictly
//! **right-to-left** (`2 * 3 + 4` parses as `2 * (3 + 4)`), while unary verbs consume the entire expression to their right
//! (`sum 1 + til 5` parses as `sum (1 + (til 5))`). Postfix indexing (`x[i;j]`) and adverbs (`+/`, `f'`) bind tightly to
//! the term on their left.
use crate::ast::{ColSpec, Expr, QSql, QSqlKind};
use crate::error::{KError, KResult};
use crate::lexer::{Lexer, Tok};
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
    let mut p = Parser::new(src)?;
    let (stmts, _) = p.parse_seq(false)?;
    if !matches!(p.peek(), Tok::Eof) {
        return Err(KError::syntax(format!("unexpected token {:?}", p.peek())));
    }
    Ok(stmts)
}

struct Parser<'a> {
    src: &'a str,
    lex: Lexer<'a>,
    look: [Tok<'a>; 3],
    /// When true, `,` separates qSQL clause items rather than acting as the binary concat operator!
    stop_on_comma: bool,
    /// When true, top-level keywords `by`, `from`, `where` terminate an expression inside a qSQL statement.
    in_qsql: bool,
    /// Whether the expression just parsed ends in a verb that waits for its argument, as `x+` and `f value@` do.
    open_verb: bool,
    /// Bitmask (`1 = x`, `2 = y`, `4 = z`) of implicit parameters referenced inside the current lambda scope.
    implicit_xyz: u8,
}

type Checkpoint<'a> = (Lexer<'a>, [Tok<'a>; 3], bool, bool, u8);

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> KResult<Self> {
        let mut lex = Lexer::new(src);
        let look = [lex.next_tok()?, lex.next_tok()?, lex.next_tok()?];
        Ok(Self {
            src,
            lex,
            look,
            stop_on_comma: false,
            in_qsql: false,
            open_verb: false,
            implicit_xyz: 0,
        })
    }

    #[inline]
    fn peek(&self) -> &Tok<'a> {
        &self.look[0]
    }

    #[inline]
    fn peek_n(&self, n: usize) -> &Tok<'a> {
        &self.look[n]
    }

    #[inline]
    fn next(&mut self) -> KResult<Tok<'a>> {
        let next3 = self.lex.next_tok()?;
        let [t0, t1, t2] = std::mem::replace(&mut self.look, [Tok::Eof, Tok::Eof, Tok::Eof]);
        self.look = [t1, t2, next3];
        if let Tok::Ident(s) = t0 {
            match s {
                "x" => self.implicit_xyz |= 1,
                "y" => self.implicit_xyz |= 2,
                "z" => self.implicit_xyz |= 4,
                _ => {}
            }
        }
        Ok(t0)
    }

    fn checkpoint(&self) -> Checkpoint<'a> {
        (self.lex, self.look.clone(), self.stop_on_comma, self.in_qsql, self.implicit_xyz)
    }

    fn restore(&mut self, cp: Checkpoint<'a>) {
        (self.lex, self.look, self.stop_on_comma, self.in_qsql, self.implicit_xyz) = cp;
    }

    fn skip_newlines(&mut self) -> KResult<()> {
        while matches!(self.peek(), Tok::Newline) {
            self.next()?;
        }
        Ok(())
    }

    fn is_expr_end(&self) -> bool {
        match self.peek() {
            Tok::Eof | Tok::Semi | Tok::Newline | Tok::RParen | Tok::RBrack | Tok::RBrace(_) => true,
            Tok::Comma if self.stop_on_comma => true,
            Tok::Ident(s) if self.in_qsql && matches!(*s, "by" | "from" | "where") => true,
            _ => false,
        }
    }

    fn parse_seq(&mut self, inside_lambda: bool) -> KResult<(Vec<Expr>, bool)> {
        let mut stmts = Vec::new();
        let mut trailing_semi = false;
        loop {
            loop {
                match self.peek() {
                    Tok::Semi => {
                        trailing_semi = true;
                        self.next()?;
                    }
                    Tok::Newline => {
                        self.next()?;
                    }
                    _ => break,
                }
            }
            if inside_lambda {
                if matches!(self.peek(), Tok::RBrace(_) | Tok::Eof) {
                    break;
                }
            } else if matches!(self.peek(), Tok::Eof | Tok::RParen | Tok::RBrack) {
                break;
            }
            trailing_semi = false;
            if let Tok::SysCmd { cmd, arg } = *self.peek() {
                self.next()?;
                stmts.push(Expr::SysCmd {
                    cmd: cmd.to_string(),
                    arg: arg.to_string(),
                });
                continue;
            }
            stmts.push(self.parse_expr()?);
            // If a semicolon terminates the last statement at top-level, kdb+ suppresses printing (returns `::` Null)!
            if !inside_lambda && matches!(self.peek(), Tok::Semi) {
                while matches!(self.peek(), Tok::Semi) {
                    self.next()?;
                }
                if matches!(self.peek(), Tok::Eof | Tok::Newline) {
                    stmts.push(Expr::Lit(K::Null));
                }
            }
        }
        Ok((stmts, trailing_semi))
    }

    fn parse_expr(&mut self) -> KResult<Expr> {
        if self.is_expr_end() {
            return Ok(Expr::Lit(K::Null));
        }

        // Control structures: `if[...]`, `do[...]`, `while[...]`, `$[...]`, qSQL `select`/`exec`/`update`/`delete`
        if matches!(self.peek(), Tok::CondOpen) {
            return self.parse_cond();
        }
        if let Tok::Ident(kw) = *self.peek() {
            match kw {
                "if" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("if"),
                "do" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("do"),
                "while" if matches!(self.peek_n(1), Tok::LBrack) => return self.parse_if_do_while("while"),
                "select" | "exec" | "update" | "delete" => return self.parse_qsql(),
                _ => {}
            }
        }

        // `:expr` returns early from a lambda (also from inside `if[...]` and friends)
        if matches!(self.peek(), Tok::Colon) {
            self.next()?;
            return Ok(Expr::Return(Box::new(self.parse_expr()?)));
        }

        // Prefix signal `'err`
        if matches!(self.peek(), Tok::Quote) {
            self.next()?;
            let arg = self.parse_expr()?;
            return Ok(Expr::Signal(Box::new(arg)));
        }

        // Assignment forms
        if let Tok::Ident(name) = *self.peek() {
            if !is_builtin_verb(name) || matches!(self.peek_n(1), Tok::Colon | Tok::DoubleColon) {
                match *self.peek_n(1) {
                    Tok::Colon => {
                        self.next()?;
                        self.next()?;
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(name), global: false, op: None, val: Box::new(val) });
                    }
                    Tok::DoubleColon => {
                        self.next()?;
                        self.next()?;
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(name), global: true, op: None, val: Box::new(val) });
                    }
                    Tok::OpColon(op) => {
                        let op_sym = intern(op);
                        self.next()?;
                        self.next()?;
                        let global = matches!(self.peek(), Tok::Colon);
                        if global {
                            self.next()?;
                        }
                        let val = self.parse_expr()?;
                        return Ok(Expr::Assign { name: intern(name), global, op: Some(op_sym), val: Box::new(val) });
                    }
                    Tok::LBrack => {
                        if let Some((indices, assign_op)) = self.try_parse_indexed_assign()? {
                            let val = self.parse_expr()?;
                            return Ok(Expr::IndexAssign { name: intern(name), indices, op: assign_op, val: Box::new(val) });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Prefix unary operator
        if let Some(op_str) = self.peek_prefix_op() {
            let adverb_word = matches!(self.peek_n(1), Tok::Ident(s) if matches!(*s, "each" | "over" | "scan" | "prior"));
            if !adverb_word && !matches!(self.peek_n(1), Tok::Adverb(_) | Tok::LBrack) {
                self.next()?;
                let op_sym = intern(op_str);
                if self.is_expr_end() {
                    return Ok(Expr::Lit(K::Func(Func::Prim(op_sym))));
                }
                let rhs = self.parse_expr()?;
                if self.waits(&rhs) {
                    return Ok(Expr::Compose(Box::new(Expr::Lit(K::Func(Func::Prim(op_sym)))), Box::new(rhs)));
                }
                return Ok(Expr::Unary { op: op_sym, arg: Box::new(rhs) });
            }
        }

        // Parse left-hand term
        let lhs = self.parse_term()?;

        // Bare unary keyword (`sum`, `til`, `neg`, `not`, ...)
        if let Expr::Var(sym) = &lhs {
            let name = resolve(*sym);
            let called = matches!(self.peek(), Tok::Op(_)) && matches!(self.peek_n(1), Tok::LBrack);
            if is_unary_keyword(name) && !self.is_expr_end() && (called || self.peek_binary_op().is_none()) {
                let sym = *sym;
                let rhs = self.parse_expr()?;
                if self.waits(&rhs) {
                    return Ok(Expr::Compose(Box::new(lhs), Box::new(rhs)));
                }
                return Ok(Expr::Unary { op: sym, arg: Box::new(rhs) });
            }
        }

        // Adverb-modified infix verb/lambda (`10 +/: 1 2 3`, `0 +/ 1 2 3`, `a {x+y}' b`)
        if !self.is_expr_end()
            && (matches!(self.peek(), Tok::LBrace(_))
                || (matches!(self.peek(), Tok::Op(_) | Tok::Comma | Tok::Ident(_))
                    && matches!(self.peek_n(1), Tok::Adverb(_))))
        {
            let cp = self.checkpoint();
            let term = self.parse_term()?;
            if matches!(term, Expr::Adverb { .. }) {
                let rhs = self.parse_expr()?;
                return Ok(Expr::Apply {
                    target: Box::new(term),
                    args: vec![Some(lhs), Some(rhs)],
                });
            }
            self.restore(cp);
        }

        // Binary operator or juxtaposition
        if !self.is_expr_end() {
            if let Some(bin_op) = self.peek_binary_op() {
                self.next()?;
                let op_sym = intern(bin_op);
                let target = Box::new(Expr::Lit(K::Func(Func::Prim(op_sym))));
                if self.is_expr_end() {
                    self.open_verb = true;
                    return Ok(Expr::Apply { target, args: vec![Some(lhs), None] });
                }
                let rhs = self.parse_expr()?;
                if self.waits(&rhs) {
                    let outer = Expr::Apply { target, args: vec![Some(lhs), None] };
                    return Ok(Expr::Compose(Box::new(outer), Box::new(rhs)));
                }
                return Ok(Expr::Binary { op: op_sym, lhs: Box::new(lhs), rhs: Box::new(rhs) });
            }

            let rhs = self.parse_expr()?;
            if self.waits(&rhs) {
                return Ok(Expr::Compose(Box::new(lhs), Box::new(rhs)));
            }
            return Ok(Expr::Juxt { func: Box::new(lhs), arg: Box::new(rhs) });
        }

        Ok(lhs)
    }

    fn waits(&self, e: &Expr) -> bool {
        self.open_verb && matches!(e, Expr::Apply { .. } | Expr::Compose(..))
    }

    fn peek_prefix_op(&self) -> Option<&'a str> {
        match *self.peek() {
            Tok::Op(s) => Some(s),
            Tok::Comma => Some(","),
            _ => None,
        }
    }

    fn peek_binary_op(&self) -> Option<&'a str> {
        if self.is_expr_end() {
            return None;
        }
        match *self.peek() {
            Tok::Op(s) => Some(s),
            Tok::Comma if !self.stop_on_comma => Some(","),
            Tok::Ident(s) if is_binary_keyword(s) && !(matches!(self.peek_n(1), Tok::Adverb(_)) && matches!(self.peek_n(2), Tok::LBrack)) => Some(s),
            _ => None,
        }
    }

    fn try_parse_indexed_assign(&mut self) -> KResult<Option<(Vec<Option<Expr>>, Option<SymId>)>> {
        let saved = self.checkpoint();
        self.next()?; // consume ident
        let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
        let saved_qsql = std::mem::replace(&mut self.in_qsql, false);
        let args_res = self.parse_bracket_args();
        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        let Ok(indices) = args_res else {
            self.restore(saved);
            return Ok(None);
        };
        if matches!(self.peek(), Tok::LBrack) {
            let after_first = self.checkpoint();
            let mut extra = false;
            while matches!(self.peek(), Tok::LBrack) && self.parse_bracket_args().is_ok() {
                extra = true;
            }
            if extra && matches!(self.peek(), Tok::Colon | Tok::DoubleColon | Tok::OpColon(_)) {
                return Err(KError::new("assign", "assign through x[i;j], not x[i][j]"));
            }
            self.restore(after_first);
        }
        match *self.peek() {
            Tok::Colon | Tok::DoubleColon => {
                self.next()?;
                Ok(Some((indices, None)))
            }
            Tok::OpColon(op) => {
                let op_sym = intern(op);
                self.next()?;
                if matches!(self.peek(), Tok::Colon) {
                    self.next()?;
                }
                Ok(Some((indices, Some(op_sym))))
            }
            _ => {
                self.restore(saved);
                Ok(None)
            }
        }
    }

    fn parse_term(&mut self) -> KResult<Expr> {
        let mut base = self.parse_atom()?;

        loop {
            match *self.peek() {
                Tok::Adverb(kind) => {
                    self.next()?;
                    base = Expr::Adverb { kind, func: Box::new(base) };
                }
                Tok::LBrack => {
                    let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
                    let saved_qsql = std::mem::replace(&mut self.in_qsql, false);
                    let args_res = self.parse_bracket_args();
                    self.stop_on_comma = saved_comma;
                    self.in_qsql = saved_qsql;
                    let mut args = args_res?;
                    let operator = matches!(&base, Expr::Lit(K::Func(Func::Prim(s))) if !resolve(*s).starts_with(|c: char| c.is_ascii_alphanumeric()));
                    if operator && matches!(args.as_slice(), [Some(_)]) {
                        args.push(None);
                    }
                    base = Expr::Apply { target: Box::new(base), args };
                }
                Tok::Field if matches!(self.peek_n(1), Tok::Ident(_)) => {
                    self.next()?; // `.`
                    if let Tok::Ident(field) = self.next()? {
                        base = Expr::Field(Box::new(base), intern(field));
                    }
                }
                _ => break,
            }
        }

        self.open_verb = false;
        Ok(base)
    }

    fn parse_atom(&mut self) -> KResult<Expr> {
        match *self.peek() {
            Tok::DoubleColon => {
                self.next()?;
                Ok(Expr::Lit(K::Null))
            }
            Tok::Lit(_) => {
                let Tok::Lit(k) = self.next()? else { unreachable!() };
                Ok(Expr::Lit(k))
            }
            Tok::Ident(s) => {
                self.next()?;
                Ok(Expr::Var(intern(s)))
            }
            Tok::Op(op) => {
                self.next()?;
                Ok(Expr::Lit(K::Func(Func::Prim(intern(op)))))
            }
            Tok::Comma if !self.stop_on_comma => {
                self.next()?;
                Ok(Expr::Lit(K::Func(Func::Prim(intern(",")))))
            }
            Tok::LBrace(brace_start) => self.parse_lambda(brace_start),
            Tok::LParen => self.parse_paren_or_table(),
            Tok::LBrack => {
                let saved = (std::mem::replace(&mut self.stop_on_comma, false), std::mem::replace(&mut self.in_qsql, false));
                let args = self.parse_bracket_args();
                (self.stop_on_comma, self.in_qsql) = saved;
                let target = Box::new(Expr::Lit(K::Func(Func::Prim(intern(";")))));
                Ok(Expr::Apply { target, args: args? })
            }
            ref other => Err(KError::syntax(format!("unexpected token {other:?}"))),
        }
    }

    fn parse_lambda(&mut self, brace_start: usize) -> KResult<Expr> {
        self.next()?; // consume `{`
        let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
        let saved_qsql = std::mem::replace(&mut self.in_qsql, false);
        let saved_imp = std::mem::replace(&mut self.implicit_xyz, 0);

        let res = (|| -> KResult<Expr> {
            self.skip_newlines()?;
            let (mut params, implicit) = if matches!(self.peek(), Tok::LBrack) {
                self.next()?;
                self.skip_newlines()?;
                let mut ps = Vec::new();
                if matches!(self.peek(), Tok::RBrack) {
                    self.next()?;
                } else {
                    loop {
                        self.skip_newlines()?;
                        match self.next()? {
                            Tok::Ident(name) => ps.push(intern(name)),
                            other => return Err(KError::syntax(format!("expected parameter name in lambda, got {other:?}"))),
                        }
                        self.skip_newlines()?;
                        match self.next()? {
                            Tok::Semi => continue,
                            Tok::RBrack => break,
                            other => return Err(KError::syntax(format!("expected ';' or ']' in lambda parameter list, got {other:?}"))),
                        }
                    }
                }
                (ps, false)
            } else {
                self.implicit_xyz = 0;
                (Vec::new(), true)
            };

            let (mut body, trailing_semi) = self.parse_seq(true)?;
            let Tok::RBrace(brace_end) = self.next()? else {
                return Err(KError::syntax("unclosed '{' in lambda"));
            };
            if trailing_semi {
                body.push(Expr::Lit(K::Null));
            }
            if implicit {
                params = if self.implicit_xyz & 4 != 0 {
                    vec![intern("x"), intern("y"), intern("z")]
                } else if self.implicit_xyz & 2 != 0 {
                    vec![intern("x"), intern("y")]
                } else {
                    vec![intern("x")]
                };
            }
            Ok(Expr::Lambda {
                params,
                implicit,
                body,
                source: self.src[brace_start..=brace_end].to_string(),
            })
        })();

        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        self.implicit_xyz = saved_imp;
        res
    }

    fn parse_bracket_args(&mut self) -> KResult<Vec<Option<Expr>>> {
        if !matches!(self.next()?, Tok::LBrack) {
            return Err(KError::syntax("expected '['"));
        }
        self.skip_newlines()?;
        if matches!(self.peek(), Tok::RBrack) {
            self.next()?;
            return Ok(vec![None]);
        }
        let mut args = Vec::new();
        loop {
            self.skip_newlines()?;
            if matches!(self.peek(), Tok::Semi | Tok::RBrack) {
                args.push(None);
            } else if matches!(self.peek(), Tok::Colon) && matches!(self.peek_n(1), Tok::Semi | Tok::RBrack) {
                self.next()?;
                args.push(Some(Expr::Lit(K::Func(Func::Prim(intern(":"))))));
            } else {
                args.push(Some(self.parse_expr()?));
            }
            self.skip_newlines()?;
            match self.peek() {
                Tok::Semi => {
                    self.next()?;
                }
                Tok::RBrack => {
                    self.next()?;
                    break;
                }
                other => return Err(KError::syntax(format!("expected ';' or ']', got {other:?}"))),
            }
        }
        Ok(args)
    }

    fn parse_paren_or_table(&mut self) -> KResult<Expr> {
        self.next()?; // consume `(`
        let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
        let saved_qsql = std::mem::replace(&mut self.in_qsql, false);

        let res = (|| -> KResult<Expr> {
            self.skip_newlines()?;
            if matches!(self.peek(), Tok::RParen) {
                self.next()?;
                return Ok(Expr::Lit(K::Vector(ColVec::General(Attr::None, Arc::new(vec![])))));
            }
            if matches!(self.peek(), Tok::DoubleColon) && matches!(self.peek_n(1), Tok::RParen) {
                self.next()?;
                self.next()?;
                return Ok(Expr::Lit(K::Null));
            }
            if matches!(self.peek(), Tok::LBrack) {
                return self.parse_table_lit();
            }

            let mut items = Vec::new();
            let mut saw_semi = false;
            loop {
                self.skip_newlines()?;
                if matches!(self.peek(), Tok::Semi | Tok::RParen) {
                    items.push(Expr::Lit(K::Null));
                } else {
                    items.push(self.parse_expr()?);
                }
                self.skip_newlines()?;
                match self.peek() {
                    Tok::Semi => {
                        saw_semi = true;
                        self.next()?;
                    }
                    Tok::RParen => {
                        self.next()?;
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
        self.next()?; // `[`
        let keys = self.parse_table_col_defs(Tok::RBrack)?;
        self.skip_newlines()?;
        if matches!(self.peek(), Tok::Semi) {
            self.next()?;
        }
        let vals = self.parse_table_col_defs(Tok::RParen)?;
        Ok(Expr::TableLit { keys, vals })
    }

    fn parse_table_col_defs(&mut self, end_tok: Tok<'static>) -> KResult<Vec<(SymId, Expr)>> {
        let mut cols = Vec::new();
        self.skip_newlines()?;
        if *self.peek() == end_tok {
            self.next()?;
            return Ok(cols);
        }
        loop {
            self.skip_newlines()?;
            if *self.peek() == end_tok {
                self.next()?;
                break;
            }
            let (name, expr) = if let Tok::Ident(id) = *self.peek() {
                if matches!(self.peek_n(1), Tok::Colon) {
                    self.next()?;
                    self.next()?;
                    (intern(id), self.parse_expr()?)
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
            self.skip_newlines()?;
            if matches!(self.peek(), Tok::Semi) {
                self.next()?;
            } else if *self.peek() == end_tok {
                self.next()?;
                break;
            } else {
                return Err(KError::syntax(format!("expected ';' or closing delimiter in table literal, got {:?}", self.peek())));
            }
        }
        Ok(cols)
    }

    fn parse_cond(&mut self) -> KResult<Expr> {
        self.next()?; // `$[`
        let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
        let saved_qsql = std::mem::replace(&mut self.in_qsql, false);

        let res = (|| -> KResult<Expr> {
            let mut parts = Vec::new();
            loop {
                self.skip_newlines()?;
                parts.push(self.parse_expr()?);
                self.skip_newlines()?;
                match self.peek() {
                    Tok::Semi => {
                        self.next()?;
                    }
                    Tok::RBrack => {
                        self.next()?;
                        break;
                    }
                    other => return Err(KError::syntax(format!("expected ';' or ']' in $[...], got {other:?}"))),
                }
            }
            if parts.len() < 3 {
                return Err(KError::syntax("$[cond; true; false] requires at least 3 arguments"));
            }
            Ok(Expr::Cond(parts))
        })();

        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        res
    }

    fn parse_if_do_while(&mut self, kw: &str) -> KResult<Expr> {
        self.next()?; // keyword
        self.next()?; // `[`
        let saved_comma = std::mem::replace(&mut self.stop_on_comma, false);
        let saved_qsql = std::mem::replace(&mut self.in_qsql, false);

        let res = (|| -> KResult<Expr> {
            self.skip_newlines()?;
            let head = self.parse_expr()?;
            let mut body = Vec::new();
            while matches!(self.peek(), Tok::Semi) {
                self.next()?;
                self.skip_newlines()?;
                if matches!(self.peek(), Tok::RBrack) {
                    break;
                }
                body.push(self.parse_expr()?);
                self.skip_newlines()?;
            }
            if !matches!(self.next()?, Tok::RBrack) {
                return Err(KError::syntax(format!("expected ']' closing {kw}[...]")));
            }
            Ok(match kw {
                "if" => Expr::If { cond: Box::new(head), body },
                "do" => Expr::Do { count: Box::new(head), body },
                _ => Expr::While { cond: Box::new(head), body },
            })
        })();

        self.stop_on_comma = saved_comma;
        self.in_qsql = saved_qsql;
        res
    }

    fn parse_qsql(&mut self) -> KResult<Expr> {
        let Tok::Ident(kw) = self.next()? else { unreachable!() };
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
            if kw == "select" && matches!(self.peek(), Tok::Ident("distinct")) {
                distinct = true;
                self.next()?;
            }

            self.in_qsql = true;
            self.stop_on_comma = true;

            let mut cols = Vec::new();
            if !matches!(self.peek(), Tok::Ident("by" | "from")) {
                cols = self.parse_qsql_col_list()?;
            }

            let by = if matches!(self.peek(), Tok::Ident("by")) {
                self.next()?;
                Some(self.parse_qsql_col_list()?)
            } else {
                None
            };

            if !matches!(self.peek(), Tok::Ident("from")) {
                return Err(KError::syntax(format!("expected 'from' in {kw} query")));
            }
            self.next()?;

            self.stop_on_comma = false;
            let from = self.parse_expr()?;

            let mut where_clauses = Vec::new();
            if matches!(self.peek(), Tok::Ident("where")) {
                self.next()?;
                self.stop_on_comma = true;
                loop {
                    where_clauses.push(self.parse_expr()?);
                    if matches!(self.peek(), Tok::Comma) {
                        self.next()?;
                    } else {
                        break;
                    }
                }
            }

            let kind = match kw {
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
            if matches!(self.peek(), Tok::Ident("by" | "from" | "where")) || self.is_expr_end_ignoring_comma() {
                break;
            }
            let spec = if let Tok::Ident(id) = *self.peek() {
                if matches!(self.peek_n(1), Tok::Colon) {
                    self.next()?;
                    self.next()?;
                    ColSpec { name: Some(intern(id)), expr: self.parse_expr()? }
                } else {
                    ColSpec { name: None, expr: self.parse_expr()? }
                }
            } else {
                ColSpec { name: None, expr: self.parse_expr()? }
            };
            out.push(spec);
            if matches!(self.peek(), Tok::Comma) {
                self.next()?;
            } else {
                break;
            }
        }
        Ok(out)
    }

    fn is_expr_end_ignoring_comma(&self) -> bool {
        matches!(self.peek(), Tok::Eof | Tok::Semi | Tok::Newline | Tok::RParen | Tok::RBrack | Tok::RBrace(_))
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
