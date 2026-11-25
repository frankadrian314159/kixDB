//! Abstract syntax tree for `q` and `qSQL`.
use crate::sym::SymId;
use crate::types::{AdverbKind, K};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColSpec {
    /// Explicit alias (`vwap: size wavg price`), or `None` when inferred (`sum price` -> `price`).
    pub name: Option<SymId>,
    pub expr: Expr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QSqlKind {
    Select,
    Exec,
    Update,
    DeleteRows,
    DeleteCols,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QSql {
    pub kind: QSqlKind,
    pub distinct: bool,
    pub limit: Option<Box<Expr>>,
    pub cols: Vec<ColSpec>,
    /// `None` when no `by` clause was written; `Some(vec![])` when `by` was written (`select ... by ...`).
    pub by: Option<Vec<ColSpec>>,
    pub from: Box<Expr>,
    /// If `from` was `` `table `` (in-place update/delete target) vs value expression `table`.
    pub where_clauses: Vec<Expr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Literal `K` value (atom or vector literal).
    Lit(K),
    /// General list literal `(e1; e2; ...)`. `()` is `Lit(K::Vector(ColVec::General([])))`.
    List(Vec<Expr>),
    /// Table literal `([k1: ...; ...] v1: ...; ...)`.
    TableLit {
        keys: Vec<(SymId, Expr)>,
        vals: Vec<(SymId, Expr)>,
    },
    /// Variable or built-in primitive name (`price`, `sum`, `.z.p`).
    Var(SymId),
    /// Field / temporal component / dict-or-table dot access (`time.minute`, `t.price`).
    Field(Box<Expr>, SymId),
    /// Unary application (`neg x`, `sum 1 2 3`, `-x`).
    Unary { op: SymId, arg: Box<Expr> },
    /// Binary infix application (`x + y`, `5 xbar t`, `t lj kt`), evaluated right-to-left: `rhs` before `lhs`!
    Binary { op: SymId, lhs: Box<Expr>, rhs: Box<Expr> },
    /// Function / indexing / bracket application `target[a0; a1; ...]`, where `None` is an omitted projection slot `f[1;]`.
    Apply { target: Box<Expr>, args: Vec<Option<Expr>> },
    /// Juxtaposition application `f x` where `f` is an arbitrary expression (`{x+1} 5`, `(+/) 1 2 3`).
    Juxt { func: Box<Expr>, arg: Box<Expr> },
    /// Adverb derived expression (`f'`, `f/`, `f\`, `f':`, `f/:`, `f\:`).
    Adverb { kind: AdverbKind, func: Box<Expr> },
    /// Assignment: `name : expr` (`global = false`) or `name :: expr` (`global = true`).
    Assign {
        name: SymId,
        global: bool,
        op: Option<SymId>,
        val: Box<Expr>,
    },
    /// Indexed assignment `name[idx0; ...] : val` or `name[idx] +: val`.
    IndexAssign {
        name: SymId,
        indices: Vec<Option<Expr>>,
        op: Option<SymId>,
        val: Box<Expr>,
    },
    /// Lambda `{[p1; p2] body}`.
    Lambda {
        params: Vec<SymId>,
        implicit: bool,
        body: Vec<Expr>,
        source: String,
    },
    /// Conditional `$[cond; true_expr; cond2; true2; ...; else_expr]`.
    Cond(Vec<Expr>),
    /// `if[cond; e1; e2; ...]`
    If { cond: Box<Expr>, body: Vec<Expr> },
    /// `do[count; e1; e2; ...]`
    Do { count: Box<Expr>, body: Vec<Expr> },
    /// `while[cond; e1; e2; ...]`
    While { cond: Box<Expr>, body: Vec<Expr> },
    /// Explicit early return `:expr` inside a lambda.
    Return(Box<Expr>),
    /// Signal `'err_expr`.
    Signal(Box<Expr>),
    /// Protected evaluation `@[f; x; handler]` (3-arg) or functional amend `@[d; i; f]` / `@[d; i; f; y]` (handled in `@` or `.`).
    /// `qSQL` query (`select`, `exec`, `update`, `delete`).
    Query(Box<QSql>),
    /// System command (`\t expr`, `\l dir`, `\a`, `\d .ns`, ...).
    SysCmd { cmd: String, arg: String },
    /// Empty / no-op expression.
    Nop,
}
