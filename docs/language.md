# Language

Expressions evaluate right to left with no operator precedence: `2 * 3 + 4` is `2 * (3 + 4)`.

## Values

Atoms and typed vectors of boolean, guid, byte, short, int, long, real, float, char, symbol, timestamp, month, date,
datetime, timespan, minute, second and time, with typed nulls and infinities (`0N`, `0W`, `0n`, `0w`); general lists;
dictionaries; tables and keyed tables; functions. Vectors carry the attributes `` `s# `` `` `u# `` `` `p# `` `` `g# ``.
Integer arithmetic and aggregates wrap on overflow.

## Syntax

* assignment `:`, global `::`, amend `x[i]:v`, `x+:1`, `@[d;i;f;y]`, deep amend `.[d;path;f;y]`
* lambdas `{[a;b] ...}` (implicit `x y z`), projections `f[1;]`, early return `:value`
* adverbs `'` `/` `\` `':` `/:` `\:`, and the words `each`, `over`, `scan`, `prior`
* `$[c;t;f]`, `if[...]`, `do[...]`, `while[...]`
* signal `'err`, protected evaluation `@[f;x;handler]` and `.[f;args;handler]`
* views `v::expression`, namespaces (`\d .name`)

## Verbs

| Group | Verbs |
| --- | --- |
| arithmetic | `+ - * % div mod xexp xlog neg abs sqrt exp log sin cos tan asin acos atan floor ceiling reciprocal signum` |
| comparison, logic | `= <> < <= > >= ~ & \| not and or null within in like` |
| lists | `til count first last # _ sublist enlist raze flip reverse distinct where group iasc idesc asc desc cut cross inter union except bin binr` |
| aggregates | `sum prd avg wavg wsum min max med var dev svar sdev cov cor` |
| running, moving | `sums prds avgs mins maxs deltas ratios differ prev next fills mavg msum mcount mmin mmax mdev ema xbar` |
| tables | `cols keys meta xasc xdesc xkey xcol xcols xgroup ungroup insert upsert fby tables views` |
| strings, casts | `string upper lower trim ltrim rtrim ss ssr vs sv $ md5` |
| system | `get set value parse eval system show hopen hclose hcount 0: 1: -8! -9!` |

`.z.p .z.P .z.d .z.D .z.t .z.T .z.u .z.i .z.o .z.K` give the time, user and process. Date and time parts are read with
`` `year$x `` or `x.year`, `.mm`, `.dd`, `.hh`, `.uu`, `.ss`, `.minute`, `.date` and so on.

## qSQL

```q
select [distinct] cols by groups from t where c1, c2     / also select[n] and select[-n]
exec cols by groups from t where c
update cols by groups from t where c                    / from `t changes t in place
delete from t where c                                    / delete c1, c2 from t
```

`where` clauses filter one after another: each sees only the rows the previous ones kept. `i` is a row's number in the
whole table, counted from 0, wherever it is used: also in a later `where` clause, after a filter and within a group.
Grouped results are keyed and sorted by their groups. Functional forms `?[t;c;b;a]` and `![t;c;b;a]` are supported.

## Joins

`lj`, `ij`, `uj`, `pj` (infix), `ej[cols;a;b]`, as-of `aj` and `aj0`, `asof`, and window joins `wj` and `wj1`.

## Parse trees

`parse "text"` gives the tree of q source and `eval tree` runs it. A tree is a list `(verb; arg; ...)` of trees:

```q
q)parse "a+b*2"                         / (+;`a;(*;`b;2))
q)eval (sum; 1 2 3)                     / 6
q)p: parse "exec sum v from t where s=`a"
q)eval p
```

* A symbol names a variable. A symbol constant is enlisted (`` ,`a ``), as is a constant symbol vector or list.
* qSQL statements parse to the functional forms `?[t;c;b;a]` and `![t;c;b;a]`. In a tree, `b` and `a` are taken as they
  are (they are trees themselves); `t` and `c` are evaluated.
* An operator applied to one argument carries a colon: `` (-:;`x) `` negates, `` (-;`x) `` is a projection.
* Several statements parse to a list headed by the verb `;`, which `eval` runs in order.

Trees follow q's shape for the common cases but are not guaranteed to match kdb+ item for item: a table literal
parses to a `.kix.table` form, and `'` applied to a named function appears as `each`.

## System commands

`\a` tables, `\b` views, `\v` variables, `\f` functions, `\d` namespace, `\l` load a script or directory, `\t` and `\ts`
(with `:n` repetitions) timing, `\w` workspace statistics.

## Not implemented

* `ej` only in bracket form.
* Foreign keys and linked columns, `.Q` and `.z` callbacks (`.z.pg`, `.z.ps`, timers), log replay (`-11!`), and the rest
  of the `.Q` namespace beyond `.Q.en`, `.Q.dpft` and `.Q.s`.
