# Language

Expressions evaluate right to left with no operator precedence: `2 * 3 + 4` is `2 * (3 + 4)`.

## Values

Atoms and typed vectors of boolean, guid, byte, short, int, long, real, float, char, symbol, timestamp, month, date,
datetime, timespan, minute, second and time, with typed nulls and infinities (`0N`, `0W`, `0n`, `0w`); general lists;
dictionaries; tables and keyed tables; functions. Vectors carry the attributes `` `s# `` `` `u# `` `` `p# `` `` `g# ``.
Integer arithmetic and aggregates wrap on overflow.

* Arithmetic on booleans, bytes, shorts and ints gives ints; `&` and `|` give the wider of their two types. `div` rounds
  down and `mod` takes the divisor's sign. Floats compare as equal within a relative difference of 2^-43.
* Month, date, datetime and timestamp compare in the finer of two types, as do minute, second, time and timespan. A list
  such as `12:34 01:02:03` takes the finer type, or the type a letter after its last item names (`u v t n`). A date plus a
  timespan is a timestamp.
* A symbol compares only with a symbol (`'type`). Writing a value of another type into a simple list, or adding one with
  `,:`, is `'type`; a general list whose items come to share a type becomes simple.
* Verbs applied to two dictionaries work key by key over the keys of both; a value only one has is kept, and compared
  with a null by comparisons. `d _ key` and `keys _ d` drop entries.
* Casting a float to a whole number rounds it. A type can be given by number (`7h$x`), and `"ijf"$x` casts item by item.
* A table is also a list of its rows: `t _ i`, `t ? row`, `inter`, `union`, `except`, `^` and `,'` work row by row or
  column by column, a keyed table is looked up, taken from and dropped from by key, and a list of dictionaries with the
  same keys is a table. `,` of two tables needs the same columns in the same order; `t,:row` keeps the column types.
* `xasc`, `xdesc`, `xkey`, `xcol` and `xcols` given a table's name change that table. `key` of a name lists a namespace,
  and `` get `.name `` gives the namespace as a dictionary.
* `0x0 vs n` and `0b vs n` give a number's bytes or bits and `sv` puts them together; with a base, or a list of bases,
  they work in digits (`24 60 60 vs 3723`).

## Syntax

* assignment `:`, global `::`, amend `x[i]:v`, `x+:1`, `@[d;i;f;y]`, deep amend `.[d;path;f;y]`
* lambdas `{[a;b] ...}` (implicit `x y z`), projections `f[1;]`, early return `:value`
* adverbs `'` `/` `\` `':` `/:` `\:`, and the words `each`, `over`, `scan`, `prior`
* `$[c;t;f]`, `if[...]`, `do[...]`, `while[...]`
* signal `'err`, protected evaluation `@[f;x;handler]` and `.[f;args;handler]`
* views `v::expression`, which read global names only (those of the view's namespace first), never a query's columns or
  a function's variables; `.z.b` lists the views that read each name, and a view that depends on itself is `'loop`
* namespaces (`\d .name`)

## Verbs

| Group | Verbs |
| --- | --- |
| arithmetic | `+ - * % div mod xexp xlog neg abs sqrt exp log sin cos tan asin acos atan floor ceiling reciprocal signum` |
| comparison, logic | `= <> < <= > >= ~ & \| not and or all any null within in like` |
| lists | `til count first last # _ sublist enlist raze flip reverse rotate distinct where group iasc idesc asc desc rank xrank xprev cut cross inter union except bin binr` |
| aggregates | `sum prd avg wavg wsum min max med var dev svar sdev cov scov cor` |
| matrices | `mmu inv lsq` |
| running, moving | `sums prds avgs mins maxs deltas ratios differ prev next fills mavg msum mcount mmin mmax mdev ema xbar` |
| tables | `cols keys meta xasc xdesc xkey xcol xcols xgroup ungroup insert upsert fby tables views` |
| strings, casts | `string upper lower trim ltrim rtrim ss ssr vs sv $ md5` |
| system | `get set value parse eval attr system show getenv setenv ltime gtime hopen hclose hcount 0: 1: -8! -9!` |
| iterators | `each peach over scan prior` (`peach` runs as `each`) |

`.z.p .z.P .z.d .z.D .z.t .z.T .z.u .z.i .z.o .z.K` give the time, user and process. Date and time parts are read with
`` `year$x `` or `x.year`, `.mm`, `.dd`, `.hh`, `.uu`, `.ss`, `.minute`, `.date` and so on.

## qSQL

```q
select [distinct] cols by groups from t where c1, c2     / also select[n] and select[-n]
exec cols by groups from t where c
update cols by groups from t where c                    / from `t changes t in place
delete from t where c                                    / delete c1, c2 from t
```

A column without a name takes the first name in its expression (the one after a verb such as `xbar`), otherwise `x`.
An `update` of some rows takes a value for each of them, of the column's type.

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
