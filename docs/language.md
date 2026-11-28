# Language

Expressions evaluate right to left with no operator precedence: `2 * 3 + 4` is `2 * (3 + 4)`.

## Values

Atoms and typed vectors of boolean, guid, byte, short, int, long, real, float, char, symbol, timestamp, month, date,
datetime, timespan, minute, second and time, with typed nulls and infinities (`0N`, `0W`, `0n`, `0w`, and the same
followed by a type's letter: `0Wh`, `0Np`, `-0Wz`); general lists;
enumerations ([below](#enumerations-foreign-keys-and-linked-columns)); dictionaries; tables and keyed tables; functions. Vectors carry the attributes `` `s# `` `` `u# `` `` `p# `` `` `g# ``.
Integer arithmetic and aggregates wrap on overflow.

* Arithmetic on booleans, bytes, shorts and ints gives ints; `&` and `|` give the wider of their two types. `div` rounds
  down and `mod` takes the divisor's sign. Floats compare as equal within a relative difference of 2^-43.
* Month, date, datetime and timestamp compare in the finer of two types, as do minute, second, time and timespan. A list
  such as `12:34 01:02:03` takes the finer type, or the type a letter after its last item names (`u v t n`). A date plus a
  timespan is a timestamp.
* A temporal null or infinity cast or converted to another temporal type is that type's null or infinity, and a date too
  far off for a timestamp is an infinite one. Casting a number's infinity keeps the number (`` `int$0Wh `` is `32767i`).
* `"P"$`, `"Z"$` and `"N"$` read the forms q writes, a date with `-` or without separators (`20191122-11:11`), and for
  a timespan digits alone (`hhmmss` and a fraction).
* In a timestamp, timespan or datetime an hour may stand alone: `2026.01.01D12`, `0D0`, `2026.01.01T9`.
* A symbol compares only with a symbol (`'type`). Writing a value of another type into a simple list, or adding one with
  `,:`, is `'type`; a general list whose items come to share a type becomes simple. One exception: longs that fit are
  taken into an `int` column of a table by `insert`, `upsert` and `t,:row` (`update` still wants ints).
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

## Enumerations, foreign keys and linked columns

```q
sym: `g`aapl`msft
ev: `sym$`msft`g                     / only values that the list sym has, else 'cast
`sym?`ibm                            / adds ibm to sym, then enumerates it
kt: ([eid: 1001 1002] name: `Dent`Prefect)
t: ([] eid: `kt$1002 1001; sc: 36 92)    / a foreign key: values of kt's key
select eid.name, sc from t           / follows the key into kt
tree: ([] pid: `tree!0N 0 0; v: 1 2 3)   / a linked column: row numbers of a table
exec pid.v from tree
```

An enumeration holds its values, tagged with the name of the list or table they come from (its domain). Verbs on
values (comparison, `in`, `?`, `string` and so on) see the values alone. Indexing, `#`, `_`, sorting, `distinct`,
queries and joining two of one domain keep the tag. Adding to an enumeration or to an enumerated column (`,:`, `insert`,
`upsert`, `update` of some rows, amend) checks the new values and signals `'cast` for one the domain lacks; nulls are
accepted. `value x` gives the values, `key x` the domain's name, `` `long$x `` where each stands in the domain, and
`meta` and `fkeys` show the foreign keys. A compound key is enumerated from a list of key rows
(`` `ktc$(`Dent`Arthur;`Prefect`Ford) ``).

Unlike kdb+, which holds positions in the domain:

* changing an item of the domain (`sym[0]:`twit`) leaves enumerations as they were;
* the type is `20h` (`-20h` for an atom) whatever the domain;
* a key removed from its table stays in the column, and following it gives a null.

## Syntax

* assignment `:`, global `::`, amend `x[i]:v`, `x+:1`, `@[d;i;f;y]`, deep amend `.[d;path;f;y]`; `x+::1` and
  `x[i]::v` change the global from inside a function (a local of that name, where there is one, as `x::v` does)
* assignment at depth `x[i;j]:v` and `x[i;j]+:v`: an index left empty (`x[;j]`) or a list of indices stands for each
  of those items, and a list of values then gives one to each (`'length` otherwise). Nothing can be assigned through a
  second pair of brackets (`x[i][j]:v` is `'assign`). A row of a table is not assigned so (`t[i;c]:v` is `'nyi`; give
  the column first, `t[c;i]:v`, or use `update`)
* lambdas `{[a;b] ...}` (implicit `x y z`), projections `f[1;]`, early return `:value`
* adverbs `'` `/` `\` `':` `/:` `\:`, and the words `each`, `over`, `scan`, `prior`
* `$[c;t;f]`, `if[...]`, `do[...]`, `while[...]`; a null condition is true; `[a;b;c]` runs expressions in turn
* `?[c;a;b]` chooses item by item; `.z.s` is the running lambda; lines holding only `/` and `\` enclose a comment
* signal `'err`, protected evaluation `@[f;x;handler]` and `.[f;args;handler]`
* views `v::expression`, which read global names only (those of the view's namespace first), never a query's columns or
  a function's variables; `.z.b` lists the views that read each name, and a view that depends on itself is `'loop`
* a compound assignment (`n+:1`, `t@:i`) in a function changes the global of that name, unless the function has a local
  of it. A name with a namespace (`.u.w:x`) is always a global's
* what is applied to a verb left waiting for its argument composes with it: `(key flip value@) t`, `f: 2#reverse@`. An
  amend with a place left empty (`@[;`sym;`g#]`, `.[;();:;]`) is a projection like any other
* namespaces (`\d .name`). A table given by its name (`` `t insert x ``, ``@[`t;...]``, `` delete from `t ``) in a
  function of a namespace is the root's if the namespace has none of that name; ``@[`.;names;f]`` changes globals
  of the root, and `` delete a,b from `.ns `` removes names from a namespace
* `0 x` evaluates `x` here, as a message through a handle is evaluated

## Verbs

| Group | Verbs |
| --- | --- |
| arithmetic | `+ - * % div mod xexp xlog neg abs sqrt exp log sin cos tan asin acos atan floor ceiling reciprocal signum` |
| comparison, logic | `= <> < <= > >= ~ & \| not and or all any null within in like` |
| lists | `til count first last # _ sublist enlist raze flip reverse rotate distinct where group iasc idesc asc desc rank xrank xprev cut cross inter union except bin binr` |
| aggregates | `sum prd avg wavg wsum min max med var dev svar sdev cov scov cor` |
| matrices | `mmu inv lsq`, and `x$y` of float lists and matrices (`mmu`) |
| running, moving | `sums prds avgs mins maxs deltas ratios differ prev next fills mavg msum mcount mmin mmax mdev ema xbar` |
| tables | `cols keys meta fkeys xasc xdesc xkey xcol xcols xgroup ungroup insert upsert fby tables views` |
| strings, casts | `string upper lower trim ltrim rtrim ss ssr vs sv $ md5` |
| system | `get set value parse eval attr system show getenv setenv ltime gtime -8! -9! -11!` ([logs](storage.md#logs)) |
| files, handles | `hopen hclose hcount hsym hdel key read0 read1 save load rsave rload csv 0: 1: -25! -30! -1 -2 1 2` ([storage.md](storage.md#files-and-tables-on-disk), [ipc.md](ipc.md#sending-to-clients)) |
| iterators | `each peach over scan prior` (`peach` runs as `each`) |

`.z.p .z.P .z.d .z.D .z.t .z.T .z.n .z.N .z.z .z.Z` give the time and date; `.z.u .z.i .z.o .z.K .z.k .z.h .z.c .z.l
.z.q .z.r` the user and process (`.z.K` is 4.1 and `.z.k` the date that kdb+ 4.1 was released); `.z.x .z.X .z.f` the command line (`.z.x`: the arguments after `--`; passwords show as
`***`); `.z.w .z.a .z.H .z.W` connections ([ipc.md](ipc.md#callbacks)). Date and time parts are read with
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
(with `:n` repetitions) timing, `\t 1000` the timer's interval in milliseconds (`\t 0` stops it), `\x name` removes a
variable, `\w` workspace statistics, `\P` significant digits of floats, `\c` lines and characters
of printed output, `\cd` the directory that relative paths are taken from (`\cd dir` changes it, for the whole
workspace), `\p` the port (`\p 5010` opens one in `kix`, `\p 0` closes it; setting this or the directory is for
[the console and startup scripts](ipc.md#shell-commands)), `\S` the random seed, `\W` the day that
weeks start on (0 Saturday; 2, Monday, at first) and `\z` how `"D"$` reads `06/01/2010` (0, month first, at first; 1,
day first). `\S`, `\W` and `\z` hold for the whole workspace and are not saved with it. Until `\S n` is given, `?`
draws differently on every run (`\S` then shows `0Ni`); with a seed its draws repeat, though they are not kdb+'s, and
guids differ still. Any other `\` command, and `system` of it, runs in the operating system's shell
where that is allowed ([shell commands](ipc.md#shell-commands)).

## Printing

```q
-1 "one line";                    / to the output; -2 to the errors. A list of strings gives a line each
1 "no line end ";                 / 1 and 2 write the text as it is
-1 each ("a"; "b");
```

The value of each is the handle, which the console shows unless the line ends with `;` (as with any statement). In `kix`
and the desktop app, what a statement and the ones typed with it print (`show` too) appears when they have all run. A
server prints what a client's request or a callback prints on its own console, as it happens.

`.Q.s x` is what the console shows for `x`; `.Q.s1 x` and `-3!x` give its one-line q form (`` `a`b!1 2 ``,
`` +`c!,1 2 ``). Both round floats to `\P` digits, as `string` does. Files written with `0:` and exported CSV keep every
digit, unlike kdb+.

## `.Q`, `.j` and `.h`

| Group | Names |
| --- | --- |
| characters, types | `.Q.A .Q.a .Q.an .Q.b6 .Q.n .Q.nA .Q.t .Q.res .Q.k .Q.ty .Q.qt` |
| text and dates | `.Q.f .Q.fmt .Q.id .Q.dd .Q.s .Q.s1 .Q.addmonths` |
| encoding | `.Q.btoa .Q.atob .Q.sha1 .Q.gz .Q.j10 .Q.x10 .Q.j12 .Q.x12 .Q.addr` |
| tables | `.Q.ff .Q.fk .Q.ft .Q.ind .Q.V` |
| databases ([storage.md](storage.md#partitioned-databases-and-column-attributes)) | `.Q.en .Q.ens .Q.dpft .Q.dpfts .Q.dpt .Q.dpts .Q.hdpf .Q.par .Q.chk .Q.view .Q.cn .Q.pn .Q.pv .Q.PV .Q.pd .Q.PD .Q.P .Q.pf .Q.pt .Q.MAP` |
| files | `.Q.fs .Q.fsn` |
| functions | `.Q.fc .Q.fu .Q.ts .Q.trp .Q.trpd .Q.sbt .Q.bt .Q.v` |
| command line, memory | `.Q.opt .Q.def .Q.x .Q.gc .Q.w` |
| JSON | `.j.j .j.jd .j.k` |
| delimited text, XML, spreadsheets | `.h.cd .h.td .h.d .h.xd .h.ed .h.edsn .h.tx .h.xt` |
| HTML and escapes | `.h.br .h.code .h.ha .h.hb .h.hc .h.hr .h.hta .h.htac .h.htc .h.html .h.http .h.hu .h.uh .h.hug .h.sc .h.nbr .h.pre .h.text .h.xmp .h.xs .h.iso8601 .h.sa .h.sb .h.c0 .h.c1 .h.ty` |

A variable assigned one of these names takes its place. Where they differ from kdb+:

* A backtrace (`.Q.bt`, and what `.Q.trp` gives its handler) lists the source of the functions that were running,
  without marking where in each. `.Q.fc` applies its function once, on one thread.
* Memory is freed as values are dropped, but the allocator keeps it for later values; `.Q.gc` asks it to give it back to
  the system, and returns 0, as how much it gave back is not measured. `.Q.w` gives nulls for what is not measured.
* `.j.j` writes floats to `\P` digits, a keyed table as the array of its rows, and a function as its source.

## Not implemented

* `ej` only in bracket form.
* A synchronous call from a server to a client.
* q's other commands of one character (`\B \C \e \g \o \r \s \T \u \_ \1 \2`).
* The parse tree of a composition, and `'[f;g]`.
* The callbacks of web and socket servers (`.z.ph .z.pp .z.pm .z.ac .z.ws .z.wo .z.wc`), `.z.pq` and `.z.pd`: kixdb has
  none of these.
* `.z.e .z.ex .z.ey .z.m .z.M .z.zd`.
* Reading binary records with `1:`, and compressed files.
* Reading logs that kdb+ wrote: kixdb's [logs](storage.md#logs) are its own.
* The rest of the `.Q` namespace: `.Q.qp`, `.Q.bv` and `.Q.vp`, `.Q.hg` and `.Q.hp` (HTTP), `.Q.host`, `.Q.K`, and
  kdb+ internals (`.Q.ipc`, `.Q.prf0`, `.Q.rp`, `.Q.m`).
* The parts of `.h` that serve web pages (`.h.he .h.hn .h.hp .h.hy .h.ka .h.jx .h.val .h.HOME .h.ht .h.fram .h.logo`):
  kixdb has no web server.
