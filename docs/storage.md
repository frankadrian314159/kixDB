# Storage

## Durable workspaces

`Server::open(dir)`, `kix dir` and `kix-serve dir` keep the workspace in `dir`:

| File | Contents |
| --- | --- |
| `kix.wal` | checksummed log: one record per change to global variables, views and distribution declarations |
| `kix.snap` | checksummed snapshot of the workspace, replaced atomically |
| `kix.lock` | held while the directory is open; a second process is refused |

A change is logged (and by default forced to disk) before it takes effect. The log is folded into a snapshot after 1,000
records (`StorageOptions::checkpoint_every`), on `.checkpoint`, and when the server closes. After a crash the snapshot is
loaded and the log replayed; an incomplete record at the end of the log is discarded.

For a vector, table or keyed table that keeps its columns, a log record holds **only what changed**: the rows added to
the end; the rows set in a column, if at most one in eight was, and otherwise the whole column; or the numbers of the
rows removed. So an `insert` costs the size of the new rows, an `update` the size of the rows or columns it assigns, and
a `delete` a few bytes a row it removes. Reordering rows, removing more rows than stay, changing which columns a table
has, a first assignment, other kinds of value, the first change to a value read from a file and, in a replica of a group
just made, the first change to a value it held already write the whole value. `kix-serve --no-sync` skips the flush per
record, at the risk of losing the latest changes in a crash.

An `insert` or `upsert` into a global table changes it where it is: it copies no more than the rows it touches (but a
column that a running query still reads is copied first), and an `upsert` looks once through the keys. It rebuilds the
table if a column holds mixed values or would change type (a [column of strings](#columns-of-strings) is neither). For other changes the old and new values are compared to
find what changed, which takes time in proportion to their size. An `upsert` into a keyed table that holds a key twice
sets the first such row and keeps the others (unless the table is rebuilt, which keeps one row a key).

Earlier versions cannot read these records. Before going back to one, close the server cleanly, which folds the log into
the snapshot: an earlier version discards the log from the first record it cannot read.

## Columns of strings

A column of a stored table that holds nothing but strings is kept **packed**: the text in one block and 8 bytes a
string, where a list of character vectors takes some 80 bytes a string besides the text. To `q` it is that list, in
every way (type `0h`). This is automatic, for every table that becomes a global or is read from a file; a list of
strings that is not a table's column is left as it is.

* `count`, indexing, filtering, joins, sorting, grouping, `distinct`, appending, `like`, `in`, `?`, `~`, `count each`,
  `upper`, `lower`, `trim` and `` `$ `` and other casts from text read the packed strings. Other verbs are given the
  list, which is made for as long as they run.
* An item taken out is a copy of its text.
* Setting rows (`update ... where`) rebuilds the column. A value that is not a string makes a list of it; once it holds
  only strings again it is packed again.
* Saved files, splayed and partitioned tables, the log, snapshots, replication and the messages between the nodes of a
  cluster hold the packed form. **Earlier versions cannot read them**; later ones read what earlier ones wrote.
  Programs connected over IPC, and `-8!`, are given ordinary lists, as the kdb+ protocol has them.

## Files and tables on disk

```q
`:prices.bin set 1 2 3            / a flat file; get `:prices.bin, or \l prices.bin for the variable prices.bin
`:ref/ set .Q.en[`:hdb; t]        / a splayed table: .d and one file per column
\l hdb/ref                        / load a splayed table as the variable ref
.Q.en[`:hdb; t]                   / t with its symbol columns enumerated from hdb/sym
.Q.ens[`:hdb; t; `venue]          / from hdb/venue, and the global venue
.Q.dpft[`:hdb; 2026.11.24; `sym; `trade]   / write a partition, sorted and parted by sym (.Q.dpfts names the domain)
\l hdb                            / mount it
```

`.Q.en` adds the symbols that `hdb/sym` lacks, sets the global `sym` to the list and enumerates the columns from it
([language.md](language.md#enumerations-foreign-keys-and-linked-columns)). So a symbol that is not in the domain cannot
be put into such a column: `insert` and `upsert` signal `'cast` unless the rows go through `.Q.en` too. (Earlier
versions returned the table unchanged, and saved plain symbols.)

### A file by its name

A file symbol can stand where the name of a global does. The whole file is read, and written again when it changes.

```q
select from `:ref where v > 1     / also exec, cols, meta
`:ref/ upsert rows                / also insert; the file is made if it is not there
`v xdesc `:ref                    / sort what the file holds
@[`:ref; `v; neg]                 / amend it; a splayed table stays splayed
.[`:log; (); ,; 42]               / add to a list; `:log is made if it is not there
.[`:log; (); :; 1 2 3]            / replace it
save `:out/trade                  / the global trade, to a file named after it; load `:out/trade reads it back
save `:out/trade.csv              / as text: also .txt (tabs), .xml and .xls
rsave `:db/trade/                 / splayed; rload `:db/trade
```

### Reading, writing and deleting

```q
read0 `:notes.txt                 / the lines; read0 (`:notes.txt; offset; length): those characters
read1 `:notes.txt                 / the bytes, also of a part
`:notes.txt 0: ("one"; "two")     / write lines; `:raw 1: 0x0102 writes bytes
.Q.fs[f; `:big.csv]               / f applied to the lines, about 131000 bytes at a time; .Q.fsn sets the size
hsym `$path                       / a file symbol; hcount its size, hdel removes it (or an empty directory)
key `:dir                         / the names in a directory; the symbol of a file; () if nothing is there
h: hopen `:app.log                / a handle to a file, made if it is not there
neg[h] "a line"                   / add lines (a string or a list of strings)
h 100 200; hclose h               / add items to a file that holds a list, otherwise the values' bytes
```

Adding to a file that holds a simple list (`1 2 3`) rewrites it, so each addition costs as much as the file is long.
A file that holds a general list is a log.

### Logs

```q
.[`:day; (); :; ()]               / an empty log
h: hopen `:day
h enlist (`upd; `trade; x)        / add a message: it costs only itself
-11!`:day                         / run the messages, as asynchronous ones from a client are (.z.ps, else their values)
-11!(100; `:day)                  / the first 100
-11!(-2; `:day)                   / run none: how many are good, with the bytes they take if the log is damaged
```

* The first addition to a file that holds a general list rewrites it as a log; later ones are appended. `get` gives the
  list, and `-11!` also replays one that `set` wrote.
* Each message is stored with its length and a checksum. Replaying a log that is cut short or damaged runs the good
  messages and then signals `'badtail`; `get` of it signals `'os`. To cut it to its good part:
  ``(`:good) 1: read1 (`:day; 0; last -11!(-2; `:day))``.
* **The format is kixdb's own:** a log that kdb+ wrote cannot be read, nor can kdb+ read this one.
* Additions are not flushed to the disk one by one: a failure of the machine can lose the last of them.

### Delimited, fixed-width and key-value text

```q
("SFJ"; enlist csv) 0: `:file.csv / read CSV with a header row; csv is ","
("SFJ"; ",") 0: lines             / without one: a list of columns
("SFJ"; 6 8 4) 0: `:fixed.txt     / fields of fixed widths, a record to a line; also (file; offset; length)
"S=;" 0: "a=1;b=2"                / keys of the first type and string values
`:file.csv 0: csv 0: t            / write CSV (fields with commas, quotes or line breaks are quoted)
```

`1:` writes bytes but does not read binary records.

Enumerated values are saved with their domain's name, not the domain: save that too to check or follow them after
loading. Earlier versions cannot read files, logs or snapshots that hold enumerations.

Relative paths are resolved against the workspace directory of a durable workspace, and otherwise against the process's
current directory. (Earlier versions resolved those given to `0:`, `1:`, `hcount` and `.Q.en` against the current
directory.) The file format is kixDB's own and is not readable by kdb+.

## Looking up keys

A keyed table of 1,000 rows or more whose keys are looked for a second time (`k 5`, an `upsert` or `insert` of a few
rows, `lj` of a few rows, `select ... where` its one key `=` a value) is given an **index** of its keys, so that finding
them does not read through the table. It is kept in memory beside the table (about 36 bytes a row, an estimate), grows
with the rows that are added, and is made again, when next wanted, after the keys change in any other way (a `delete`,
an `update` of a key, a new table). Keys of floating point numbers, strings and general lists have none: they are read
through as before. Nothing about it is written to files or sent to replicas.

## Partitioned databases and column attributes

Directories named `YYYY.MM.DD`, `YYYY.MM`, `YYYY` (`1800..=2999`) or an integer are partitions; each table inside is
splayed. The partition appears as the virtual column `date`, `month`, `year` or `int`, and its values are listed under
that name (`date`). A `par.txt` file in the database's directory lists further directories (segments), one to a line,
whose partitions belong to it too.

* The columns of a table are those of its last partition. A query that needs one that an older partition lacks signals
  `'mismatch` until `.Q.bv[]` (or `` .Q.bv[`] ``, which also fills in virtual empty slices where an older partition lacks
  the table directory altogether) populates `.Q.vp` with empty schema prototypes to backfill missing columns with nulls.
  `.Q.chk` writes empty splayed tables on disk for partitions that lack them.
* Assigning to the name of a mounted table replaces it, until the database is mounted again; `\l` of the database
  replaces variables of its tables' names. (A variable that was there before `.Q.dpft`, or that a durable workspace
  restores, stays beside the table: queries read the table, and other uses of the name the variable.)
* `.Q.view 2026.11.23 2026.11.24` hides the other partitions from queries until `.Q.view[]`. `.Q.pv` and `.Q.PV` give
  the values shown and all of them, `.Q.pf` the partition column, `.Q.pt` the partitioned tables, and
  `.Q.ind[table; rows]` rows by number.
* Mounting a directory loads its tables and values; it does not run the scripts in it. `\l` looks in the workspace's
  directory first, then in the process's.
* A linked column (`` `other!rows ``) is not followed from one partitioned table to another.
* `.Q.par[dir; partition; table]` says where a table goes: in `dir`, or in the segment that `par.txt` gives the
  partition to, each in turn. `.Q.dpft` and the others write there. `.Q.dpt[dir; partition; table]` (`.Q.dpts` with a
  domain) writes without sorting by a field.
* `.Q.hdpf[address; dir; partition; field]` ends a day: it writes every table of the root namespace, empties it and,
  if a process listens at `address`, has it run `\l .`. The tables are not mounted in the process that writes them.
* `.Q.cn table` counts the rows of each partition shown, from the table's column headers; `.Q.pn` does so for every
  table. `.Q.pd` and `.Q.PD` are the partitions' directories (shown, all), `.Q.P` the segments, and `.Q.qp x` gives
  `1b` for a partitioned table, `0b` for a splayed table and `0` otherwise. `.Q.MAP[]` keeps every mounted partition's
  column files memory-mapped across queries (with mapped bytes reported in `.Q.w[]` and `\w`).

* Any `where` clause on the partition column prunes partitions (`=`, `<`, `<=`, `>`, `>=`, `in`, `within`), whether it is
  the first clause or appears after other row-wise filters. Leading single-column filters also narrow each partition
  before reading the matching rows of other columns, using per-partition `` `p# `` and `` `s# `` attributes directly.
* A `select` or `exec` that lists its columns reads only the columns it mentions, unless it can name one at run time
  (through `value`, `get`, `eval`, `reval`, `system` or a variable holding a verb), and `count`, `cols`, `meta` and
  queries that touch only the partition column read column headers alone. In memory too, only the columns that are read
  are copied.
* Column attributes accelerate lookups and joins: `` `s# `` uses `O(log N)` binary search (`=`, `<`, `<=`, `>`, `>=`, `in`,
  `within`, `?`, keyed joins), while `` `u# ``, `` `p# `` (such as HDB `sym` written by `.Q.dpft`) and `` `g# `` maintain
  cached `O(1)` hash, run-range, and inverted indexes. A column keeps `` `g# `` as it is added to (`insert`, `,:`); its
  index is built the second time the column is looked in without having changed.
* Splayed and partitioned column files are read through read-only OS memory mappings (`Mmap`), extracting only the
  surviving rows when filtered.
