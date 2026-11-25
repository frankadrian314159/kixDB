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
the end, and whole any column that changed in another way; or the numbers of the rows removed. So an `insert` costs the
size of the new rows, an `update` the size of the columns it assigns, and a `delete` a few bytes a row it removes.
Reordering rows, removing more rows than stay, changing which columns a table has, a first assignment, other kinds of
value, the first change to a value read from a file and, in a replica of a group just made, the first change to a value
it held already write the whole value. The old and new values are compared to find what changed, which takes time in
proportion to their size. `kix-serve --no-sync` skips the flush per record, at the risk of losing the
latest changes in a crash.

Earlier versions cannot read these records. Before going back to one, close the server cleanly, which folds the log into
the snapshot: an earlier version discards the log from the first record it cannot read.

## Files and tables on disk

```q
`:prices.bin set 1 2 3            / a flat file; get `:prices.bin
`:ref/ set t                      / a splayed table: .d and one file per column
.Q.en[`:hdb; t]                   / record the table's symbols in hdb/sym
.Q.dpft[`:hdb; 2026.11.24; `sym; `trade]   / write a partition, sorted and parted by sym
\l hdb                            / mount it
("SFJ"; enlist ",") 0: `:file.csv / read CSV with a header row
`:file.csv 0: "," 0: t            / write CSV
```

Relative paths given to `set`, `get` and `.Q.dpft` are resolved against the workspace directory; `0:` and `.Q.en` use
the process's current directory. The file format is kixDB's own and is not readable by kdb+.

## Partitioned databases and column attributes

Directories named `YYYY.MM.DD`, `YYYY.MM`, `YYYY` (`1800..=2999`) or an integer are partitions; each table inside is
splayed. The partition appears as the virtual column `date`, `month`, `year` or `int`.

* Any `where` clause on the partition column prunes partitions (`=`, `<`, `<=`, `>`, `>=`, `in`, `within`), whether it is
  the first clause or appears after other row-wise filters.
* A `select` or `exec` that lists its columns reads only the columns it mentions, unless it can name one at run time
  (through `value`, `get`, `eval`, `reval`, `system` or a variable holding a verb). In memory too, only the
  columns that are read are copied.
* Column attributes accelerate lookups and joins: `` `s# `` uses `O(log N)` binary search (`=`, `<`, `<=`, `>`, `>=`, `in`,
  `within`, `?`, keyed joins), while `` `u# ``, `` `p# `` (such as HDB `sym` written by `.Q.dpft`) and `` `g# `` maintain
  cached `O(1)` hash, run-range, and inverted indexes.
* Selected partitions are loaded into memory for each query; nothing is memory-mapped.
