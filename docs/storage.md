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

A log record holds the **whole new value** of each variable it changes, so every write to a large table writes that table
again. For high write rates, batch rows. `kix-serve --no-sync` skips the flush per record, at the risk of losing the
latest changes in a crash.

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

## Partitioned databases

Directories named `YYYY.MM.DD`, `YYYY.MM` or an integer are partitions; each table inside is splayed. The partition
appears as the virtual column `date`, `month` or `int`.

* A `where` clause on the partition column prunes partitions when it is the **first** clause (`=`, `<`, `<=`, `>`,
  `>=`, `in`, `within`).
* A `select` or `exec` that lists its columns reads only the columns it mentions.
* Selected partitions are loaded into memory for each query; nothing is memory-mapped.
