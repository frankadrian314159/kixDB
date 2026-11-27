# Benchmark results: scale factor 0.3

## Threads and keyed tables

Measured 2026-11-26 on mains power (6 cores, 12 threads), one run each. Provisional, as below.

TPC-H's single-statement queries on one engine for 8 s, from 1 client and from 12 at once, with each number of threads
(`RAYON_NUM_THREADS`), on the build with packed strings:

| Threads | Queries/s, 1 client | Processor ms a query | Queries/s, 12 clients | Processor ms a query |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 29 | 34 | 54 | 80 |
| 4 | 42 | 73 | 49 | 92 |
| **6** | **45** | **93** | **51** | **118** |
| 8 | 45 | 116 | 51 | 144 |
| 12 | 43 | 156 | 51 | 171 |

All 22 TPC-H and 24 TPC-DS queries once each took 0.58 and 0.19 s with 1 thread, 0.48 and 0.16 s with 6, and 0.46 and
0.17 s with 12. kixDB's programs now use one thread a core.

A keyed table of 1,000,000 rows, before the [index of keys](storage.md#looking-up-keys) and with it, milliseconds
for all the repetitions together:

| Statement | Times | `9de4c2e` | now |
| --- | ---: | ---: | ---: |
| upsert a key that is there (the second builds the index) | 100 | 304 | 66 |
| upsert a new key | 1,000 | 3,039 | 8 |
| `k 999999` (the last key) | 100 | 4,898 | under 1 |
| `select from k where id = 999999` | 100 | 50 | under 1 |
| `x lj k`, 4 rows | 100 | 1,255 | under 1 |
| two key columns: upsert (the second builds the index) | 100 | 2,064 | 86 |
| two key columns: look up | 100 | 12,204 | under 1 |

## Packed string columns

Measured 2026-11-26 on mains power: commit `305ae9c` and [packed columns of strings](storage.md#columns-of-strings),
run back to back, one run each. Provisional, as below.

| | `305ae9c` | now |
| --- | ---: | ---: |
| TPC-H tables in memory | 460 MB | **270 MB** |
| `lineitem.l_comment`, bytes a row | 104 | 32 |
| TPC-DS tables (few strings) | 107 MB | 107 MB |
| TPC-H load, 3 shards in-process | 2.28 s | 1.12 s |
| TPC-H load, single node | 1.84 s | 2.00 s |
| Queries, TPC-H / TPC-DS, single node | 0.44 / 0.15 s | 0.47 / 0.16 s |
| Queries, TPC-H / TPC-DS, 3 shards | 0.53 / 0.28 s | 0.54 / 0.30 s |

The single node's TPC-H total includes one run of Q1 at 85 ms against 54; Q1, Q10 and Q13 run again alone, one after
the other, took the same on both. The load's peak memory (1.0 GB) is unchanged: the generator holds lists while the
tables are packed.

A table of 1,000,000 rows with two columns of strings, milliseconds a statement:

| Statement | `305ae9c` | now |
| --- | ---: | ---: |
| insert one row | 75 | 0.07 |
| `count each s` | 187 | 2 |
| `where c ~\: "alpha"` | 231 | 11 |
| `where c in ("alpha"; "delta")` | 100 | 20 |
| `distinct c` | 77 | 15 |
| `select count i by c` | 82 | 19 |
| join on a string key | 107 | 25 |
| `upper s` | 249 | 81 |
| `` `$c `` | 73 | 24 |
| `` `s xasc t `` | 88 | 34 |
| `where s like "*12*"` | 32 | 22 |
| filter that keeps the strings | 22 | 14 |
| `update ... where` setting a string | 24 | 17 |
| `-8! t` | 59 | 25 |
| save / load the splayed table | 76 / 227 | 40 / 33 |
| **slower:** `` t[`c] = t`c `` (the verb is given a list) | 399 | 650 |
| **slower:** `` t[`s] ,\: "!" `` | 497 | 608 |
| **slower:** `` {x, "!"} each t`s `` | 639 | 809 |

A string taken from a packed column is a copy, where an item of a list was shared: this is what makes the last three
slower, and some queries allocate more for it (TPC-H Q13, 37 to 67 MB).

## After the second round of changes

Measured 2026-11-26 on the machine described under [Setup](#setup), on mains power: commit `5a15686` and the changes
below, run back to back, one run each. **Treat these figures as provisional:** the machine's memory has still not been
tested. On battery the same machine runs about half as fast.

| Configuration | Queries, TPC-H / TPC-DS (s): `5a15686` | now | Load, TPC-H / TPC-DS (s): `5a15686` | now |
| --- | ---: | ---: | ---: | ---: |
| Single node | 0.49 / 0.21 | 0.42 / 0.14 | 1.92 / 0.32 | 1.71 / 0.17 |
| 3 shards, in-process | 0.61 / 0.35 | 0.56 / 0.28 | 2.34 / 0.58 | 2.23 / 0.38 |

The cluster figures are from the build before the last change to loading, which affects memory and the single node's load.

Single statements on 1,000,000 rows or items, milliseconds:

| Statement | `5a15686` | now |
| --- | ---: | ---: |
| `d[count d]: 1`, 4,000 times | 869 | 8 |
| `x,: 1`, 40,000 times | 180 | 27 |
| `a+: 1`, 1,000,000 times | 665 | 367 |
| `count each s` (strings) | 306 | 108 |
| `` `sym xasc t `` | 328 | 32 |
| `` `u insert t `` into an empty table | 102 | 2 |
| `update v: 0 from t` | 18 | 1.3 |
| `` update v: 0 from t where sym = `a `` | 17 | 2.0 |
| upsert one row of a keyed table | 11.8 | 2.1 |
| `-8! t` (3 columns, one of symbols) | 31 | 9.5 |
| `-9!` of that | 49 | 37 |

Appending 20,000 items one at a time allocates 17 MB instead of 1.5 GB, and 5,000 dictionary inserts 6 MB instead of
3 GB.

What changed:

* **Assignments change a variable where it is** ([amend.rs](../src/engine/amend.rs)): `x,: y`, `x[i]: y` and `d[k]: y`
  for one item or key, where the value keeps its type, cost the items they touch. They are logged and replicated as those
  changes. A log with a change to a dictionary cannot be replayed by an earlier version.
* **Symbol names are read without a lock or a copy** ([sym.rs](../src/sym.rs)), which speeds calls of primitives,
  sorting, and sending symbols.
* **Writes and updates work column by column:** bulk `insert` no longer makes a list for every row, and `update` builds
  columns of their types.
* **`xasc` and `xdesc`** grade one typed column at a time.
* **A faster hash function** (foldhash) for grouping, joins, `in`, indexes and the symbol table.
* **`.Q.gc` asks the allocator to give memory back.** After the TPC-H tables were dropped, the process held 909 MB, and
  72 MB after `.Q.gc[]`.

## After the performance changes

The seven [recommendations](#recommendations) below were carried out, except that new tables are still created in two
phases, so that changes stay atomic. They were measured on 2026-11-25 against the figures for `5e031c1`, on the same
machine, one run each. **Treat these figures as provisional:** the machine had stopped with
system errors several times, once during this work, and its memory had not been tested. They were measured after a clean
rebuild.

### Queries and loading

TPC-H / TPC-DS query totals in seconds, and the time to load TPC-H.

| Configuration | Queries: `5e031c1` | now | Load: `5e031c1` | now |
| --- | ---: | ---: | ---: | ---: |
| Single node | 0.85 / 0.26 | 0.47 / 0.17 | 3.08 | 1.94 |
| Single node, `RAYON_NUM_THREADS=1` | | 0.54 / 0.20 | | 1.96 |
| 1 shard process | 1.22 / 0.56 | 0.78 / 0.43 | 9.11 | 3.27 |
| 3 shard processes | 0.76 / 0.43 | 0.63 / 0.33 | 10.09 | 2.29 |
| 6 shard processes | 0.82 / 0.46 | 0.62 / 0.34 | 7.45 | 2.19 |
| 6 shards, in-process | 1.13 / 0.44 | 0.57 / 0.37 | 7.76 | 2.23 |

* **Most of the query gain on one node is not from threads:** one thread gives 0.54 / 0.20.
* **Slower queries:** the largest were TPC-DS Q38 with one shard (73 to 111 ms), TPC-H Q3 with three shards (22 to
  38 ms), TPC-H Q7 on the single node (23 to 32 ms) and TPC-DS Q19 there (6 to 16 ms). With single runs, the cause is
  not established.

### Writes

Median milliseconds a statement, `5e031c1` to now.

| Statement | Rows | Node, memory | Node, durable | 3 shards, memory | 3 shards, durable | 3 replicas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| insert 1 row | 10,000 | 0.09 to 0.006 | 0.48 to 0.45 | 0.45 to 0.19 | 0.92 to 0.67 | 1.93 to 1.44 |
| | 1,000,000 | 5.8 to 0.005 | 8.7 to 0.46 | 5.1 to 0.19 | 7.0 to 0.68 | 14.3 to 1.47 |
| insert 1,000 rows | 10,000 | 0.26 to 0.09 | 0.84 to 0.61 | 1.37 to 0.90 | 3.9 to 3.4 | 2.9 to 2.0 |
| | 1,000,000 | 6.5 to 0.06 | 9.2 to 0.59 | 16.2 to 0.86 | 20.3 to 3.4 | 15.7 to 2.0 |
| upsert 1 keyed row | 10,000 | 2.4 to 0.12 | 3.0 to 0.56 | 1.6 to 0.22 | 1.9 to 0.73 | 6.0 to 1.55 |
| | 1,000,000 | 287 to 12 | 284 to 13 | 191 to 4.3 | 193 to 4.9 | 648 to 14 |
| update 1 column | 10,000 | 0.30 to 0.30 | 1.47 to 1.50 | 0.74 to 0.77 | 3.6 to 3.6 | 4.4 to 3.3 |
| | 1,000,000 | 4.0 to 2.7 | 16.1 to 16.3 | 9.1 to 3.8 | 20.0 to 14.7 | 72 to 58 |

* An update of a whole column still takes time in proportion to the table.
* Of 1,100 durable one-row inserts into 1,000,000 rows the median took 0.50 ms (before: 8.5) and the worst 65 ms
  (before: 47).

### Concurrent clients

Queries a second with 1, 4 and 12 clients.

| Query | Single node: `5e031c1` | now | 3 shards: `5e031c1` | now |
| --- | --- | --- | --- | --- |
| `select sum v by sym from t` | 97, 96, 96 | 105, 196, 215 | 157, 170, 165 | 160, 221, 225 |
| `exec sum v from t where sym = `a` | 955, 964, 970 | 1285, 1707, 2101 | 1135, 1616, 1626 | 1158, 2539, 3246 |
| `select from k where id = 5` | 716, 724, 724 | 1836, 2666, 2784 | 1335, 2026, 2021 | 1683, 4124, 6107 |
| `count t` | 962,000, 514,000, 503,000 | 411,000, 789,000, 855,000 | 4149, 9613, 15,387 | 3217, 9680, 16,244 |

### What got worse

* **A change made while a read is running copies the list of global variables** (see the table below).
* **The shortest requests are slower for one client:** `count t` fell from 962,000 to 411,000 a second on one node and
  from 4149 to 3217 on three shards.

### Cost of the number of variables

In the runs above, each read copied the list of global variables. The list is now shared until a write changes it; this
was measured afterwards, the other figures were not repeated. Milliseconds on one node:

| Global variables | `count t`: copying | now | An assignment | during a read |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0024 | 0.0028 | 0.0017 | 0.0019 |
| 100 | 0.0031 | 0.0023 | 0.0016 | 0.0026 |
| 1,000 | 0.044 | 0.0023 | 0.0016 | 0.010 |
| 10,000 | 0.19 | 0.0024 | 0.0015 | 0.083 |
| 100,000 | 3.7 | 0.0024 | 0.0015 | 2.9 |

## Evaluation of commit `5e031c1`, with writes and concurrency

Measured 2026-11-25 on the machine described under [Setup](#setup), one run each. The sections after this one are older
and cover read queries only.

### Queries and loading

TPC-H / TPC-DS query totals in seconds, and the time to load TPC-H through the control node. **Previous** is commit
`2be8fc5`, before changes across shards became atomic. **mimalloc** is `5e031c1` with that memory allocator added to the
benchmark programs and the shard in a scratch copy.

| Configuration | Queries: previous | `5e031c1` | mimalloc | Load: previous | `5e031c1` | mimalloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | | 0.85 / 0.26 | 0.60 / 0.17 | | 3.08 | 2.20 |
| 3 shard processes | 0.72 / 0.41 | 0.76 / 0.43 | 0.62 / 0.36 | 4.84 | 10.09 | 5.66 |
| 6 shard processes | 0.73 / 0.41 | 0.82 / 0.46 | 0.65 / 0.42 | 3.96 | 7.45 | 4.62 |
| 6 shards, in-process | 1.06 / 0.39 | 1.13 / 0.44 | 0.65 / 0.43 | 4.23 | 7.76 | 4.83 |

* The single node is unchanged from the last measurements below.
* **Loading a cluster is about twice as slow** since two-phase commit: a table is sent to every shard, stored there, and
  decoded again to commit it. The same applies to tables a multi-statement query assigns: TPC-DS Q22 went from 65 to
  82 ms and Q36 from 29 to 38 ms on six shard processes. Other query differences are small enough to be run-to-run noise.
* Link-time optimisation with one code generation unit gave 0.81 / 0.25 and a 2.65 s load on the single node (about 4%
  on queries).

### Writes

Median milliseconds a statement, from one client (60 runs each). The tables hold the given number of rows in three
columns; the cluster's are spread over the shards. Durable engines force their log to disk.

| Statement | Rows | Node, memory | Node, durable | 3 shards, memory | 3 shards, durable | 3 replicas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| insert 1 row | 10,000 | 0.09 | 0.48 | 0.45 | 0.92 | 1.93 |
| | 1,000,000 | 5.8 | 8.7 | 5.1 | 7.0 | 14.3 |
| insert 1,000 rows | 10,000 | 0.26 | 0.84 | 1.37 | 3.9 | 2.9 |
| | 1,000,000 | 6.5 | 9.2 | 16.2 | 20.3 | 15.7 |
| upsert 1 keyed row | 10,000 | 2.4 | 3.0 | 1.6 | 1.9 | 6.0 |
| | 1,000,000 | **287** | **284** | **191** | **193** | **648** |
| update 1 column | 10,000 | 0.30 | 1.47 | 0.74 | 3.6 | 4.4 |
| | 1,000,000 | 4.0 | 16.1 | 9.1 | 20.0 | 72 |

Of 1,100 durable one-row inserts into 1,000,000 rows the median took 8.5 ms and the worst 47 ms (the log is folded into
a snapshot every 1,000 records).

### Concurrent clients

Queries a second against tables of 1,000,000 rows, with 1, 4 and 12 clients asking at once.

| Query | Single node | 3 shards, in-process |
| --- | --- | --- |
| `select sum v by sym from t` | 97, 96, 96 | 157, 170, 165 |
| `exec sum v from t where sym = `a` (one shard) | 955, 964, 970 | 1135, 1616, 1626 |
| `select from k where id = 5` (one shard) | 716, 724, 724 | 1335, 2026, 2021 |
| `count t` | 962,000, 514,000, 503,000 | 4149, 9613, 15,387 |

A short request to the cluster takes about 0.2 to 0.3 ms (`count t` 0.28 ms), a query routed to one shard 0.7 to 0.9 ms,
and a grouped join with a small table 16 ms.

### Recommendations

In the order suggested. "Code" means found by reading it, not measured on its own.

| # | Where | Change | Evidence |
| --- | --- | --- | --- |
| 1 | both | Use mimalloc in `kix-shard` and `kix-cluster`, and build `kix-serve` with it by default | Measured: query time down 29-35% on one node and 2-42% on the cluster; load time down 29-44% |
| 2 | both | Make changes across shards cheaper: send row bytes without encoding them one by one, keep what prepare worked out for commit, and create tables that do not exist yet without two phases (a design change) | Measured: loading takes 1.8-2.1 times as long as before; code |
| 3 | shard, node | Keyed `upsert` and `uj`: look keys up with the typed grouping code and change columns in place, instead of rebuilding every row as separate values ([joins.rs](../src/engine/joins.rs)); the shard's own key set likewise | Measured: 190-650 ms a row at 1,000,000 rows; code |
| 4 | control | Save round trips: `insert` counts the table on every shard first (return the count with the load), the planner counts tables for each join (cache the counts), and a replicated shard is probed before each change | Measured: a one-row insert (0.45 ms) takes nearly twice a `count` (0.24 ms); code |
| 5 | shard, node | Append to columns in place when no one else holds them, instead of copying the table | Measured: a one-row insert grows from 0.09 to 5.8 ms between 10,000 and 1,000,000 rows |
| 6 | shard, node | Let reads run side by side (they take the workspace's write lock), then run `where`, grouping and aggregation on several threads. Both change the engine's design | Measured: no gain from more clients on one node; code |
| 7 | shard | Replicate single changed values, not whole columns, for sparse updates (a new log record) | Measured: updating a column of 1,000,000 rows takes 72 ms with replicas, 16 ms durable without |

Not measured: `--state` files (written after every change; code), more than one machine, TLS under load, memory use
under concurrency.

Measured 2026-11-25 with the programs described in [benchmarks.md](benchmarks.md). Six changes were measured, each
against the build before it. **Before** below is the build with the [filtering
change](#pattern-matching-and-filter-results); **after** adds the [row number and constant column
change](#row-numbers-and-constant-columns). Earlier: [join keys and
requests](#typed-join-keys-and-queries-encoded-once), [slot tables and
merges](#slot-tables-for-narrow-ranges-and-merges-by-column), [aggregates for all groups at
once](#aggregates-are-computed-for-all-groups-at-once) and [columns, operators and
casts](#first-change-columns-operators-and-casts).

## Summary

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 0.84 s | **0.85 s** | 0.28 s | **0.25 s** | 1.0x | 1.00x |
| Cluster, 1 shard, in-process | 1.27 s | 1.25 s | 0.52 s | 0.48 s | 1.0x | 0.63x |
| Cluster, 3 shards, in-process | 0.96 s | 0.93 s | 0.41 s | 0.37 s | 1.1x | 0.85x |
| Cluster, 6 shards, in-process | 1.13 s | 1.08 s | 0.42 s | 0.38 s | 1.1x | 0.75x |
| Cluster, 12 shards, in-process | 1.57 s | 1.54 s | 0.50 s | 0.45 s | 1.0x | 0.55x |
| Cluster, 3 shard processes | 0.77 s | 0.71 s | 0.46 s | 0.46 s | 1.1x | 0.94x |
| Cluster, 6 shard processes | 0.78 s | **0.73 s** | 0.46 s | **0.41 s** | 1.1x | 0.96x |

Each figure in this table is from a single run.

* **DS Q38 and Q87**, which group by constant columns, went from 65 to 43 ms and from 57 to 44 ms on the single node
  (78 to 53 and 67 to 55 ms with six shard processes). Elsewhere the totals changed little.
* **Slower queries:** 15 of the 322 query and setup combinations were more than 5% (plus 1 ms) slower, the largest DS
  Q1 with one shard (20 to 31 ms) and DS Q22 with three shard processes (58 to 71 ms). The "before" runs were taken
  about an hour earlier, and the cause of these differences is not established.
* **Correctness:** every cluster answer in the 14 new runs matched the single node's, and all 46 queries returned
  identical rows on the single node. Generated comparisons agreed everywhere except where `i` is read after a filter,
  which changed on purpose.

## Setup

* AMD Ryzen 5 7430U (6 cores, 12 threads), 31 GB RAM, Windows 11, on mains power. Release build, rustc 1.95.
* In memory: no write-ahead log, no replication, no TLS. Shards and control node on one machine over loopback TCP.
* One client running one query at a time. This measures latency, not throughput under concurrency.
* The last four changes were run once per build and setup. Earlier changes report medians of five interleaved runs.
  Repetitions then mostly agreed within 5%, the worst by 12% (47% in the first session). Treat differences under about
  20% between single runs as inconclusive, and more on short cluster queries.
* Data: TPC-H 2,598,816 rows (`lineitem` 1,800,786). TPC-DS 1,310,782 rows (`store_sales` 539,498, `inventory`
  360,000, `catalog_sales` 179,761, `web_sales` 119,410).

These are **not TPC results**. The data comes from the repository's generator, and the queries are `q` rewrites. The
TPC-DS "scale factor" is 30 times the repository's 0.01 baseline: its `store_sales` is about a fifth of official SF 1,
and it covers 24 of the 99 queries.

## Load time and memory

The table is from the first change's run; peak memory was the same, within 30 MB, in the later runs. In that run
load times barely changed. Cluster load is the copy from an already generated single-node engine through the control
node; single-node load is generation plus load, so the two are not like for like. Memory is peak working set, sampled
every 50 ms. The cluster programs also hold the single-node reference engine.

| Configuration | Load H / DS (after) | Peak memory H, before to after | Peak memory DS (after) |
| --- | --- | --- | --- |
| Single node | 3.3 s / 0.29 s | 1.19 to 0.95 GB | 192 MB |
| 1 shard, in-process | 10.2 s / 1.28 s | 1.82 to 1.66 GB | 357 MB |
| 3 shards, in-process | 5.5 s / 0.84 s | 1.84 to 1.64 GB | 335 MB |
| 6 shards, in-process | 4.2 s / 0.78 s | 1.85 to 1.64 GB | 351 MB |
| 12 shards, in-process | 3.9 s / 0.78 s | 1.90 to 1.72 GB | 370 MB |
| 3 shard processes (control + shards) | 5.1 s / 0.83 s | 1.20 + 1.10 to 0.95 + 0.92 GB | 203 + 212 MB |
| 6 shard processes (control + shards) | 4.0 s / 0.74 s | 1.20 + 1.12 to 0.95 + 0.96 GB | 200 + 260 MB |

## What changed

### Row numbers and constant columns

* **`i` is now a row's number in the whole table**, as in q. Before, after a filter, `select`, `exec`, later `where`
  clauses and grouped queries numbered the rows that were left, from 0, and a grouped `update` numbered each group's rows
  from 0. Queries that relied on that give different answers now.
* **A constant column**, as in `select ok: 1b by ...`, is computed for all groups at once instead of group by group.

One timing each, on 1,800,000 rows (milliseconds; "before" here is commit `930628b`):

| Query | Before | After |
| --- | ---: | ---: |
| `select ok: 1b by k`, 440,000 groups | 375 | 148 |
| `select ok: 1b by k, d`, two keys | 2232 | 1021 |
| `select ok: 1b, sum x by k` | 572 | 206 |
| a grouped lambda reading `i` after a filter, 20,000 groups | 125 | 153 |

The last row is the price of the right numbers where a query still runs group by group (one timing).

### Pattern matching and filter results

* **`like`** ([ops.rs](../src/engine/ops.rs)) matched a fresh copy of every string or symbol name. It now matches the
  text where it lies. On text of one line it also drops a leading or trailing `*` from the pattern, so that the regular
  expression engine searches for the rest instead of running through the whole text.
* **`where`** ([qsql.rs](../src/engine/qsql.rs)) counts the rows a clause keeps and then writes their numbers without
  a branch per row.

From `930628b`, one run each:

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 0.96 s | **0.84 s** | 0.31 s | **0.28 s** | 1.1x | 1.00x |
| Cluster, 1 shard, in-process | 1.41 s | 1.27 s | 0.52 s | 0.52 s | 1.1x | 0.63x |
| Cluster, 3 shards, in-process | 1.03 s | 0.96 s | 0.44 s | 0.41 s | 1.1x | 0.82x |
| Cluster, 6 shards, in-process | 1.17 s | 1.13 s | 0.46 s | 0.42 s | 1.0x | 0.72x |
| Cluster, 12 shards, in-process | 1.67 s | 1.57 s | 0.50 s | 0.50 s | 1.0x | 0.54x |
| Cluster, 3 shard processes | 0.79 s | 0.77 s | 0.45 s | 0.46 s | 1.0x | 0.91x |
| Cluster, 6 shard processes | 0.79 s | **0.78 s** | 0.49 s | **0.46 s** | 1.0x | 0.90x |

One timing each (nanoseconds per row, on 1,800,000 rows of numbers or 450,000 of text); the timer counts whole
milliseconds, about 0.5 ns per row of numbers:

| Operation | Before | After |
| --- | ---: | ---: |
| one comparison, half the rows kept | 7 | 4 |
| one comparison, 1 row in 50 kept | 1 | 2 |
| four clauses, the shape of TPC-H Q6 | 11 | 6 |
| strings `like "*special*requests*"` | 200 | 127 |
| strings `like "forest*"` | 142 | 89 |
| strings `like "*green*"` | 216 | 107 |
| symbols `like`, 150 distinct | 84 | 31 |
| symbols `like`, all distinct | 71 | 22 |

A hand-written substring search for patterns whose only wildcard is `*` was tried and dropped: it was slower than the
regular expression engine on `"*green*"`.

### Typed join keys, and queries encoded once

* **Joins** ([joins.rs](../src/engine/joins.rs)) number the keys of both sides together, with the same typed code as
  grouping, instead of building a byte key in memory for every row. A key column with an attribute is still looked up in
  its index when there are fewer than a quarter as many rows to look up as it has keys, or when it is a general list.
* **The control node** encodes a query once for all shards, and unpacks a reply of more than 16,384 cells on another
  thread ([client.rs](../crates/parallel/control/src/client.rs)).

From `2d8e4ee` to `930628b`, one run each:

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 1.05 s | **0.96 s** | 0.35 s | **0.29 s** | 1.1x | 1.00x |
| Cluster, 1 shard, in-process | 1.52 s | 1.41 s | 0.60 s | 0.58 s | 1.1x | 0.63x |
| Cluster, 3 shards, in-process | 1.11 s | 1.01 s | 0.50 s | 0.45 s | 1.1x | 0.85x |
| Cluster, 6 shards, in-process | 1.28 s | 1.13 s | 0.51 s | 0.45 s | 1.1x | 0.79x |
| Cluster, 12 shards, in-process | 1.93 s | 1.69 s | 0.59 s | 0.49 s | 1.2x | 0.57x |
| Cluster, 3 shard processes | 0.92 s | 0.79 s | 0.54 s | 0.48 s | 1.2x | 0.98x |
| Cluster, 6 shard processes | 0.90 s | **0.78 s** | 0.53 s | **0.54 s** | 1.1x | 0.94x |

One timing each, on 1,800,000 rows (nanoseconds per row):

| Operation | Before | After |
| --- | ---: | ---: |
| `lj` to a 450,000-row result of `by`, keys at random | 251 | 27 |
| the same, keys in order | 70 | 27 |
| `lj` to a 450,000-row `xkey` table, keys at random | 407 | 26 |
| the same, keys in order | 218 | 23 |
| `ij` to a 450,000-row `xkey` table | 423 | 42 |
| `lj` on two key columns, 1,800,000-row table | 919 | 69 |
| `lj` to a 25-row result of `by` | 33 | 19 |
| `lj` to a 25-row `xkey` table | 110 | 23 |

Unpacking replies on other threads, against the same build unpacking them one at a time (six shards in one process,
milliseconds, averages of 40 requests in one run):

| Reply | One at a time | On other threads |
| --- | ---: | ---: |
| 600,000 rows of 4 number columns | 68.5 | 65.2 |
| 60,000 rows of 4 number columns | 7.1 | 6.1 |
| 200,000 rows with a string column | 85.9 | 70.6 |
| 20,000 rows with a string column | 5.6 | 5.5 |
| 1,000 groups from each shard | 3.7 | 3.8 |

With one shard, using other threads ranged from 13% faster to 8% slower.

### Slot tables for narrow ranges, and merges by column

* **`in`, grouping and `distinct`** now find integers, temporals, characters and symbols whose values span a narrow
  range in a table with one slot per value, instead of hashing them ([ops.rs](../src/engine/ops.rs)). The table has at
  most four slots per row (plus 256). Wider ranges, including any column containing integer nulls beside ordinary
  values, are hashed as before, as are floats.
* **The control node** groups the shards' partial results with the engine's typed grouping, aggregates them by column,
  and sorts merged keys with the typed sorts ([exec.rs](../crates/parallel/control/src/exec.rs),
  [merge.rs](../crates/parallel/control/src/merge.rs)).

From `404420b` to `2d8e4ee`, one run each:

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 1.66 s | **1.07 s** | 0.60 s | **0.38 s** | 1.6x | 1.00x |
| Cluster, 1 shard, in-process | 2.19 s | 1.58 s | 0.85 s | 0.63 s | 1.4x | 0.65x |
| Cluster, 3 shards, in-process | 1.43 s | 1.17 s | 0.66 s | 0.52 s | 1.2x | 0.85x |
| Cluster, 6 shards, in-process | 1.56 s | 1.29 s | 0.66 s | 0.50 s | 1.2x | 0.81x |
| Cluster, 12 shards, in-process | 2.11 s | 1.82 s | 0.74 s | 0.58 s | 1.2x | 0.60x |
| Cluster, 3 shard processes | 1.23 s | 0.95 s | 0.67 s | 0.55 s | 1.3x | 0.97x |
| Cluster, 6 shard processes | 1.14 s | **0.91 s** | 0.68 s | **0.56 s** | 1.2x | 0.99x |

One timing each, on 1,800,000 rows (nanoseconds per row):

| Operation | Before | After |
| --- | ---: | ---: |
| long `in` 10 longs | 14 | 2 |
| long `in` 100,000 longs, narrow range | 24 | 3 |
| long `in` 100,000 longs, wide range | 24 | 24 |
| group by two symbols | 46 | 14 |
| group by long, 450,000 groups | 192 | 84 |
| group by date and symbol | 57 | 18 |
| `distinct`, 450,000 longs | 112 | 12 |

Merging on six shards in one process (milliseconds, averages of 5 requests in one run; the "after" figures also gain
from the faster grouping on the shards):

| Query | Single node before / after | Cluster before / after |
| --- | ---: | ---: |
| `sum ... by`, about 95,000 groups, two-phase | 40 / 24 | 146 / 43 |
| `sum ... by` the distribution column, 100,000 groups | 19 / 11 | 12 / 9 |

### Aggregates are computed for all groups at once

`select ... by` and `exec ... by` used to copy each group's rows and evaluate every column expression on the copy. Now a
column made of aggregates (`sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `med`, `prd`, `var`, `dev`, `svar`,
`sdev`, `wsum`, `wavg`), combined with item-by-item verbs and constants, is computed for all groups together
([qsql.rs](../src/engine/qsql.rs)). Each aggregate's argument is evaluated once over all rows, and each group then
aggregates its own values. The argument must depend on its own row only (columns, `i`, temporal fields, item-by-item
verbs, `in`, `within`), possibly under `distinct`. Other columns, and any that signal an error, are still evaluated
group by group. Groups are also found and sorted faster.

From `77c9682` to `3c00f98`, medians of five runs:

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 2.97 s | **1.62 s** | 1.16 s | **0.58 s** | 1.9x | 1.00x |
| Cluster, 1 shard, in-process | 3.43 s | 2.10 s | 1.43 s | 0.91 s | 1.6x | 0.73x |
| Cluster, 3 shards, in-process | 1.98 s | 1.40 s | 0.97 s | 0.63 s | 1.5x | 1.09x |
| Cluster, 6 shards, in-process | 1.79 s | 1.42 s | 0.90 s | 0.63 s | 1.3x | 1.08x |
| Cluster, 12 shards, in-process | 2.41 s | 1.97 s | 0.92 s | 0.70 s | 1.2x | 0.82x |
| Cluster, 3 shard processes | 1.74 s | 1.20 s | 0.94 s | 0.67 s | 1.4x | 1.18x |
| Cluster, 6 shard processes | 1.46 s | **1.09 s** | 0.90 s | **0.66 s** | 1.3x | 1.26x |

Milliseconds on 1,800,786 rows (median of three, five repetitions each):

| `by` query | Groups | Before | After |
| --- | ---: | ---: | ---: |
| `sum x` | 440,000 | 953 | 376 |
| `sum (x * 1 - d) - y * q` | 440,000 | 1843 | 400 |
| `sum x, avg y, min d, count i, first f` | 440,000 | 2016 | 797 |
| `(sum x * f = `A) % sum x` | 440,000 | 1470 | 471 |
| `count distinct f` | 440,000 | 1196 | 617 |
| `sum x`, two keys | 990,000 | 2500 | 904 |
| `sum x`, symbol key | 20,000 | 164 | 95 |
| `sum x, avg y, count i` | 3 | 100 | 64 |
| `sums x` (not an aggregate) | 440,000 | 1024 | 815 |

## First change: columns, operators and casts

From `a06fb4b` to `77c9682`:

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 9.76 s | **3.01 s** | 2.71 s | **1.18 s** | 3.0x | 1.00x |
| Cluster, 1 shard, in-process | 10.04 s | 3.66 s | 2.95 s | 1.48 s | 2.5x | 0.82x |
| Cluster, 3 shards, in-process | 4.94 s | 1.98 s | 1.70 s | 0.95 s | 2.3x | 1.43x |
| Cluster, 6 shards, in-process | 4.15 s | 1.77 s | 1.53 s | 0.92 s | 2.1x | 1.55x |
| Cluster, 12 shards, in-process | 4.32 s | 2.46 s | 1.42 s | 0.92 s | 1.7x | 1.24x |
| Cluster, 3 shard processes | 4.64 s | 1.71 s | 1.76 s | 0.96 s | 2.4x | 1.57x |
| Cluster, 6 shard processes | 3.59 s | **1.46 s** | 1.48 s | **0.92 s** | 2.1x | 1.76x |

* **Queries copy only the columns they read.** `where` used to copy every column of the kept rows, for each clause after
  the first and again for the result, and grouping copied them again for each group. A query that can name columns at
  run time (through `value`, `get`, `eval`, `reval`, `system`, a view or a variable holding a verb) still sees them all.
* **Typed loops instead of a value per row** ([ops.rs](../src/engine/ops.rs)): comparisons, `within` and float
  arithmetic run over typed vectors; `in`, grouping and `distinct` compare fixed-width values as bits.
* **Constant casts are pushed down.** The control node computes casts such as `` `$"Brand#23" `` while planning
  ([plan.rs](../crates/parallel/control/src/plan.rs)), so such filters run on the shards and can route like literals.
  TPC-H Q8, Q16, Q17 and Q19 no longer gather any table.

Nanoseconds per row on 1,800,786 rows (median of three, ten repetitions each):

| Operation | Before | After |
| --- | ---: | ---: |
| long `<` long | 0.7 | 0.9 |
| symbol `=` symbol | 1.5 | 0.3 |
| date `>=` date | 27 | 0.9 |
| date `within` | 11 | 2.1 |
| float `<` float | 12 | 1.4 |
| float `<` long | 43 | 1.4 |
| char `=` char | 25 | 0.8 |
| symbol `in` two symbols | 60 | 4.7 |
| long `-` float | 30 | 1.9 |
| `count i` with a `where` that keeps every row | 88 | 8.9 |
| `sum ... by`, 3 groups | 206 | 36 |
| `sum ... by`, 450,000 groups | 796 | 255 |

## Per query (milliseconds)

Single runs. Cluster columns other than "6 proc before" are after the latest change. Each gain is before divided by
after.

### TPC-H

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 128 | 134 | 1.0x | 140 | 67 | 56 | 61 | 65 | 55 | 50 | 1.1x |
| Q2 | 3 | 3 | 1.0x | 7 | 7 | 9 | 13 | 6 | 6 | 7 | 0.9x |
| Q3 | 28 | 26 | 1.1x | 44 | 30 | 43 | 76 | 21 | 24 | 21 | 1.1x |
| Q4 | 14 | 14 | 1.0x | 23 | 18 | 25 | 41 | 15 | 18 | 15 | 1.2x |
| Q5 | 16 | 17 | 0.9x | 31 | 26 | 39 | 62 | 18 | 23 | 20 | 1.1x |
| Q6 | 15 | 14 | 1.1x | 16 | 7 | 7 | 11 | 7 | 6 | 6 | 1.0x |
| Q7 | 23 | 27 | 0.9x | 39 | 31 | 41 | 70 | 22 | 26 | 26 | 1.0x |
| Q8 | 16 | 16 | 1.0x | 29 | 24 | 38 | 60 | 17 | 22 | 20 | 1.1x |
| Q9 | 158 | 158 | 1.0x | 258 | 212 | 231 | 314 | 156 | 173 | 176 | 1.0x |
| Q10 | 46 | 45 | 1.0x | 126 | 98 | 130 | 182 | 79 | 89 | 86 | 1.0x |
| Q11 | 3 | 3 | 1.0x | 5 | 5 | 6 | 6 | 3 | 3 | 3 | 1.0x |
| Q12 | 39 | 39 | 1.0x | 48 | 23 | 25 | 41 | 23 | 22 | 19 | 1.2x |
| Q13 | 59 | 59 | 1.0x | 64 | 42 | 49 | 88 | 36 | 34 | 31 | 1.1x |
| Q14 | 10 | 10 | 1.0x | 13 | 10 | 13 | 16 | 6 | 6 | 6 | 1.0x |
| Q15 | 10 | 10 | 1.0x | 16 | 13 | 18 | 31 | 10 | 9 | 9 | 1.0x |
| Q16 | 18 | 19 | 0.9x | 27 | 32 | 40 | 52 | 21 | 21 | 21 | 1.0x |
| Q17 | 7 | 6 | 1.2x | 11 | 7 | 7 | 8 | 6 | 7 | 6 | 1.2x |
| Q18 | 108 | 111 | 1.0x | 162 | 103 | 111 | 134 | 74 | 102 | 76 | 1.3x |
| Q19 | 13 | 12 | 1.1x | 15 | 9 | 10 | 14 | 8 | 7 | 7 | 1.0x |
| Q20 | 21 | 21 | 1.0x | 23 | 13 | 12 | 20 | 11 | 11 | 10 | 1.1x |
| Q21 | 73 | 73 | 1.0x | 91 | 66 | 69 | 108 | 48 | 52 | 46 | 1.1x |
| Q22 | 32 | 31 | 1.0x | 61 | 83 | 96 | 133 | 58 | 67 | 68 | 1.0x |

### TPC-DS subset

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 7 | 7 | 1.0x | 31 | 13 | 14 | 15 | 14 | 15 | 16 | 0.9x |
| Q3 | 3 | 2 | 1.5x | 5 | 4 | 4 | 5 | 5 | 4 | 5 | 0.8x |
| Q6 | 6 | 7 | 0.9x | 18 | 15 | 15 | 16 | 16 | 15 | 16 | 0.9x |
| Q7 | 5 | 5 | 1.0x | 10 | 8 | 7 | 8 | 10 | 8 | 8 | 1.0x |
| Q12 | 2 | 3 | 0.7x | 7 | 6 | 6 | 7 | 6 | 6 | 6 | 1.0x |
| Q15 | 7 | 5 | 1.4x | 14 | 13 | 13 | 15 | 15 | 29 | 14 | 2.1x |
| Q19 | 6 | 5 | 1.2x | 17 | 14 | 18 | 14 | 15 | 15 | 15 | 1.0x |
| Q20 | 2 | 2 | 1.0x | 10 | 5 | 5 | 6 | 5 | 6 | 5 | 1.2x |
| Q22 | 15 | 16 | 0.9x | 47 | 47 | 55 | 83 | 71 | 65 | 65 | 1.0x |
| Q26 | 2 | 3 | 0.7x | 9 | 6 | 6 | 6 | 6 | 6 | 6 | 1.0x |
| Q27 | 6 | 6 | 1.0x | 16 | 15 | 16 | 21 | 17 | 18 | 18 | 1.0x |
| Q36 | 9 | 10 | 0.9x | 21 | 23 | 26 | 42 | 25 | 29 | 28 | 1.0x |
| Q38 | 65 | 43 | 1.5x | 68 | 53 | 51 | 52 | 81 | 78 | 53 | 1.5x |
| Q42 | 2 | 2 | 1.0x | 4 | 3 | 3 | 3 | 3 | 3 | 3 | 1.0x |
| Q43 | 9 | 9 | 1.0x | 12 | 10 | 8 | 11 | 10 | 9 | 9 | 1.0x |
| Q50 | 30 | 31 | 1.0x | 41 | 25 | 21 | 22 | 27 | 24 | 27 | 0.9x |
| Q52 | 2 | 2 | 1.0x | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 1.0x |
| Q55 | 2 | 2 | 1.0x | 4 | 2 | 3 | 4 | 3 | 3 | 3 | 1.0x |
| Q69 | 4 | 5 | 0.8x | 13 | 8 | 8 | 10 | 9 | 9 | 8 | 1.1x |
| Q87 | 57 | 44 | 1.3x | 70 | 54 | 53 | 55 | 64 | 67 | 55 | 1.2x |
| Q96 | 2 | 2 | 1.0x | 3 | 3 | 2 | 2 | 3 | 2 | 3 | 0.7x |
| Q98 | 6 | 6 | 1.0x | 10 | 9 | 8 | 8 | 9 | 8 | 8 | 1.0x |
| Q11 | 30 | 32 | 0.9x | 47 | 31 | 33 | 37 | 39 | 36 | 38 | 0.9x |
| Q51 | 1 | 1 | 1.0x | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1.0x |

## Where the time goes

Temporary timers in a copy of the source, one run each. Neither profile includes the latest change.

### Single node

| Work | TPC-H, 0.84 s (0.94 s) | TPC-DS, 0.29 s (0.29 s) |
| --- | ---: | ---: |
| Other `where` work (comparisons, `like`, copying kept rows) | 23% (32%) | 8% (9%) |
| Aggregating, group by group or all at once | 21% (18%) | 26% (26%) |
| Finding groups | 16% (14%) | 15% (15%) |
| `in` | 12% (11%) | 12% (12%) |
| Joins | 6% (5%) | 7% (7%) |
| Everything else | 22% (20%) | 32% (31%) |

Figures in parentheses are from before the filtering change. On TPC-H, `like` fell from 115 to 63 ms and turning
results into row numbers from 99 to 50 ms. Copying the kept rows for the next clause takes 40 ms; a different copy loop
made no measurable difference and was not kept.

### Cluster

From the profile of the join change; the filtering change was not profiled on the cluster.

| Time (ms) | TPC-H before | after | TPC-DS before | after |
| --- | ---: | ---: | ---: | ---: |
| Single node, same run | 1106 | 1011 | 325 | 262 |
| **Cluster** | **1287** | **1180** | **502** | **439** |
| Slowest shard of each request: decoding it, running it, packing the result | 611 | 624 | 124 | 112 |
| Rest of each request: transport and unpacking; all of a load or a shuffle | 273 | 243 | 147 | 151 |
| Control node: merging shard results | 135 | 136 | 38 | 38 |
| Control node: running query parts itself (joins, grouping, `distinct`) | 206 | 110 | 152 | 94 |
| Control node: parsing, planning, redistributing assigned tables | 13 | 13 | 27 | 28 |
| Not timed | 48 | 55 | 14 | 16 |

* **Encoding requests** fell from 51 to 5 ms on TPC-H.
* **Transport and unpacking** remain the largest cost after the shards' own work.
* **Merging** takes 136 ms on TPC-H: mostly merging by key and putting rows back in order.
* **Assigned tables travel twice.** A table assigned to a variable in a multi-statement query is sent to the shards, and
  later statements fetch it back (DS Q22 and Q36).
* **TPC-H Q22** is slower on the cluster than on the single node: `` `$2#'c_phone `` is not pushed down, so `customer`
  is gathered.

## Recommended next steps

None of the gains has been measured.

1. Profile aggregation, now the largest part of TPC-DS and close to `where` on TPC-H.
2. Profile the control node's merging by key and ordering of rows.
3. Keep intermediate tables on the control node. This changes where assigned tables live, so it needs a design decision.

## Cost of TLS between nodes

Measured after mutual TLS was added: six shards in one process on one machine, one run each, with everything else as above.

| | Unprotected | Mutual TLS |
| --- | ---: | ---: |
| TPC-H, load | 4.17 s | 4.20 s |
| TPC-H, 22 queries | 1.08 s | 1.07 s |
| TPC-DS, load | 0.78 s | 0.77 s |
| TPC-DS, 24 queries | 0.39 s | 0.39 s |

No difference was measurable. A network between machines was not tested.

## Not measured

Durability (`--durable`, `--data`), replicated shards, client TLS, more than one machine, concurrent load, and any comparison
with another database.

## Reproducing

```
cargo build --release --example tpc
cargo build --release -p kix-control --examples
cargo build --release -p kix-storage --bin kix-shard
target/release/examples/tpc --sf 0.3
target/release/examples/tpch_cluster --sf 0.3 --shards 6
target/release/examples/tpcds_cluster --sf 0.3 --shards 6
```

For shard processes, start `kix-shard --id N --listen 127.0.0.1:PORT --insecure` for each shard (the benchmark
programs connect without TLS), list them in a file such as
`{"shards":[{"id":0,"addr":"127.0.0.1:52050"}]}`, and pass `--config FILE` to the cluster program. Restart the shards
between runs.
