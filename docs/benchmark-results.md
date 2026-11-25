# Benchmark results: scale factor 0.3

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

## Not measured

Durability (`--durable`, `--data`), replicated shards, TLS, more than one machine, concurrent load, and any comparison
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

For shard processes, start `kix-shard --id N --listen 127.0.0.1:PORT` for each shard, list them in a file such as
`{"shards":[{"id":0,"addr":"127.0.0.1:52050"}]}`, and pass `--config FILE` to the cluster program. Restart the shards
between runs.
