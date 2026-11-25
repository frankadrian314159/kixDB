# Benchmark results: scale factor 0.3

Measured 2026-11-25 with the programs described in [benchmarks.md](benchmarks.md). Four changes were measured, each
against the build before it. **Before** below is commit `2d8e4ee`; **after** matches join keys as typed columns,
encodes a query once for all shards, and unpacks large replies on other threads. Earlier: [slot tables and
merges](#slot-tables-for-narrow-ranges-and-merges-by-column), [aggregates for all groups at
once](#aggregates-are-computed-for-all-groups-at-once) and [columns, operators and
casts](#first-change-columns-operators-and-casts).

## Summary

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 1.05 s | **0.96 s** | 0.35 s | **0.29 s** | 1.1x | 1.00x |
| Cluster, 1 shard, in-process | 1.52 s | 1.41 s | 0.60 s | 0.58 s | 1.1x | 0.63x |
| Cluster, 3 shards, in-process | 1.11 s | 1.01 s | 0.50 s | 0.45 s | 1.1x | 0.85x |
| Cluster, 6 shards, in-process | 1.28 s | 1.13 s | 0.51 s | 0.45 s | 1.1x | 0.79x |
| Cluster, 12 shards, in-process | 1.93 s | 1.69 s | 0.59 s | 0.49 s | 1.2x | 0.57x |
| Cluster, 3 shard processes | 0.92 s | 0.79 s | 0.54 s | 0.48 s | 1.2x | 0.98x |
| Cluster, 6 shard processes | 0.90 s | **0.78 s** | 0.53 s | **0.54 s** | 1.1x | 0.94x |

Each figure in this table is from a single run.

* **The single node is 1.1x faster** (1.40 s to 1.25 s for both suites), and the cluster setups 1.07x to 1.16x. Since
  `a06fb4b` (12.47 s on one node, 5.07 s on six shard processes) that is about 10x and 3.8x.
* **Joins on their own are 1.7 to 16 times faster**, but were 14% (TPC-H) and 23% (TPC-DS) of single-node time.
* **The cluster is still not ahead overall:** six shard processes take 1.32 s against 1.25 s on one node.
* **Slower queries:** 13 of the 322 query and setup combinations were more than 5% (plus 1 ms) slower. Among the
  largest were DS Q38 (95 to 137 ms), Q43 (11 to 17 ms) and Q15 (20 to 27 ms) with six shard processes, and H Q3 (51
  to 65 ms) and DS Q22 (43 to 54 ms) with one shard. A separate run with six shards in one process showed no query
  more than 8% (plus 3 ms) slower. The cause of these differences is not established.
* **Correctness:** every cluster answer in the 28 runs matched the single node's. Before and after gave identical result
  rows for all 46 queries on the single node, and identical output for 45,540 generated joins, 30,153 grouped queries
  and (results or error kinds) 69,692 operator expressions.

## Setup

* AMD Ryzen 5 7430U (6 cores, 12 threads), 31 GB RAM, Windows 11, on mains power. Release build, rustc 1.95.
* In memory: no write-ahead log, no replication, no TLS. Shards and control node on one machine over loopback TCP.
* One client running one query at a time. This measures latency, not throughput under concurrency.
* The last two changes were run once per build and setup, old and new within minutes of each other. Earlier changes
  report medians of five interleaved runs. Repetitions then mostly agreed within 5%, the worst by 12% (47% in the first
  session), so treat differences under about 20% between single runs as inconclusive.
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

### Typed join keys, and queries encoded once

* **Joins** ([joins.rs](../src/engine/joins.rs)) number the keys of both sides together, with the same typed code as
  grouping, instead of building a byte key in memory for every row. A key column with an attribute is still looked up in
  its index when there are fewer than a quarter as many rows to look up as it has keys, or when it is a general list.
* **The control node** encodes a query once for all shards, and unpacks a reply of more than 16,384 cells on another
  thread ([client.rs](../crates/parallel/control/src/client.rs)).

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
| Q1 | 132 | 134 | 1.0x | 140 | 71 | 56 | 64 | 66 | 53 | 55 | 1.0x |
| Q2 | 3 | 3 | 1.0x | 6 | 7 | 10 | 14 | 6 | 7 | 7 | 1.0x |
| Q3 | 33 | 31 | 1.1x | 65 | 34 | 47 | 83 | 25 | 23 | 29 | 0.8x |
| Q4 | 16 | 16 | 1.0x | 24 | 23 | 28 | 53 | 18 | 19 | 18 | 1.1x |
| Q5 | 20 | 19 | 1.1x | 34 | 32 | 42 | 81 | 22 | 24 | 24 | 1.0x |
| Q6 | 22 | 21 | 1.0x | 23 | 10 | 11 | 19 | 9 | 7 | 7 | 1.0x |
| Q7 | 29 | 27 | 1.1x | 46 | 36 | 45 | 72 | 26 | 28 | 25 | 1.1x |
| Q8 | 18 | 18 | 1.0x | 32 | 29 | 40 | 64 | 21 | 23 | 23 | 1.0x |
| Q9 | 249 | 164 | 1.5x | 273 | 223 | 231 | 306 | 167 | 262 | 176 | 1.5x |
| Q10 | 54 | 50 | 1.1x | 135 | 104 | 129 | 216 | 85 | 105 | 87 | 1.2x |
| Q11 | 3 | 3 | 1.0x | 5 | 5 | 5 | 6 | 3 | 3 | 3 | 1.0x |
| Q12 | 52 | 53 | 1.0x | 66 | 36 | 36 | 64 | 29 | 23 | 23 | 1.0x |
| Q13 | 99 | 95 | 1.0x | 101 | 56 | 58 | 96 | 47 | 47 | 39 | 1.2x |
| Q14 | 15 | 15 | 1.0x | 17 | 12 | 13 | 20 | 8 | 9 | 8 | 1.1x |
| Q15 | 15 | 13 | 1.2x | 19 | 15 | 19 | 35 | 10 | 11 | 11 | 1.0x |
| Q16 | 22 | 22 | 1.0x | 30 | 33 | 40 | 54 | 22 | 25 | 20 | 1.2x |
| Q17 | 6 | 8 | 0.8x | 16 | 9 | 9 | 11 | 9 | 8 | 8 | 1.0x |
| Q18 | 108 | 110 | 1.0x | 156 | 103 | 104 | 135 | 73 | 82 | 79 | 1.0x |
| Q19 | 15 | 15 | 1.0x | 16 | 11 | 14 | 18 | 10 | 7 | 8 | 0.9x |
| Q20 | 30 | 30 | 1.0x | 31 | 17 | 18 | 29 | 15 | 12 | 13 | 0.9x |
| Q21 | 78 | 77 | 1.0x | 100 | 68 | 78 | 119 | 59 | 50 | 50 | 1.0x |
| Q22 | 31 | 32 | 1.0x | 75 | 80 | 98 | 132 | 61 | 67 | 69 | 1.0x |

### TPC-DS subset

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 8 | 8 | 1.0x | 21 | 12 | 14 | 15 | 13 | 17 | 15 | 1.1x |
| Q3 | 3 | 3 | 1.0x | 5 | 4 | 4 | 5 | 4 | 5 | 5 | 1.0x |
| Q6 | 9 | 7 | 1.3x | 23 | 15 | 15 | 16 | 16 | 19 | 15 | 1.3x |
| Q7 | 6 | 6 | 1.0x | 12 | 10 | 8 | 8 | 9 | 8 | 8 | 1.0x |
| Q12 | 3 | 3 | 1.0x | 7 | 6 | 6 | 7 | 6 | 7 | 6 | 1.2x |
| Q15 | 9 | 6 | 1.5x | 14 | 14 | 14 | 15 | 15 | 20 | 27 | 0.7x |
| Q19 | 9 | 5 | 1.8x | 16 | 16 | 14 | 15 | 15 | 18 | 15 | 1.2x |
| Q20 | 3 | 2 | 1.5x | 6 | 5 | 6 | 6 | 5 | 6 | 5 | 1.2x |
| Q22 | 16 | 15 | 1.1x | 54 | 54 | 75 | 84 | 58 | 69 | 64 | 1.1x |
| Q26 | 3 | 3 | 1.0x | 7 | 6 | 6 | 6 | 6 | 8 | 7 | 1.1x |
| Q27 | 5 | 6 | 0.8x | 18 | 16 | 16 | 20 | 18 | 19 | 18 | 1.1x |
| Q36 | 10 | 9 | 1.1x | 24 | 22 | 27 | 41 | 22 | 28 | 28 | 1.0x |
| Q38 | 82 | 67 | 1.2x | 119 | 80 | 73 | 76 | 89 | 95 | 137 | 0.7x |
| Q42 | 2 | 2 | 1.0x | 4 | 3 | 3 | 3 | 3 | 3 | 4 | 0.8x |
| Q43 | 9 | 9 | 1.0x | 14 | 9 | 10 | 10 | 10 | 11 | 17 | 0.6x |
| Q50 | 41 | 30 | 1.4x | 42 | 26 | 19 | 21 | 27 | 35 | 30 | 1.2x |
| Q52 | 2 | 2 | 1.0x | 4 | 4 | 3 | 3 | 4 | 3 | 3 | 1.0x |
| Q55 | 2 | 2 | 1.0x | 4 | 3 | 3 | 3 | 3 | 3 | 3 | 1.0x |
| Q69 | 5 | 5 | 1.0x | 12 | 9 | 8 | 10 | 9 | 10 | 10 | 1.0x |
| Q87 | 85 | 58 | 1.5x | 97 | 82 | 71 | 70 | 95 | 94 | 71 | 1.3x |
| Q96 | 2 | 2 | 1.0x | 3 | 2 | 3 | 2 | 2 | 2 | 2 | 1.0x |
| Q98 | 6 | 6 | 1.0x | 10 | 8 | 8 | 9 | 9 | 8 | 9 | 0.9x |
| Q11 | 32 | 32 | 1.0x | 58 | 37 | 37 | 39 | 36 | 41 | 40 | 1.0x |
| Q51 | 2 | 1 | 2.0x | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1.0x |

## Where the time goes

Profile after the latest change: temporary timers in a copy of the source, one run each, the cluster as six shards in
one process. Figures in parentheses, and "before" columns, are from the same profile before the change.

### Single node

| Work | TPC-H, 0.96 s (1.11 s) | TPC-DS, 0.29 s (0.36 s) |
| --- | ---: | ---: |
| Other `where` work (comparisons, `like`, copying kept rows) | 31% (28%) | 9% (8%) |
| Aggregating, group by group or all at once | 18% (20%) | 26% (21%) |
| Finding groups | 14% (12%) | 15% (13%) |
| `in` | 11% (9%) | 12% (10%) |
| Joins | 5% (14%) | 7% (23%) |
| Everything else | 20% (17%) | 31% (26%) |

Joins went from 142 to 43 ms in H Q9 and from 33 to 9 ms in DS Q87. `like` is most of H Q13's 88 ms of `where` work.

### Cluster

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

1. Profile what remains of `where`, starting with `like`.
2. Compute constant columns for all groups at once: `select ok: 1b by ...`, which several TPC-DS queries use, is still
   evaluated group by group.
3. Profile the control node's merging by key and ordering of rows.
4. Keep intermediate tables on the control node. This changes where assigned tables live, so it needs a design decision.

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
