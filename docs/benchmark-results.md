# Benchmark results: scale factor 0.3

Measured 2026-11-25 with the programs described in [benchmarks.md](benchmarks.md). Three changes were measured, each
against the build before it. **Before** below is commit `404420b`; **after** finds narrow ranges of values in slot
tables and merges shard results by whole columns. Earlier: [aggregates for all groups at
once](#aggregates-are-computed-for-all-groups-at-once) and [columns, operators and
casts](#first-change-columns-operators-and-casts).

## Summary

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 1.66 s | **1.07 s** | 0.60 s | **0.38 s** | 1.6x | 1.00x |
| Cluster, 1 shard, in-process | 2.19 s | 1.58 s | 0.85 s | 0.63 s | 1.4x | 0.65x |
| Cluster, 3 shards, in-process | 1.43 s | 1.17 s | 0.66 s | 0.52 s | 1.2x | 0.85x |
| Cluster, 6 shards, in-process | 1.56 s | 1.29 s | 0.66 s | 0.50 s | 1.2x | 0.81x |
| Cluster, 12 shards, in-process | 2.11 s | 1.82 s | 0.74 s | 0.58 s | 1.2x | 0.60x |
| Cluster, 3 shard processes | 1.23 s | 0.95 s | 0.67 s | 0.55 s | 1.3x | 0.97x |
| Cluster, 6 shard processes | 1.14 s | **0.91 s** | 0.68 s | **0.56 s** | 1.2x | 0.99x |

Each figure in this table is from a single run.

* **The single node is 1.6x faster** (2.27 s to 1.45 s for both suites), and six shard processes 1.2x (1.82 s to
  1.47 s). Since `a06fb4b` (12.47 s and 5.07 s) that is about 8.6x and 3.4x.
* **The cluster is no longer ahead overall.** Six shard processes now take as long as the single node: 1.2x as fast on
  TPC-H, 0.7x on TPC-DS.
* **Slower queries:** nine of the 322 query and setup combinations were more than 5% (plus 1 ms) slower. Eight differ
  by 2 to 6 ms. DS Q15 with six shard processes went from 24 to 41 ms, and was not slower by that margin in any other
  setup. With one run each, the cause of such differences is not established.
* **Correctness:** every cluster answer in the 14 runs matched the single node's. Before and after gave identical result
  rows for all 46 queries on the single node. They also agreed on 30,153 generated grouped queries, and on the results
  or error kinds of 69,692 generated operator expressions.

## Setup

* AMD Ryzen 5 7430U (6 cores, 12 threads), 31 GB RAM, Windows 11, on mains power. Release build, rustc 1.95.
* In memory: no write-ahead log, no replication, no TLS. Shards and control node on one machine over loopback TCP.
* One client running one query at a time. This measures latency, not throughput under concurrency.
* The latest change was run once per build and setup, the new build a few minutes after the old one. Earlier changes
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

### Slot tables for narrow ranges, and merges by column

* **`in`, grouping and `distinct`** now find integers, temporals, characters and symbols whose values span a narrow
  range in a table with one slot per value, instead of hashing them ([ops.rs](../src/engine/ops.rs)). The table has at
  most four slots per row (plus 256). Wider ranges, including any column containing integer nulls beside ordinary
  values, are hashed as before, as are floats.
* **The control node** groups the shards' partial results with the engine's typed grouping, aggregates them by column,
  and sorts merged keys with the typed sorts ([exec.rs](../crates/parallel/control/src/exec.rs),
  [merge.rs](../crates/parallel/control/src/merge.rs)).

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
| Q1 | 194 | 134 | 1.4x | 150 | 86 | 67 | 64 | 69 | 66 | 56 | 1.2x |
| Q2 | 7 | 3 | 2.3x | 8 | 8 | 10 | 16 | 6 | 8 | 6 | 1.3x |
| Q3 | 57 | 32 | 1.8x | 72 | 34 | 50 | 93 | 25 | 33 | 23 | 1.4x |
| Q4 | 44 | 16 | 2.8x | 25 | 28 | 35 | 49 | 19 | 22 | 18 | 1.2x |
| Q5 | 60 | 21 | 2.9x | 35 | 33 | 44 | 77 | 27 | 34 | 25 | 1.4x |
| Q6 | 21 | 21 | 1.0x | 23 | 10 | 14 | 19 | 13 | 7 | 7 | 1.0x |
| Q7 | 54 | 28 | 1.9x | 60 | 41 | 50 | 84 | 28 | 34 | 29 | 1.2x |
| Q8 | 57 | 19 | 3.0x | 38 | 30 | 43 | 83 | 23 | 32 | 21 | 1.5x |
| Q9 | 345 | 261 | 1.3x | 389 | 308 | 329 | 398 | 271 | 312 | 264 | 1.2x |
| Q10 | 68 | 52 | 1.3x | 134 | 126 | 145 | 205 | 102 | 115 | 106 | 1.1x |
| Q11 | 6 | 3 | 2.0x | 6 | 5 | 6 | 7 | 3 | 4 | 3 | 1.3x |
| Q12 | 59 | 54 | 1.1x | 69 | 35 | 37 | 59 | 32 | 23 | 23 | 1.0x |
| Q13 | 113 | 100 | 1.1x | 106 | 58 | 57 | 96 | 49 | 95 | 41 | 2.3x |
| Q14 | 15 | 15 | 1.0x | 17 | 13 | 15 | 21 | 13 | 9 | 8 | 1.1x |
| Q15 | 15 | 14 | 1.1x | 20 | 15 | 21 | 36 | 11 | 13 | 10 | 1.3x |
| Q16 | 28 | 22 | 1.3x | 32 | 36 | 42 | 59 | 24 | 27 | 23 | 1.2x |
| Q17 | 27 | 7 | 3.9x | 16 | 9 | 8 | 9 | 9 | 14 | 8 | 1.8x |
| Q18 | 220 | 108 | 2.0x | 145 | 105 | 106 | 137 | 79 | 108 | 79 | 1.4x |
| Q19 | 17 | 14 | 1.2x | 17 | 15 | 14 | 18 | 8 | 8 | 8 | 1.0x |
| Q20 | 36 | 30 | 1.2x | 31 | 17 | 18 | 28 | 19 | 15 | 12 | 1.2x |
| Q21 | 172 | 80 | 2.1x | 104 | 71 | 77 | 127 | 55 | 80 | 52 | 1.5x |
| Q22 | 48 | 33 | 1.5x | 87 | 91 | 104 | 139 | 63 | 85 | 90 | 0.9x |

### TPC-DS subset

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 11 | 8 | 1.4x | 24 | 16 | 14 | 16 | 17 | 21 | 16 | 1.3x |
| Q3 | 11 | 3 | 3.7x | 6 | 5 | 5 | 5 | 5 | 7 | 5 | 1.4x |
| Q6 | 20 | 10 | 2.0x | 23 | 21 | 19 | 20 | 25 | 23 | 19 | 1.2x |
| Q7 | 14 | 8 | 1.8x | 14 | 10 | 8 | 8 | 11 | 11 | 9 | 1.2x |
| Q12 | 5 | 3 | 1.7x | 8 | 6 | 7 | 7 | 7 | 7 | 6 | 1.2x |
| Q15 | 14 | 9 | 1.6x | 21 | 17 | 17 | 20 | 19 | 24 | 41 | 0.6x |
| Q19 | 18 | 9 | 2.0x | 19 | 21 | 18 | 18 | 22 | 22 | 20 | 1.1x |
| Q20 | 6 | 2 | 3.0x | 6 | 7 | 5 | 6 | 5 | 6 | 6 | 1.0x |
| Q22 | 27 | 15 | 1.8x | 46 | 49 | 63 | 86 | 58 | 67 | 65 | 1.0x |
| Q26 | 6 | 3 | 2.0x | 8 | 7 | 6 | 6 | 6 | 8 | 6 | 1.3x |
| Q27 | 14 | 7 | 2.0x | 18 | 17 | 18 | 21 | 18 | 23 | 18 | 1.3x |
| Q36 | 21 | 10 | 2.1x | 25 | 23 | 29 | 41 | 24 | 31 | 29 | 1.1x |
| Q38 | 113 | 83 | 1.4x | 130 | 92 | 92 | 99 | 109 | 112 | 94 | 1.2x |
| Q42 | 8 | 2 | 4.0x | 4 | 3 | 3 | 4 | 3 | 5 | 3 | 1.7x |
| Q43 | 21 | 10 | 2.1x | 13 | 11 | 10 | 11 | 10 | 24 | 11 | 2.2x |
| Q50 | 51 | 41 | 1.2x | 50 | 37 | 31 | 35 | 40 | 37 | 37 | 1.0x |
| Q52 | 9 | 2 | 4.5x | 3 | 3 | 3 | 3 | 4 | 5 | 3 | 1.7x |
| Q55 | 9 | 2 | 4.5x | 4 | 4 | 3 | 3 | 3 | 5 | 3 | 1.7x |
| Q69 | 18 | 5 | 3.6x | 13 | 9 | 9 | 11 | 10 | 13 | 10 | 1.3x |
| Q87 | 114 | 106 | 1.1x | 125 | 110 | 94 | 100 | 98 | 107 | 94 | 1.1x |
| Q96 | 9 | 2 | 4.5x | 3 | 3 | 2 | 2 | 3 | 4 | 3 | 1.3x |
| Q98 | 13 | 6 | 2.2x | 10 | 9 | 8 | 9 | 9 | 11 | 9 | 1.2x |
| Q11 | 70 | 33 | 2.1x | 51 | 40 | 38 | 42 | 39 | 100 | 48 | 2.1x |
| Q51 | 1 | 2 | 0.5x | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 1.0x |

## Where the time goes

Profile after the latest change: temporary timers in a copy of the source, one run each, the cluster as six shards in
one process. Figures in parentheses are from the same profile before the change.

### Single node

| Work | TPC-H, 1.11 s (1.68 s) | TPC-DS, 0.36 s (0.59 s) |
| --- | ---: | ---: |
| Other `where` work (comparisons, `like`, copying kept rows) | 28% (18%) | 8% (5%) |
| Aggregating, group by group or all at once | 20% (10%) | 21% (12%) |
| Joins | 14% (9%) | 23% (13%) |
| Finding groups | 12% (22%) | 13% (20%) |
| `in` | 9% (29%) | 10% (33%) |
| Everything else | 17% (12%) | 25% (17%) |

Joins now take 142 of 258 ms in H Q9 and 33 of 84 ms in DS Q87. `like` is most of H Q13's 87 ms of `where` work.

One timing of each join on 1,800,000 rows, which neither change touched:

| Operation | ns per row |
| --- | ---: |
| `lj` to a 450,000-row result of `by` (binary search), keys at random / in order | 279 / 82 |
| `lj` to a 450,000-row `xkey` table (hash), keys at random / in order | 406 / 226 |
| `lj` to a 25-row table: result of `by` / `xkey` | 32 / 110 |
| `lj` on two key columns, 1,800,000-row table | 889 |

Hash joins build a byte key in memory for every row ([joins.rs](../src/engine/joins.rs)), likely why the table made by
`xkey` is slower than the sorted one made by `by`.

### Cluster

| Time (ms) | TPC-H before | after | TPC-DS before | after |
| --- | ---: | ---: | ---: | ---: |
| Single node, same run | 1715 | 1106 | 572 | 325 |
| **Cluster** | **1459** | **1287** | **674** | **502** |
| Slowest shard of each request: decoding it, running it, packing the result | 683 | 611 | 167 | 124 |
| Rest of each request: transport and unpacking; all of a load or a shuffle | 262 | 273 | 172 | 147 |
| Control node: merging shard results | 222 | 135 | 117 | 38 |
| Control node: running query parts itself (joins, grouping, `distinct`) | 231 | 206 | 173 | 152 |
| Control node: parsing, planning, redistributing assigned tables | 13 | 13 | 28 | 27 |
| Not timed | 49 | 48 | 17 | 14 |

* **Transport and unpacking** take 273 ms on TPC-H and 147 ms on TPC-DS. A request costs 0.2 to 0.3 ms before any
  real work, which is 6 to 9% of TPC-DS time over its 158 requests.
* **The control node packs and unpacks one shard at a time.** It encodes the same request once per shard (51 ms of
  TPC-H) and converts the replies one after another (62 ms on TPC-H, 58 ms on TPC-DS).
* **The control node's own query work** is single-threaded, and includes joins on gathered results.
* **Assigned tables travel twice.** A table assigned to a variable in a multi-statement query is sent to the shards, and
  later statements fetch it back (DS Q22 and Q36).
* **Merging** is still 135 ms on TPC-H: 65 ms merging by key and 62 ms putting rows back in order.
* **TPC-H Q22** is slower on the cluster than on the single node: `` `$2#'c_phone `` is not pushed down, so `customer`
  is gathered.

## Recommended next steps

Ordered by the time each one touches. None of the gains has been measured.

1. Give joins typed keys, as grouping has.
2. Encode each request once, and convert replies in parallel.
3. Profile what remains of `where`, starting with `like`.
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
