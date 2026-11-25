# Benchmark results: scale factor 0.3

Measured 2026-11-25 with the programs described in [benchmarks.md](benchmarks.md). Two changes were measured, each
against the build before it in its own interleaved run. **Before** is commit `77c9682`; **after** computes grouped
aggregates for all groups at once. The [first change](#first-change-columns-operators-and-casts) led from `a06fb4b` to
`77c9682`.

## Summary

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 2.97 s | **1.62 s** | 1.16 s | **0.58 s** | 1.9x | 1.00x |
| Cluster, 1 shard, in-process | 3.43 s | 2.10 s | 1.43 s | 0.91 s | 1.6x | 0.73x |
| Cluster, 3 shards, in-process | 1.98 s | 1.40 s | 0.97 s | 0.63 s | 1.5x | 1.09x |
| Cluster, 6 shards, in-process | 1.79 s | 1.42 s | 0.90 s | 0.63 s | 1.3x | 1.08x |
| Cluster, 12 shards, in-process | 2.41 s | 1.97 s | 0.92 s | 0.70 s | 1.2x | 0.82x |
| Cluster, 3 shard processes | 1.74 s | 1.20 s | 0.94 s | 0.67 s | 1.4x | 1.18x |
| Cluster, 6 shard processes | 1.46 s | **1.09 s** | 0.90 s | **0.66 s** | 1.3x | 1.26x |

* **The single node is 1.9x faster** (4.13 s to 2.20 s for both suites), and six shard processes 1.35x (2.36 s to
  1.75 s). Since `a06fb4b` (12.47 s and 5.07 s in the first run) that is about 5.7x and 2.9x.
* **The cluster now helps only TPC-H.** Six shard processes run it 1.5x as fast as the single node, but run TPC-DS
  slower (0.88x), as does every other cluster setup (0.64x to 0.92x).
* **Use no more shards than cores:** twelve shards remain slower than six.
* **One query's median was more than 5% (plus 1 ms) slower:** H Q4 with six shard processes, 21 to 24 ms. It did not
  change on the single node.
* **Correctness:** all 140 benchmark runs exited cleanly and every cluster answer matched the single node's (2,760
  query comparisons). Nine queries are also recomputed independently from the generated rows (H: Q1, Q6, Q12, Q13, Q14,
  Q18; DS: Q3, Q42, Q96). Before and after gave identical result rows for all 46 queries on the single node, and
  identical output, errors included, for 30,153 generated grouped queries.

## Setup

* AMD Ryzen 5 7430U (6 cores, 12 threads), 31 GB RAM, Windows 11, on mains power. Release build, rustc 1.95.
* In memory: no write-ahead log, no replication, no TLS. Shards and control node on one machine over loopback TCP.
* One client running one query at a time. This measures latency, not throughput under concurrency.
* Each figure is the median of five runs, with before, after and the configurations interleaved. Suite totals are
  medians of the per-run totals. Treat differences under about 10% as noise: repetitions mostly agreed within 5% (the
  worst was 12% from its median), and the build both runs measured differed by under 2% in single-node total.
* Data: TPC-H 2,598,816 rows (`lineitem` 1,800,786). TPC-DS 1,310,782 rows (`store_sales` 539,498, `inventory`
  360,000, `catalog_sales` 179,761, `web_sales` 119,410).

These are **not TPC results**. The data comes from the repository's generator, and the queries are `q` rewrites. The
TPC-DS "scale factor" is 30 times the repository's 0.01 baseline: its `store_sales` is about a fifth of official SF 1,
and it covers 24 of the 99 queries.

## Load time and memory

The table is from the first change's run; peak memory was the same, within a few MB, in the grouping run. In that run
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

### Aggregates are computed for all groups at once

`select ... by` and `exec ... by` used to copy each group's rows and evaluate every column expression on the copy. Now a
column made of aggregates (`sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `med`, `prd`, `var`, `dev`, `svar`,
`sdev`, `wsum`, `wavg`), combined with item-by-item verbs and constants, is computed for all groups together
([qsql.rs](../src/engine/qsql.rs)). Each aggregate's argument is evaluated once over all rows, and each group then
aggregates its own values. The argument must depend on its own row only (columns, `i`, temporal fields, item-by-item
verbs, `in`, `within`), possibly under `distinct`. Other columns, and any that signal an error, are still evaluated
group by group. Groups are also found and sorted faster.

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

Cluster columns other than "6 proc before" are after the grouping change. Each gain is before divided by after.

### TPC-H

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 241 | 187 | 1.3x | 197 | 86 | 65 | 73 | 84 | 77 | 63 | 1.2x |
| Q2 | 8 | 6 | 1.3x | 10 | 8 | 10 | 14 | 8 | 8 | 7 | 1.1x |
| Q3 | 107 | 56 | 1.9x | 74 | 44 | 53 | 98 | 35 | 47 | 32 | 1.5x |
| Q4 | 43 | 43 | 1.0x | 53 | 33 | 34 | 51 | 28 | 21 | 24 | 0.9x |
| Q5 | 75 | 59 | 1.3x | 73 | 46 | 50 | 76 | 37 | 39 | 32 | 1.2x |
| Q6 | 21 | 21 | 1.0x | 22 | 11 | 11 | 19 | 9 | 7 | 7 | 1.0x |
| Q7 | 80 | 53 | 1.5x | 72 | 47 | 50 | 84 | 38 | 40 | 33 | 1.2x |
| Q8 | 90 | 57 | 1.6x | 75 | 45 | 49 | 72 | 36 | 40 | 31 | 1.3x |
| Q9 | 882 | 338 | 2.6x | 460 | 336 | 339 | 423 | 316 | 425 | 291 | 1.5x |
| Q10 | 220 | 65 | 3.4x | 149 | 123 | 143 | 202 | 104 | 141 | 105 | 1.3x |
| Q11 | 14 | 6 | 2.3x | 9 | 6 | 6 | 7 | 4 | 7 | 4 | 1.8x |
| Q12 | 58 | 56 | 1.0x | 71 | 35 | 34 | 58 | 30 | 22 | 23 | 1.0x |
| Q13 | 153 | 113 | 1.4x | 134 | 90 | 105 | 169 | 83 | 129 | 95 | 1.4x |
| Q14 | 15 | 15 | 1.0x | 18 | 12 | 14 | 21 | 9 | 8 | 7 | 1.1x |
| Q15 | 21 | 15 | 1.4x | 22 | 17 | 24 | 43 | 14 | 23 | 14 | 1.6x |
| Q16 | 47 | 28 | 1.7x | 38 | 37 | 43 | 58 | 27 | 38 | 26 | 1.5x |
| Q17 | 28 | 28 | 1.0x | 37 | 16 | 15 | 13 | 15 | 13 | 13 | 1.0x |
| Q18 | 575 | 212 | 2.7x | 253 | 150 | 133 | 163 | 125 | 201 | 104 | 1.9x |
| Q19 | 17 | 17 | 1.0x | 19 | 12 | 14 | 18 | 9 | 8 | 8 | 1.0x |
| Q20 | 36 | 36 | 1.0x | 38 | 19 | 18 | 29 | 17 | 14 | 14 | 1.0x |
| Q21 | 193 | 168 | 1.1x | 195 | 107 | 96 | 135 | 85 | 81 | 74 | 1.1x |
| Q22 | 49 | 47 | 1.0x | 79 | 96 | 113 | 148 | 75 | 82 | 83 | 1.0x |

### TPC-DS subset

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 37 | 11 | 3.4x | 26 | 20 | 20 | 21 | 21 | 27 | 20 | 1.4x |
| Q3 | 11 | 11 | 1.0x | 13 | 7 | 6 | 6 | 7 | 7 | 7 | 1.0x |
| Q6 | 38 | 19 | 2.0x | 45 | 23 | 22 | 23 | 23 | 27 | 22 | 1.2x |
| Q7 | 20 | 13 | 1.5x | 19 | 12 | 10 | 10 | 12 | 13 | 10 | 1.3x |
| Q12 | 11 | 5 | 2.2x | 9 | 7 | 7 | 7 | 7 | 8 | 6 | 1.3x |
| Q15 | 34 | 15 | 2.3x | 24 | 19 | 20 | 21 | 21 | 27 | 21 | 1.3x |
| Q19 | 36 | 18 | 2.0x | 28 | 21 | 20 | 20 | 22 | 25 | 20 | 1.2x |
| Q20 | 11 | 5 | 2.2x | 8 | 6 | 6 | 6 | 6 | 8 | 6 | 1.3x |
| Q22 | 55 | 26 | 2.1x | 57 | 54 | 57 | 89 | 60 | 80 | 67 | 1.2x |
| Q26 | 11 | 6 | 1.8x | 10 | 7 | 7 | 7 | 7 | 9 | 7 | 1.3x |
| Q27 | 20 | 13 | 1.5x | 25 | 19 | 18 | 21 | 19 | 24 | 20 | 1.2x |
| Q36 | 39 | 19 | 2.1x | 32 | 25 | 31 | 43 | 26 | 37 | 31 | 1.2x |
| Q38 | 225 | 109 | 2.1x | 164 | 106 | 105 | 108 | 111 | 162 | 105 | 1.5x |
| Q42 | 9 | 8 | 1.1x | 10 | 5 | 5 | 5 | 6 | 5 | 5 | 1.0x |
| Q43 | 31 | 20 | 1.6x | 26 | 20 | 22 | 29 | 20 | 38 | 23 | 1.7x |
| Q50 | 155 | 49 | 3.2x | 55 | 36 | 32 | 33 | 37 | 64 | 35 | 1.8x |
| Q52 | 8 | 8 | 1.0x | 10 | 5 | 5 | 4 | 6 | 5 | 5 | 1.0x |
| Q55 | 8 | 9 | 0.9x | 10 | 5 | 5 | 4 | 5 | 5 | 5 | 1.0x |
| Q69 | 17 | 17 | 1.0x | 25 | 14 | 13 | 14 | 14 | 13 | 14 | 0.9x |
| Q87 | 214 | 110 | 1.9x | 147 | 108 | 105 | 106 | 111 | 149 | 104 | 1.4x |
| Q96 | 9 | 9 | 1.0x | 9 | 5 | 4 | 4 | 5 | 4 | 4 | 1.0x |
| Q98 | 20 | 13 | 1.5x | 16 | 11 | 11 | 10 | 12 | 13 | 10 | 1.3x |
| Q11 | 136 | 61 | 2.2x | 105 | 85 | 92 | 101 | 90 | 143 | 97 | 1.5x |
| Q51 | 3 | 1 | 3.0x | 2 | 2 | 2 | 2 | 2 | 4 | 2 | 2.0x |

## What still limits performance

* **Finding the groups** is now most of a simple grouped query: `group` alone on the column with 440,000 distinct
  values takes 0.31 s.
* **Aggregates other than `count`, `first` and a long or float `sum`** still copy each group's values.
* **Other grouped expressions** (lambdas, running verbs such as `sums`, `$[...]`), `update ... by` and `fby` are still
  evaluated once per group, about 1 µs per group.
* **TPC-DS is slower on the cluster than on the single node**, so coordination probably costs more than sharding saves
  there. Where that time goes was not measured. Multi-statement queries, which most are, return intermediate lists to
  the control node and send them back to the shards. Rollups (DS Q22, Q27, Q36) gather derived tables to union them.
* **TPC-H Q22** is slower on the cluster than on the single node: `` `$2#'c_phone `` is not pushed down, so `customer`
  is gathered.
* **Joins still build a byte key per row** ([joins.rs](../src/engine/joins.rs)), and temporal fields (`.year`) and date
  arithmetic still make a value per row. Their share of query time was not measured.

## Recommended next steps

1. Profile the slowest single-node queries left (H Q9, Q18, Q1 and Q21; DS Q38 and Q87), starting with the joins.
2. Measure where the cluster spends its time on short queries.
3. Measure durable and replicated shards and concurrent clients.

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
