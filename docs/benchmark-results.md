# Benchmark results: scale factor 0.3

Measured 2026-11-25 with the programs described in [benchmarks.md](benchmarks.md). **Before** is commit `a06fb4b`.
**After** adds column pruning in qSQL, typed loops for the slowest operators, and push-down of constant casts.

## Summary

| Configuration | TPC-H before | after | TPC-DS before | after | Both suites: gain | After, against single node |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single node | 9.76 s | **3.01 s** | 2.71 s | **1.18 s** | 3.0x | 1.00x |
| Cluster, 1 shard, in-process | 10.04 s | 3.66 s | 2.95 s | 1.48 s | 2.5x | 0.82x |
| Cluster, 3 shards, in-process | 4.94 s | 1.98 s | 1.70 s | 0.95 s | 2.3x | 1.43x |
| Cluster, 6 shards, in-process | 4.15 s | 1.77 s | 1.53 s | 0.92 s | 2.1x | 1.55x |
| Cluster, 12 shards, in-process | 4.32 s | 2.46 s | 1.42 s | 0.92 s | 1.7x | 1.24x |
| Cluster, 3 shard processes | 4.64 s | 1.71 s | 1.76 s | 0.96 s | 2.4x | 1.57x |
| Cluster, 6 shard processes | 3.59 s | **1.46 s** | 1.48 s | **0.92 s** | 2.1x | 1.76x |

* **The single node is 3.0x faster** (12.47 s to 4.19 s for both suites), and the best cluster setup 2.1x (5.07 s to
  2.38 s). No query's median was more than 5% (plus 1 ms) slower than before in any configuration.
* **The cluster's advantage shrank**, from 2.5x over the single node to 1.8x with six shard processes (2.1x on TPC-H,
  1.3x on TPC-DS). Queries now do less work, so coordination is probably a larger share. One shard costs 22% (H) to
  25% (DS).
* **Use no more shards than cores:** on TPC-H, twelve shards are now clearly slower than six.
* **Correctness:** all 140 benchmark runs exited cleanly and every cluster answer matched the single node's (2,760
  query comparisons). Nine queries are also recomputed independently from the generated rows (H: Q1, Q6, Q12, Q13, Q14,
  Q18; DS: Q3, Q42, Q96). Before and after also gave identical result rows for all 46 queries on the single node, and
  identical results or error kinds for 69,692 generated operator, grouping and `distinct` expressions.

## Setup

* AMD Ryzen 5 7430U (6 cores, 12 threads), 31 GB RAM, Windows 11, on mains power. Release build, rustc 1.95.
* In memory: no write-ahead log, no replication, no TLS. Shards and control node on one machine over loopback TCP.
* One client running one query at a time. This measures latency, not throughput under concurrency.
* Each figure is the median of five runs, with before, after and the configurations interleaved. Suite totals are
  medians of the per-run totals. The first two repetitions ran slower for both builds (some totals by up to 47%), which
  the medians mostly set aside; treat differences under about 10% as noise.
* Data: TPC-H 2,598,816 rows (`lineitem` 1,800,786). TPC-DS 1,310,782 rows (`store_sales` 539,498, `inventory`
  360,000, `catalog_sales` 179,761, `web_sales` 119,410).

These are **not TPC results**. The data comes from the repository's generator, and the queries are `q` rewrites. The
TPC-DS "scale factor" is 30 times the repository's 0.01 baseline: its `store_sales` is about a fifth of official SF 1,
and it covers 24 of the 99 queries.

## Load time and memory

Load times barely changed. Cluster load is the copy from an already generated single-node engine through the control
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

### Queries copy only the columns they read

`where` used to copy every column of the kept rows, for each clause after the first and again for the result, and
grouping copied them again for each group. Now only the columns a query names are copied
([qsql.rs](../src/engine/qsql.rs)). A query that can name columns at run time (through `value`, `get`, `eval`,
`reval`, `system`, a view or a variable holding a verb) still sees them all.

### Typed loops instead of a value per row

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

Comparisons, `within` and float arithmetic run over typed vectors; `in`, grouping and `distinct` compare fixed-width
values as bits instead of building a byte key per row ([ops.rs](../src/engine/ops.rs)).

### Constant casts are pushed down

The control node computes casts of constants such as `` `$"Brand#23" `` while planning
([plan.rs](../crates/parallel/control/src/plan.rs)), so such filters run on the shards and can route like literals.
TPC-H Q8, Q16, Q17 and Q19 no longer gather any table.

## Per query (milliseconds)

Cluster columns other than "6 proc before" are after the changes. Each gain is before divided by after.

### TPC-H

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 888 | 243 | 3.7x | 284 | 135 | 88 | 161 | 124 | 279 | 76 | 3.7x |
| Q2 | 23 | 8 | 2.9x | 12 | 9 | 10 | 15 | 10 | 13 | 8 | 1.6x |
| Q3 | 419 | 107 | 3.9x | 129 | 69 | 64 | 107 | 58 | 149 | 49 | 3.0x |
| Q4 | 235 | 43 | 5.5x | 52 | 38 | 34 | 55 | 28 | 79 | 23 | 3.4x |
| Q5 | 293 | 75 | 3.9x | 90 | 51 | 51 | 84 | 43 | 103 | 36 | 2.9x |
| Q6 | 272 | 21 | 13.0x | 24 | 10 | 10 | 20 | 11 | 95 | 7 | 13.6x |
| Q7 | 296 | 80 | 3.7x | 99 | 56 | 53 | 93 | 54 | 122 | 46 | 2.7x |
| Q8 | 268 | 91 | 2.9x | 102 | 57 | 52 | 76 | 46 | 113 | 39 | 2.9x |
| Q9 | 1630 | 903 | 1.8x | 1045 | 624 | 517 | 579 | 504 | 664 | 432 | 1.5x |
| Q10 | 478 | 222 | 2.2x | 311 | 181 | 170 | 228 | 173 | 225 | 150 | 1.5x |
| Q11 | 30 | 14 | 2.1x | 17 | 9 | 8 | 9 | 8 | 12 | 7 | 1.7x |
| Q12 | 604 | 58 | 10.4x | 72 | 35 | 33 | 65 | 30 | 176 | 22 | 8.0x |
| Q13 | 381 | 157 | 2.4x | 178 | 129 | 145 | 206 | 115 | 247 | 131 | 1.9x |
| Q14 | 167 | 16 | 10.4x | 18 | 12 | 13 | 25 | 9 | 63 | 7 | 9.0x |
| Q15 | 195 | 21 | 9.3x | 28 | 22 | 28 | 51 | 19 | 90 | 23 | 3.9x |
| Q16 | 103 | 50 | 2.1x | 58 | 49 | 53 | 73 | 41 | 74 | 38 | 1.9x |
| Q17 | 109 | 27 | 4.0x | 36 | 16 | 12 | 13 | 15 | 44 | 13 | 3.4x |
| Q18 | 1857 | 578 | 3.2x | 644 | 283 | 218 | 233 | 253 | 483 | 194 | 2.5x |
| Q19 | 184 | 17 | 10.8x | 21 | 11 | 13 | 19 | 9 | 125 | 8 | 15.6x |
| Q20 | 292 | 36 | 8.1x | 38 | 18 | 18 | 31 | 18 | 101 | 14 | 7.2x |
| Q21 | 797 | 192 | 4.2x | 215 | 111 | 111 | 147 | 97 | 237 | 83 | 2.9x |
| Q22 | 107 | 47 | 2.3x | 101 | 99 | 112 | 157 | 82 | 139 | 83 | 1.7x |

### TPC-DS subset

| Query | Single before | Single after | Gain | 1 in-proc | 3 in-proc | 6 in-proc | 12 in-proc | 3 proc | 6 proc before | 6 proc after | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 64 | 37 | 1.7x | 52 | 31 | 29 | 26 | 36 | 36 | 29 | 1.2x |
| Q3 | 40 | 13 | 3.1x | 14 | 8 | 7 | 7 | 8 | 16 | 7 | 2.3x |
| Q6 | 88 | 38 | 2.3x | 49 | 32 | 30 | 27 | 30 | 47 | 28 | 1.7x |
| Q7 | 71 | 20 | 3.5x | 26 | 15 | 13 | 12 | 15 | 29 | 13 | 2.2x |
| Q12 | 20 | 12 | 1.7x | 15 | 10 | 9 | 9 | 9 | 12 | 8 | 1.5x |
| Q15 | 71 | 34 | 2.1x | 42 | 30 | 29 | 27 | 28 | 46 | 27 | 1.7x |
| Q19 | 87 | 36 | 2.4x | 46 | 29 | 27 | 24 | 28 | 44 | 26 | 1.7x |
| Q20 | 24 | 11 | 2.2x | 14 | 9 | 8 | 8 | 9 | 14 | 8 | 1.8x |
| Q22 | 114 | 55 | 2.1x | 80 | 67 | 75 | 104 | 75 | 105 | 81 | 1.3x |
| Q26 | 34 | 11 | 3.1x | 16 | 9 | 9 | 9 | 10 | 15 | 9 | 1.7x |
| Q27 | 73 | 20 | 3.6x | 32 | 23 | 22 | 25 | 23 | 39 | 23 | 1.7x |
| Q36 | 124 | 40 | 3.1x | 54 | 34 | 39 | 48 | 34 | 64 | 38 | 1.7x |
| Q38 | 403 | 233 | 1.7x | 280 | 174 | 166 | 162 | 175 | 255 | 166 | 1.5x |
| Q42 | 36 | 8 | 4.5x | 10 | 6 | 5 | 5 | 6 | 13 | 5 | 2.6x |
| Q43 | 104 | 31 | 3.4x | 38 | 30 | 38 | 44 | 30 | 65 | 37 | 1.8x |
| Q50 | 288 | 157 | 1.8x | 163 | 78 | 66 | 54 | 78 | 95 | 64 | 1.5x |
| Q52 | 35 | 8 | 4.4x | 10 | 6 | 5 | 5 | 6 | 14 | 5 | 2.8x |
| Q55 | 36 | 8 | 4.5x | 10 | 5 | 5 | 4 | 5 | 14 | 5 | 2.8x |
| Q69 | 72 | 17 | 4.2x | 26 | 14 | 13 | 14 | 15 | 37 | 13 | 2.8x |
| Q87 | 384 | 216 | 1.8x | 244 | 172 | 154 | 146 | 166 | 249 | 152 | 1.6x |
| Q96 | 37 | 8 | 4.6x | 10 | 5 | 4 | 4 | 5 | 14 | 4 | 3.5x |
| Q98 | 54 | 20 | 2.7x | 26 | 15 | 14 | 13 | 15 | 26 | 13 | 2.0x |
| Q11 | 400 | 141 | 2.8x | 193 | 159 | 138 | 140 | 142 | 237 | 147 | 1.6x |
| Q51 | 5 | 3 | 1.7x | 5 | 4 | 4 | 4 | 4 | 6 | 4 | 1.5x |

## What still limits performance

* **Grouping evaluates the column expressions once per group**, about 1 µs per group: a `sum ... by` over 450,000 groups
  takes 0.46 s. The two slowest queries left, H Q9 (0.9 s) and Q18 (0.6 s), both group by order key.
* **Joins still build a byte key per row** ([joins.rs](../src/engine/joins.rs)), and temporal fields (`.year`) and date
  arithmetic still make a value per row. Their share of query time was not measured.
* **TPC-H Q22** is slower on the cluster than on the single node: `` `$2#'c_phone `` is not pushed down, so `customer`
  is gathered.
* **Multi-statement queries** return intermediate lists to the control node, which sends them back to the shards inside
  later statements. Most queries here have this shape. Its cost was not measured separately.
* **Rollups (DS Q22, Q27, Q36)** gather derived tables to union them.

## Recommended next steps

1. Compute simple aggregates (`sum`, `count`, `min`, `max`, `avg`, `first`, `last`) for all groups in one pass.
2. Profile the joins, and give them the typed keys that grouping now uses.
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
