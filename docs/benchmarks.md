# Benchmarks (TPC-H and TPC-DS)

`kixdb` includes decision-support benchmarks modeled on **TPC-H** (all 22 queries) and a **TPC-DS** retail snowflake
subset (15 tables, 24 queries), written in `q` and `qSQL` with the standard scale-factor (`--sf`) cardinality paradigm.
Both single-node and distributed cluster runners default to **scale factor `0.05`** (~300,000 `lineitem` rows and
75,000 `orders` in TPC-H; 5x the `0.01` snowflake baseline in TPC-DS).

They are self-contained stress and differential tests rather than audited TPC kits: data is generated deterministically
in memory as columnar vectors, selected answers are recomputed independently in Rust from the generated rows, and the
cluster runners require every query's distributed answer to match a single-node engine. Each binary exits non-zero on
any query error or answer mismatch.

Measured timings and their analysis are in [Benchmark results: scale factor 0.3](benchmark-results.md).

## Commands

| Benchmark | Command | Description |
| --- | --- | --- |
| **Single-node TPC-H & TPC-DS** | `cargo run --release --example tpc` | Runs all 22 TPC-H queries and 24 TPC-DS queries at `--sf 0.05` |
| **TPC-H only / custom SF** | `cargo run --release --example tpc -- h --sf 0.05 [--durable] [--plans] [--query Q5]` | Filter suite (`h` or `ds`), durable WAL mode, execution plans, or single query |
| **Cluster TPC-H** | `cargo run --release -p kix-control --example tpch_cluster [-- --sf 0.05 --shards 3]` | Runs all 22 TPC-H queries on a multi-shard Arrow Flight cluster and checks equivalence against one node |
| **Cluster TPC-DS** | `cargo run --release -p kix-control --example tpcds_cluster [-- --sf 0.05 --shards 3]` | Runs all 24 TPC-DS queries on a multi-shard Arrow Flight cluster and checks equivalence against one node |

## Schemas, Distribution, and Verification

* **TPC-H (`examples/tpc/h.rs`):** All 8 tables (`region`, `nation`, `supplier`, `part`, `partsupp`, `customer`,
  `orders`, `lineitem`) and all 22 queries (`Q1`–`Q22`) using `qSQL` grouped aggregations, `lj`/`ij` keyed joins, `pj`
  plus-joins (`Q13` outer count distribution), `fby` filter-by-group (`Q2`, `Q17`), `sublist` top-N ordering, and
  multi-step `q` scripts. Answers for **Q1, Q6, Q12, Q13, Q14, and Q18** are independently recomputed in Rust and
  verified. On a cluster (`tpch_cluster`), `region` and `nation` are declared `REPLICATED`, while `orders` and
  `lineitem` are co-located by order key and `part` / `partsupp` by part key.
* **TPC-DS (`examples/tpc/ds.rs`):** 15 retail snowflake tables (`date_dim`, `time_dim`, `item`,
  `customer_demographics`, `household_demographics`, `customer_address`, `customer`, `store`, `promotion`, `warehouse`,
  `store_sales`, `store_returns`, `catalog_sales`, `web_sales`, `inventory`) and 24 queries covering star joins, `fby`
  correlated filters and analytic ratios/ranks, multi-level rollups, channel `ij`/`lj`/`uj` set intersections and
  differences, year-over-year channel comparisons, and grouped running `sums`. Answers for **Q3, Q42, and Q96** are
  independently recomputed in Rust and verified. On a cluster (`tpcds_cluster`), dimension tables are `REPLICATED` and
  fact tables (`store_sales`, `store_returns`, `catalog_sales`, `web_sales`, `inventory`) and `item` are sharded by item
  surrogate key.
