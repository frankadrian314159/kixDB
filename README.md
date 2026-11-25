# kixDB

A black-box clone of KX's kdb+ columnstore database and its q language (including qSQL), written in Rust. It runs as a
library, a shell, a server speaking kdb+ binary IPC, a desktop app, and a distributed cluster.

kixDB is an independent implementation of documented behavior. It is not affiliated with KX, covers a subset of q, and
has not been tested against a kdb+ installation.

## Quick start

```
cargo run --release --bin kix                      # q) prompt, in memory
cargo run --release --bin kix -- mydata            # durable workspace in ./mydata
cargo run --release --bin kix-serve -- mydata      # kdb+ IPC server on 127.0.0.1:5000
cargo run --release --bin kix -- --connect :5000   # shell attached to a server
cargo run --release --features gui --bin kix-gui   # desktop app
```

```q
q)trade: ([] time: 09:30:00 09:31:00 09:35:00; sym: `AAPL`MSFT`AAPL; price: 180 400 182.0; size: 100 200 300)
q)select vwap: size wavg price by sym from trade
q)quote: ([] time: 09:29:00 09:30:30; sym: `AAPL`MSFT; bid: 179.5 399.5)
q)aj[`sym`time; trade; quote]
```

As a library:

```rust
let server = kixdb::Server::new();            // or Server::open("mydata")?
let mut conn = server.connect();
assert_eq!(conn.eval("sum 1 + til 10")?.to_string(), "55");
```

## A cluster

```
cargo build --release --workspace
kix-cluster --make-certs certs control shard0 shard1      # a certificate authority and one certificate per node
kix-shard --id 0 --listen 127.0.0.1:50051 --node-ca certs/ca.pem --node-cert certs/shard0.pem --node-key certs/shard0.key
kix-shard --id 1 --listen 127.0.0.1:50052 --node-ca certs/ca.pem --node-cert certs/shard1.pem --node-key certs/shard1.key
echo '{"shards":[{"id":0,"addr":"127.0.0.1:50051"},{"id":1,"addr":"127.0.0.1:50052"}]}' > cluster.json
kix-cluster --config cluster.json --state cluster.state --listen 5000 \
    --node-ca certs/ca.pem --node-cert certs/control.pem --node-key certs/control.key
kix --connect 127.0.0.1:5000
```

Tables are spread over the shards and queries give the answers one server would give. Shards can be replicated with
Raft, and a write that spans shards is made [on all of them or none](docs/distributed.md#writes-that-span-shards). Read
[what the cluster does not guarantee](docs/distributed.md#limits) before relying on it. Nodes talk over [mutual TLS](docs/distributed.md#security), unless started with
`--insecure`.

## Layout

| Path | Builds | Contents |
| --- | --- | --- |
| `src/` | `kixdb` (library), `kix`, `kix-serve` | engine, q / qSQL, joins, storage, IPC, shell, server |
| `src/gui/` | `kix-gui` (feature `gui`) | desktop app |
| `crates/parallel/raft` | `kix-raft` | Raft consensus core |
| `crates/parallel/shared-ces` | `kix-shared-ces` | cluster configuration, wire messages, Arrow conversion, routing, metrics |
| `crates/parallel/storage` | `kix-storage`, `kix-shard` | shard node |
| `crates/parallel/control` | `kix-control`, `kix-cluster` | control node and query planner |

`cargo build` and `cargo test` at the root cover the single-node package only; add `--workspace` for the cluster.

Building needs a C compiler (on Windows, the Visual Studio build tools) for the [mimalloc](https://crates.io/crates/mimalloc)
memory allocator, which the servers and the desktop app use because the engine runs markedly faster with it. `--no-default-features` leaves
it out of `kix-serve` and `kix-gui`; the cluster's programs always use it.

## Documentation

* [Language](docs/language.md)
* [Storage](docs/storage.md)
* [IPC and TLS](docs/ipc.md)
* [Shell and desktop app](docs/shell-and-gui.md)
* [Distributed edition](docs/distributed.md)
* [Benchmarks (TPC-H and TPC-DS)](docs/benchmarks.md)

## Testing and Benchmarks

```
cargo test --workspace --features gui
cargo run --release --example tpc                          # TPC-H + TPC-DS (default --sf 0.05)
cargo run --release -p kix-control --example tpch_cluster  # TPC-H on cluster vs single node (--sf 0.05)
cargo run --release -p kix-control --example tpcds_cluster # TPC-DS on cluster vs single node (--sf 0.05)
```

The cluster tests start shard nodes in the test process and compare every answer with a single server, including a few
thousand generated queries and writes.

## License

MIT
