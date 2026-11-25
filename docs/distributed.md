# The distributed edition

A **control node** (`kix-cluster`) that clients talk to, and **shard nodes** (`kix-shard`) that each hold part of the
data. Clients use the same kdb+ IPC protocol as with one server. The control node and shards talk Arrow Flight (gRPC).

| Crate | Role |
| --- | --- |
| `kix-control` | the control node: distribution, query planning, merging, IPC gateway |
| `kix-storage` | a shard: a complete single-node engine behind an Arrow Flight service, optionally a Raft replica |
| `kix-raft` | Raft: election, log replication, leader lease, snapshots, membership; tested under a simulated faulty network |
| `kix-shared-ces` | configuration, wire messages, Arrow conversion, key routing, metrics |

## Running

Make the certificates first (see [Security](#security)), and copy to each node `ca.pem` and its own `.pem` and `.key`:

```
kix-cluster --make-certs certs control s0r1 s0r2 s0r3 s1
```

Then start each shard on its machine:

```
# shard 0 with three replicas: --id is the shard, --replica the member of its group
kix-shard --id 0 --listen 10.0.0.1:50051 --data s0r1 --replica 1 --peers 2=10.0.0.2:50051,3=10.0.0.3:50051 \
    --node-ca ca.pem --node-cert s0r1.pem --node-key s0r1.key
kix-shard --id 0 --listen 10.0.0.2:50051 --data s0r2 --replica 2 --peers 1=10.0.0.1:50051,3=10.0.0.3:50051 \
    --node-ca ca.pem --node-cert s0r2.pem --node-key s0r2.key
kix-shard --id 0 --listen 10.0.0.3:50051 --data s0r3 --replica 3 --peers 1=10.0.0.1:50051,2=10.0.0.2:50051 \
    --node-ca ca.pem --node-cert s0r3.pem --node-key s0r3.key
# shard 1, not replicated (in memory without --data)
kix-shard --id 1 --listen 10.0.0.4:50051 --data s1 \
    --node-ca ca.pem --node-cert s1.pem --node-key s1.key
```

`cluster.json` lists the shards in order; the replicas of a shard share one address string:

```json
{"shards": [
  {"id": 0, "addr": "10.0.0.1:50051,10.0.0.2:50051,10.0.0.3:50051"},
  {"id": 1, "addr": "10.0.0.4:50051"}
]}
```

```
kix-cluster --config cluster.json --state cluster.state --listen 5000 -u ann:secret [--tls] [--force-tls] \
    --node-ca ca.pem --node-cert control.pem --node-key control.key
# or run a script
kix-cluster --config cluster.json --state cluster.state --plan \
    --node-ca ca.pem --node-cert control.pem --node-key control.key < script.q
```

`--tls` and `--force-tls` concern the clients' connections to the control node; the `--node-` flags concern the
connections between nodes.

From Rust: `Cluster::connect_with(&config, tls)` or `Cluster::open_with(state_file, &config, tls)`, where `tls` is a
`NodeTls::from_files(..)` (`connect` and `open` use unprotected connections), then `cluster.session()`,
`session.eval(q)` and `session.last_plan()`. Do not call these from inside a tokio runtime.

**Keep the state file.** It is checksummed (`KXCS` + CRC32), locked against concurrent processes (`cluster.state.lock`),
and records the shards, table schemas, distribution, and the variables, functions and views of the control node. A control
node started without it does not know about data already on the shards.

## Security

All connections between nodes (control node to shard, shard to shard, and replica to replica) use **mutual TLS**: both
ends show a certificate signed by the cluster's certificate authority, and refuse the connection otherwise. `kix-shard`
and `kix-cluster` do not start without `--node-ca`, `--node-cert` and `--node-key`. `--insecure` starts them without
protection, with a warning: then anyone who reaches a shard's port can run any q on it. The two kinds of node cannot be
mixed.

```
kix-cluster --make-certs certs control shard0 shard1     # ca.pem, ca.key, and NAME.pem and NAME.key for each name
kix-cluster --make-certs certs shard2                    # later: another node, signed by the same authority
```

* **Guard `ca.key`.** It is only needed to make certificates; do not copy it to the nodes. Whoever holds it, or any
  certificate it signed with its key, can run any q on every shard.
* Every certificate of the authority gives the same access, and none can be revoked: if a key leaks, make a new
  authority in an empty directory and give every node a new certificate.
* Key files are readable only by their owner on Unix. On Windows they take the folder's permissions: restrict them.
* Made certificates expire on 1 January ten years after the year they were made. Replace them before then and restart
  the nodes.
* **Your own certificates** work too (PEM files). The authority must sign nothing but this cluster's nodes. Every node's
  certificate needs the DNS name `kix-node` (callers check for that name, not for the host, so a node can move) and,
  if it restricts key usage, both server and client authentication.

Clients reach the control node over kdb+ IPC, whose TLS and passwords are set separately ([ipc.md](ipc.md)). To listen
for them on an address other machines can reach (`--bind`), `kix-cluster` needs `-u` and `--force-tls`, or
`--insecure-clients`. The
`--metrics-listen` pages are plain HTTP without authentication.

## Distribution

A table is distributed when it is assigned. The default:

| Table | Placement |
| --- | --- |
| has a `sym` column (a key column, if keyed) | sharded by `sym` |
| other keyed table | sharded by the first key column |
| other table | replicated: a full copy on every shard |

Declare something else before creating the table, or move an existing one:

```
.kix.dist[`trade; `sharded; `sym]        DISTRIBUTE trade BY sym
.kix.dist[`inst; `replicated]            DISTRIBUTE inst REPLICATED
.kix.dist[`daily; `partitioned; `date]   DISTRIBUTE daily PARTITIONED BY date
.kix.dist[`quote; `partitioned; `date`sym]   DISTRIBUTE quote PARTITIONED BY date, sym
```

* **Sharded:** the key's value is hashed into one of 1,024 slots, each owned by a shard. Symbols hash by their text.
* **Partitioned:** an integer or temporal value modulo the number of shards.
* **Co-partitioned** (partitioned by several columns, such as `date` and `sym`): the values are hashed together, so one
  day is spread over the shards and so is one symbol, while the rows of one (day, symbol) pair share a shard. The order
  the columns are named in does not matter.
* A keyed table can only be distributed by key columns.

### Choosing co-partitioning

Use it when a single day is too large or too busy for one shard, or a few symbols dominate. It is not the default,
because it takes both columns to find rows:

| With `date`, `sym` co-partitioning | runs on the shards |
| --- | --- |
| `where date=d, sym=s` | on one shard |
| `... by date, sym`, or `... by sym` where `date=d` | grouped on each shard |
| `where date=d` or `where sym=s` alone | on every shard, merged |
| combinable aggregates `by sym` over several days | two-phase |
| other `by sym` work over several days, such as `last` or `med` | gathered |
| `aj[`sym`date`time; trade; quote]` | on each shard, without moving rows |
| `aj[`sym`time; ...]` where both tables have `date` | on the control node |

Compare `date` in joins between co-partitioned tables. If both have the column and the join does not compare it, the
right table's `date` replaces the left's and the join cannot run on the shards.

Variables, functions and views live on the control node. So do tables named like a built-in verb (such as `log`) and
partitioned on-disk databases mounted with `\l`, which are not spread over the shards.

### Row order

q results depend on row order. A shard keeps its rows in their original relative order, and for each sharded table a
parallel vector `.kix.ord.<table>` holds every row's global ordinal. The control node merges rows from several shards by
ordinal, so they come back in the order one server would hold them.

## How a statement runs

`EXPLAIN statement` shows the plan without running it.

| Plan | When | What happens |
| --- | --- | --- |
| schema only | `cols`, `keys`, `type`, `meta` | answered by the control node |
| count | `count t` | the shards' counts are added |
| replicated | every table is replicated | one shard answers |
| route | leading row-by-row `where` clauses fix every distribution column to values on one shard | that shard runs the whole query, including `deltas`, `mavg`, `first` and so on |
| grouped by the distribution columns | `by` includes each one that the filters do not fix to a single value | each group lies on one shard: results are concatenated |
| rows merged in order | row-by-row filters and columns | shard results are merged by ordinal |
| two-phase aggregation | `sum count min max prd avg wavg wsum var svar dev sdev`, and arithmetic on them | shards compute partial states, the control node merges them |
| gather | everything else | the tables named are copied to the control node and the statement runs there |

For a gathered query over one sharded table, leading row-by-row filters run on the shards and only the columns used are
copied. A statement that calls `value`, `get`, `system` or similar may name tables at run time, so every table is
gathered.

A filter may compare with a literal, a variable or a cast of constants (`` sym = `$"BRK.B" ``, `` d > "D"$"2026.11.24" ``).
The control node computes the cast, so it filters on the shards and can route like a literal.

Independent subqueries (`where sym in exec sym from inst where ...`) run first and are sent along as values.

### Joins

`lj`, `ij`, `pj`, `ej`, `aj`, `aj0`, `wj` and `wj1` run on the shards when the matching rows are together: the other
table is replicated, or both are distributed the same way and the join compares all their distribution columns (trades
and quotes sharded by `sym`, or co-partitioned by `date` and `sym`).
Otherwise:

* a table of at most 100,000 rows (`--broadcast-limit`) is **broadcast**: copied to every shard;
* larger ones are **shuffled**, shard to shard, into temporary tables. If the join compares all of the left table's
  distribution columns, only the right table moves, to match them; otherwise both are spread by one join column
  (`sym` where possible).

`uj`, joins of joins that would need a second move, and `wj` windows not computed from the left table's own columns are
gathered.

## Writes

* `insert` and `upsert` are split by the distribution columns. Rows take the table's column types as on one server.
  Duplicate keys of a keyed `insert` are checked on every shard before any shard changes.
* `update` and `delete` run on every shard when each row (or group, with `by` on the distribution columns) can be handled
  alone and no distribution column changes. They are evaluated on every shard before any applies, so an error changes nothing.
* Any other change gathers the table, applies it on the control node, and distributes the table again.

## Concurrency

There is one control node. Pushed-down reads and selective partial gathers (`slice`) share the cluster concurrently
(and temporary `.kix.tmp.*` broadcast/shuffle join tables stay in shard RAM without writing to the WAL or Raft log);
writes and full-table mirror gathers take exclusive cluster access.

## Replication

A replicated shard elects a leader. A change takes effect only after a majority of replicas has logged it; without a
majority writes fail with `'cluster`. When the leader dies the control node finds the new one. Replicas cut their Raft
log with snapshots (`--snapshot-every`, 5,000 entries), and one that fell behind is sent a snapshot.

The leader has one thing on its way to each replica at a time, a snapshot or a batch of log records (a batch takes no
further record once it holds 4 MB), over a connection of its own; heartbeats go on meanwhile. It is given 2 seconds and
a second more for each megabyte before it is sent again. Neither a snapshot nor a record can be larger than 256 MB.

A log record holds what changed ([storage.md](storage.md)): an `insert` replicates its rows, an `update` the columns it
assigns (the first change to a table after a replica starts replicates it whole). A `delete` or a re-sort always does,
so on a table of many megabytes it takes correspondingly long, and can time out.

A replica keeps its Raft state in `raft.log`, to which records are added and which is written afresh when it is cut, and
`raft.snap`, the latest snapshot. It takes over the single `raft.bin` of earlier versions, which cannot read the new
state or the new log records: **stop all replicas of a shard, upgrade them, and start them again.** Do not run versions
side by side in one shard.

To add a replica, first make its certificate and start it, with `--join` and the addresses of the others (`--join`
without `--peers` is refused: the replica could not answer the leader):

```
kix-cluster --make-certs certs s0r4
kix-shard --id 0 --listen 10.0.0.5:50051 --data s0r4 --replica 4 --join --addr 10.0.0.5:50051 \
    --peers 1=10.0.0.1:50051,2=10.0.0.2:50051,3=10.0.0.3:50051 \
    --node-ca ca.pem --node-cert s0r4.pem --node-key s0r4.key
```

Then tell the control node (if it answers that the replica did not catch up in time, as it may when the snapshot takes
long, the replica goes on receiving it; run the command again to finish):

```
SHARDS                                  / shard, address, role, term, applied, memory
REPLICAS 0                              / members of shard 0
ADD REPLICA 0 4 10.0.0.5:50051
REMOVE REPLICA 0 1
```

The control node follows membership changes made through it, and learns a new leader's address when a replica redirects
to it; the state file records both. If a control node cannot reach a group at all (every address it knows is gone), start
it with a `--config` that lists a current replica: its addresses are added to those in the state file. The number of
shards must match.

## When a shard does not answer

* **A node or link gone silent:** while a request is open, nodes ping one another; a connection that answers nothing for
  15 seconds is given up. A read of a replicated shard then tries its other replicas.
* **A shard that takes too long:** `kix-cluster --request-timeout SECS` (300; 0: no limit; from Rust,
  `Cluster::set_request_timeout`) fails a statement whose shard has not answered in that time, and statements waiting
  behind it go on. Raise it if queries or loads legitimately take longer. Requests for status and metrics get at most 10
  seconds.
* **A change that fails this way may still be made:** the error says so. The shard goes on with what it was doing, and
  other shards may already have applied their part. Check the data before running the statement again.
* A replica whose engine is stuck can remain the leader, since replication goes on without the engine: its requests fail
  until it is restarted.

## Operations

* **Stopping a shard:** on Ctrl-C or SIGTERM it refuses new work, lets requests finish (`--drain-secs`, 15), writes a
  checkpoint and exits.
* **Metrics:** `--metrics-listen HOST:PORT` on either binary serves `/metrics` (Prometheus text) and `/health`.

## Limits

* **A write that spans shards is not atomic.** There is no two-phase commit: if a shard fails while a statement is being
  applied, other shards keep their part. This includes the redistribution after a gathered change.
* **Every node is trusted fully.** A certificate of the cluster's authority gives full access to every shard, and
  certificates cannot be revoked one by one (see [Security](#security)).
* **The control node is a single point of failure**, and gathering needs memory there for the whole table.
* **The number of shards is fixed** when the cluster is created. There is no rebalancing.
* **Floating point sums** added in two phases can differ from one server's in the last digits.
* A shard runs one request at a time.
* A change that is not an append or an assignment to some columns (a `delete`, a re-sort) is logged and replicated as the
  whole new value of the table's slice (see [storage.md](storage.md)). A replica's log is held in memory until it is cut.
* Tables move between shards (shuffle and broadcast) as kdb+ IPC bytes inside Flight actions; query results stream as
  Arrow batches.
