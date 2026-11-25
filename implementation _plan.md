kixDB — Architecture & Staged Implementation Plan

kixdb is a clean-room Rust implementation of KX's kdb+ columnstore database and its q array/time-series query language (including qSQL), organized along the lines of tsql-server with a single-node root crate, interactive CLI, binary IPC server, eframe/egui desktop GUI, and a distributed Raft + Arrow Flight cluster workspace in crates/parallel/.

IMPORTANT

Key adaptations from tsql-server to kdb+/q:

Columnar vector storage (not 8 KiB row B+trees): Tables are native column dictionaries (Flip: symbol list mapped to contiguous typed vectors) with kdb+ attributes (`s# sorted, `u# unique, `p# parted, `g# grouped), persisted as flat files, splayed directories, date/int partitioned HDBs, and a checksummed append-only tickerplant/WAL log.
Order-preserving distribution: Because q arrays and time-series queries (first, last, prev, deltas, mavg, aj, wj) depend on row order, sharded tables partition by entity key (e.g., `sym) while preserving intra-shard row/time order, alongside date/time partition awareness, replicated reference tables, and a control-node gather fallback for exactness.
Dual wire protocol: Clients, kix, and kix-gui speak native kdb+ binary IPC (default port 5000), while kix-control and kix-storage shards communicate over Arrow Flight gRPC (mirroring tsql-server).
System Architecture
Workspace & Crate Layout

Mirroring Cargo.toml, cargo build and cargo test at the root build the standalone single-node package; --workspace includes the distributed cluster crates:

Path	Target / Crate	Responsibility
Cargo.toml, src/lib.rs	kixdb (lib)	Server (new, open, replication & snapshot hooks) and Connection (execute, eval, query, explain)
src/main.rs	kix (bin)	Interactive q) REPL, script runner, -p listener mode, remote --connect, \ system commands & . commands
src/serve.rs	kix-serve (bin)	Standalone TCP server speaking kdb+ binary IPC v3 (port 5000, optional -u auth & rustls TLS)
src/gui/	kix-gui (bin, features = ["gui"])	eframe/egui (0.32.3) desktop app with Backend trait (embedded or remote IPC), Navigation, Datasheet, Meta Designer, Query Pad, CSV/Bulk import, sample tick DB
crates/parallel/raft	kix-raft (lib)	Pure state-machine Raft consensus (leader election, log replication, snapshots, learner-to-voter membership, file/mem log)
crates/parallel/shared-ces	kix-shared-ces (lib)	ClusterConfig, postcard Work/Fragment/RunReply, K <-> Apache Arrow (60) batch conversion, 1024-slot key hash & partition router, Prometheus metrics
crates/parallel/storage	kix-storage / kix-shard	Single-node kixdb::Server behind arrow-flight (60) / tonic (0.14), Raft Replica, shard-to-shard shuffle/load, graceful drain
crates/parallel/control	kix-control / kix-cluster	Distributed q/qSQL planner (route, scatter-gather, co-located aj/lj/ij, broadcast/shuffle joins, gather), cluster.state, and kdb+ IPC gateway
Core Design Details
1. Data Model & Columnar Vectors (src/types.rs, src/sym.rs)
Interned Symbols (SymPool): Global thread-safe interner mapping symbol names to SymId(u32) for O(1) equality, hashing, and enum persistence.
K Value Representation:
Atoms (negative kdb+ type numbers): Bool(-1), Guid(-2), Byte(-4), Short(-5), Int(-6), Long(-7), Real(-8), Float(-9), Char(-10), Sym(-11), Timestamp(-12) (i64 ns since 2000.01.01), Month(-13), Date(-14) (i32 days since 2000.01.01), Datetime(-15), Timespan(-16), Minute(-17), Second(-18), Time(-19) (i32 ms), Null.
Typed Vectors (ColVec, types 1..=19) & Mixed Lists (0): Contiguous Rust Arc<Vec<T>> with copy-on-write (Arc::make_mut) mutation and an attached Attr (None, Sorted, Unique, Parted, Grouped) plus optional lazy index structure (hash map / parted offset table).
Null & Infinity Sentinels: Faithful to kdb+ (0N = i64::MIN, -0W = i64::MIN + 1, 0W = i64::MAX for long; i32::MIN for int/date/time; f64::NAN / ±INFINITY for float; empty symbol for `).
Dictionaries (99), Tables (98 Flip), and Keyed Tables (99 Flip -> Flip): Unkeyed table = ordered Vec<SymId> column names + equal-length Vec<ColVec>. Keyed table = KeyedTable { keys: Flip, vals: Flip }.
Callables (100+): Lambdas {[p1;p2] body} (defaulting to implicit x, y, z), projections f[1;], unary/binary primitives, and adverb-derived verbs (', /, \, ':, /:, \: — each, over, scan, each-prior, each-right, each-left).
2. q & qSQL Language Surface (src/lexer.rs, src/parser.rs, src/engine/)
Syntax & Evaluation: Right-to-left (right-of) expression evaluation, ; statement/list separators, local (:) vs global (::) assignment, indexed/amend assignment (x[i]:v, @[x;i;f;y], .[x;p;f;y]), conditionals ($[cond;t;f]), do[n;...], while[c;...], signal 'err, protected evaluation @[f;x;handler], views (v::expr), and namespaces (.q, .Q, .z, custom .ns).
Vector Primitives:
Arithmetic & math (+ - * % & | ^ = <> < <= > >= ~, neg, abs, sqrt, exp, log, sin, cos, div, mod, xexp, xlog).
Structural & order (til, count, first, last, take #, drop _, sublist, enlist, raze, flip, reverse, distinct, where, group, iasc, idesc, asc, desc, xasc, xdesc, xkey, keys, cols, meta, type, null, fills, within, in, bin, binr).
Aggregations & rolling windows (sum, prd, avg, wavg, wsum, min, max, med, var, dev, svar, sdev, cov, cor, sums, prds, avgs, mins, maxs, deltas, ratios, prev, next, mavg, msum, mmin, mmax, mdev, ema).
Temporal & bucketing (xbar, .z.p, .z.P, .z.d, .z.D, .z.t, .z.T, date/time component extraction `year`mm`dd`hh`uu`ss$x).
Strings, symbols & casts (string, like, ss, ssr, vs, sv, upper, lower, trim, ltrim, rtrim, $ cast/pad/parse).
qSQL & Joins:
select [distinct] [N] [cols] [by groups] from t [where c1, c2, ...] (sequential where filtering with virtual i column; vector grouped aggregation; auto-keyed result when by is present).
exec, update, and delete with both in-memory value semantics (when t is a table expression) and in-place workspace mutation (when `t is passed by symbol or updated in update ... fromt`).
Functional forms ?[t;c;b;a] and ![t;c;b;a].
Joins: ij, lj, uj, ej, pj, aj / aj0 (as-of join), and wj / wj1 (window join).
3. Storage & Persistence (src/storage.rs, src/persist.rs)
Flat & Splayed Files: `:path set val / get `:path; `:dir/t/ set t writes .d + per-column binary vector files + sym enumeration file (.Q.en).
Partitioned HDB: Directories dir/<partition>/<table_name>/ (where <partition> is YYYY.MM.DD, YYYY.MM, YYYY, or integer); \l dir mounts partitioned tables lazily, pruning partitions from where date = ... / date within ... and loading only referenced columns.
Durable Workspace (Server::open(dir)): Checksummed write-ahead mutation log (kix.wal) + atomic checkpoint (kix.snap) + file lock, plus replication hooks (set_replicator, apply_replicated, export_state, install_state) identical in role to src/lib.rs.
4. Binary IPC, CLI & Desktop GUI (src/ipc/, src/main.rs, src/gui/)
kdb+ Binary IPC v3 (src/ipc/): Standard [endian, msg_type, compressed, 0, total_len_u32] header + kdb+ type serialization (-8! / -9!) + block compression, TCP handshake (username:password\x03\x00 -> capability byte 3), sync (1), async (0), and response (2) messages, plus q handle expressions (h:hopen `:localhost:5000; h "select from trade").
kix CLI (src/main.rs): Interactive q) prompt, multi-line {...} / (...) / [...] continuation, -p PORT background IPC listener, --connect host:port, -q/-Q expression flags, -f/-i script files, kdb+ \ commands (\a, \b, \v, \f, \d, \l, \t, \ts, \p, \w, \\) and tsql-style . commands (.help, .tables, .meta, .import, .export, .plan, .storage).
kix-gui (src/gui/): eframe/egui app structured like tsql-gui:
Connects to 127.0.0.1:5000, starts an in-process IPC server there if free, connects via --connect host:port (including kix-cluster), or runs --embedded.
Navigation Pane: Namespace picker + filter + Tables (Unkeyed / Keyed / Splayed / Partitioned), Views, Functions, Dictionaries, Variables.
Datasheet: Virtualized scrollable grid (egui_extras::TableBuilder), row limit selector, q where filter, click-to-sort (xasc/xdesc), inline edit / add row / delete row for keyed tables.
Meta / Schema View: Column types, keys (xkey), attribute controls (`s#, `u#, `p#, `g#), storage layout, script generator.
Query Pad: q script editor (Ctrl+Enter / F5 runs selection or buffer on worker Session), Explain plan view, result grids + formatted K console output, CSV import/export, streaming bulk CSV loader, and a built-in Sample Tick Database (instrument, trade, quote).
5. Distributed Cluster (crates/parallel/)
Distribution Modes (declared via .kix.dist[\trade; `sharded; `sym],.kix.dist[`inst; `replicated],.kix.dist[`trade; `partitioned; `date], orDISTRIBUTE` helper commands):
Sharded { key } (default: sym or first key column): 1024-slot stable hash on key column while strictly preserving insertion/time order within each shard.
Partitioned { col } (date/time/int): Routes partitions across shards (or co-partitions date + sym) for HDB/RDB scale-out.
Replicated: Reference tables (instrument, exchange) stored identically on every shard.
Distributed Execution Plans (kix-control):
route: where sym = \AAPL(orsym in ... mapping to 1 shard) executes whole on that shard — including stateful order-dependent functions (deltas,mavg,prev,first,last`)!
scatter-gather (shard-local by sym & two-phase vector agg):
Queries grouped by the shard key (select vwap: size wavg price, hi: max price by sym from trade or by sym, 5 xbar time.minute) compute completely on each shard with zero cross-shard key overlap and concatenate on the control node!
Non-co-located aggregations (select sum size, avg price by exchange from trade) rewrite avg/wavg/dev into combinable partial states (sum + count, sum-of-squares) on shards and merge on the control node's mirror engine.
Co-located Time-Series Joins (aj, wj, lj, ij): When trade and quote are both sharded on `sym (or joined against a Replicated table), aj[\sym`time; trade; quote]andwj` execute 100% shard-by-shard in parallel without moving rows!
Broadcast & Shard-to-Shard Shuffle: Small non-replicated tables broadcast to shards; mismatched join keys shuffle Arrow batches directly shard-to-shard (ACTION_SHUFFLE / ACTION_LOAD).
gather Baseline: Any arbitrary stateful/cross-row q expression that cannot be safely pushed down gathers the referenced columns/filtered slices into the control mirror engine so every valid q query produces exact single-node results.
Availability & Operations: kix-raft multi-replica shard groups with snapshots & membership changes, graceful shard drain, Prometheus /metrics, and a kdb+ binary IPC listener on kix-cluster so kix and kix-gui work transparently against a cluster.
Staged Execution Plan
Stage 1 — Core Columnar Engine, q / qSQL, Joins & Persistence (kixdb lib)
Scaffold root Cargo.toml, .cargo/config.toml (RUST_MIN_STACK), .gitignore.
Implement types, sym, lexer, parser, ast, engine (primitives, adverbs, qSQL, aj/wj/lj/ij/uj), storage & persist (flat, splayed, partitioned HDB, WAL + replication hooks), inspect, and tools.
Verify with integration test suites: datatypes, vectors_adverbs, qsql, joins_timeseries, persistence.
Stage 2 — kdb+ Binary IPC, kix-serve & kix CLI
Implement src/ipc/ (v3 wire codec, compression, TCP listener, client, hopen handles), src/serve.rs, and src/main.rs.
Verify with process-level CLI and live TCP IPC integration tests (ipc, cli).
Stage 3 — kix-gui Desktop Application (--features gui)
Implement src/gui/ (backend, remote, session, ui, datasheet, design, query, sample, app, main).
Verify with cargo check --features gui and headless egui view/backend tests (cargo test --features gui).
Stage 4 — Distributed Edition (crates/parallel/) & Documentation
Implement kix-raft, kix-shared-ces (Arrow conversion & slot/partition router), kix-storage (kix-shard), and kix-control (kix-cluster planner + IPC front end).
Verify with Raft network simulation tests, multi-shard Arrow Flight + IPC cluster tests, and single-node vs cluster differential equivalence tests (including aj, xbar, mavg, two-phase agg, shuffle, and gather).
Write README.md and docs/ (README.md, language.md, storage.md, ipc.md, shell-and-gui.md, distributed.md).