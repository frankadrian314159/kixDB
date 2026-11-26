# IPC and TLS

`kix-serve`, `kix -p`, `kix-gui` and `kix-cluster` speak the kdb+ binary IPC protocol (version 3): the
`user:password` handshake, synchronous, asynchronous and response messages, error replies, and message compression.
Compatibility is by the published format; it has not been tested against kdb+ itself. Enumerations, foreign keys and
linked columns are sent as their plain values.

## Server

```
kix-serve [DATA_DIR] [-p PORT] [--bind ADDR] [-u USER:PASSWORD] [--tls-cert FILE --tls-key FILE | --tls] [--force-tls]
          [--insecure-clients] [--no-sync]
```

It listens on `127.0.0.1:5000` by default. **Without `-u` (or `KIX_AUTH=user:password`) any client is accepted**, and a
client can run any q, including reading and writing files. So `kix-serve` and `kix-cluster` listen on an address other
machines can reach only with a password **and** `--force-tls` (with `--tls` or `--tls-cert` and `--tls-key`).
`--insecure-clients` lifts this, with a warning; use it only where something else keeps strangers away from the port.
The shell (`kix -p`) and `kix-gui` listen on this machine only.

Compressed messages are always accepted. Replies over 2,000 bytes are compressed for clients on other machines.

## Callbacks

A function assigned to one of these names is called by `kix-serve`, `kix -p` and `kix-gui`:

| Name | Called with | When |
| --- | --- | --- |
| `.z.pw` | user (symbol), password (string) | a client connects, after `-u`; only `1b` lets it in |
| `.z.po`, `.z.pc` | the client's handle | after a client is let in, and after it leaves |
| `.z.pg`, `.z.ps` | the message | a synchronous or asynchronous request, in place of evaluating it |
| `.z.bm` | `(handle; bytes)` | a message cannot be read; the connection is then closed |
| `.z.ts` | the time (UTC timestamp) | every `\t` milliseconds, in `kix` and `kix-serve` |
| `.z.vs` | name, `()` | a global in the root namespace is set (the indices of an amend are not given) |
| `.z.pi` | the line | in the shell, in place of evaluating what is typed (not system commands) |
| `.z.exit` | the exit code | the shell ends (`kix-serve`, which is stopped by a signal, does not call it) |

In a callback `.z.w` is the client's handle, `.z.a` its address and `.z.u` its user. `\x .z.pg` restores a default.
What `show` and `0N!` print in a callback goes to the console of the process.

* **While any request has a callback to run, it runs alone:** with `.z.pg` defined, queries no longer run at the same
  time as each other (see [Concurrency](#concurrency)).
* **Callbacks are variables,** so a durable workspace keeps them. A `.z.pw` that lets nobody in locks every client out:
  open the data directory with `kix` and remove it with `\x .z.pw`.
* **They are not a sandbox.** `.z.pg` replaces evaluation only for synchronous requests; define `.z.ps` too to control
  asynchronous ones. Any client let in can otherwise run any q, including changing the callbacks.
* `.z.pw` does not count as the password that listening beyond this machine requires.
* Nothing can be sent to a client other than the answer to its request: `neg[.z.w] msg` signals `'nyi`.

## Concurrency

Statements that only read run side by side, each on the data as it was when it started, and a change does not wait for
them. A statement that changes anything runs alone. So does one that may: it assigns a name, calls a function or a view,
applies a number (a connection handle) to something, or uses a verb such as `insert`, `upsert`, `set`, `get`, `value`,
`eval`, `system`, `show` or `hopen`. A script runs beside others only if all its statements do. A change made
while a read is running first copies the list of global variables, which takes time in proportion to their number (about
0.01 ms with 1,000 and 3 ms with 100,000).

Work on 100,000 items or more (comparisons, arithmetic, `like`, `where`, sums, `min`, `max`, `avg`, grouping with few
groups, and `by` queries that go group by group) is shared among the processor's threads; `RAYON_NUM_THREADS` sets how
many. A sum of that many floating point numbers can differ in its last digits from one added in order, but is the same
with any number of threads.

## Client

```q
h: hopen `:localhost:5000
h "select from trade"
h (`upsert; `trade; (09:30:00; `AAPL; 180.0; 100))     / a functional message
(neg h) "x: 1"                                          / asynchronous
hclose h
```

`hopen` sends the current user name with an empty password. From Rust, `kixdb::ipc::IpcClient::connect_addr(addr, user,
password)` connects with one.

## TLS

With `--tls-cert` and `--tls-key` (PEM), or `--tls` (a new self-signed certificate, written to `kix-cert.pem`), the
server accepts TLS and plain connections on the same port. It does not refuse plain ones, so a password sent by a
plain client is readable on the network.

A client connects to `tcps://host:port` and trusts the certificates in the file named by `KIX_SSL_CA_CERT_FILE`:

```
KIX_SSL_CA_CERT_FILE=kix-cert.pem kix --connect tcps://127.0.0.1:5000 -U ann -P secret
```
