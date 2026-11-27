# IPC, TLS and shell commands

`kix-serve`, `kix -p`, `kix-gui` and `kix-cluster` speak the kdb+ binary IPC protocol (version 3): the
`user:password` handshake, synchronous, asynchronous and response messages, error replies, and message compression.
Compatibility is by the published format; it has not been tested against kdb+ itself. Enumerations, foreign keys and
linked columns are sent as their plain values.

## Server

```
kix-serve [DATA_DIR] [-p PORT] [--bind ADDR] [-u USER:PASSWORD] [--tls-cert FILE --tls-key FILE | --tls] [--force-tls]
          [--insecure-clients] [--no-sync] [--max-queue MB] [--allow-shell]
```

It listens on `127.0.0.1:5000` by default. **Without `-u` (or `KIX_AUTH=user:password`) any client is accepted**, and a
client can run any q, including reading and writing files. So `kix-serve` and `kix-cluster` listen on an address other
machines can reach only with a password **and** `--force-tls` (with `--tls` or `--tls-cert` and `--tls-key`).
`--insecure-clients` lifts this, with a warning; use it only where something else keeps strangers away from the port.
The shell (`kix -p`) and `kix-gui` listen on this machine only. A handshake with a line break in it is refused, so
that a web page cannot have a browser send q to a server on its machine as the body of an HTTP request.

Compressed messages are always accepted. Replies over 2,000 bytes are compressed for clients on other machines.

## Callbacks

A function assigned to one of these names is called by `kix-serve`, `kix -p` and `kix-gui`:

| Name | Called with | When |
| --- | --- | --- |
| `.z.pw` | user (symbol), password (string) | a client connects, after `-u`; only `1b` lets it in |
| `.z.po`, `.z.pc` | the client's handle | after a client is let in, and after it leaves (`.z.pc` also when a server that was connected to closes) |
| `.z.pg`, `.z.ps` | the message | a synchronous or asynchronous request, in place of evaluating it (`.z.ps` also what a server sends, and what `-11!` replays) |
| `.z.bm` | `(handle; bytes)` | a message cannot be read; the connection is then closed |
| `.z.ts` | the time (UTC timestamp) | every `\t` milliseconds, in `kix` and `kix-serve` |
| `.z.vs` | name, `()` | a global in the root namespace is set (the indices of an amend are not given) |
| `.z.pi` | the line | in the shell, in place of evaluating what is typed (not system commands) |
| `.z.exit` | the exit code | the shell ends (`kix-serve`, which is stopped by a signal, does not call it) |

In a callback `.z.w` is the client's handle, `.z.a` its address and `.z.u` its user. `\x .z.pg` restores a default.
What `show`, `0N!` and `-1` print in a callback, or while a client's request runs, goes to the console of the process.

* **While any request has a callback to run, it runs alone:** with `.z.pg` defined, queries no longer run at the same
  time as each other (see [Concurrency](#concurrency)).
* **Callbacks are variables,** so a durable workspace keeps them. A `.z.pw` that lets nobody in locks every client out:
  open the data directory with `kix` and remove it with `\x .z.pw`.
* **They are not a sandbox.** `.z.pg` replaces evaluation only for synchronous requests; define `.z.ps` too to control
  asynchronous ones. Any client let in can otherwise run any q, including changing the callbacks.
* `.z.pw` does not count as the password that listening beyond this machine requires.

## Sending to clients

A server can send to a connected client at any time: from a request, a callback or the timer.

```q
subs: `int$()
sub: {subs:: distinct subs, .z.w}              / a client asks to hear
.z.pc: {subs:: subs except x}
.z.ts: {-25!(subs; (`upd; .z.p))}              / one message, encoded once, to each of them
\t 1000
neg[first subs] "hello"                         / to one
hclose first subs                              / send what waits, then disconnect
```

* **Only asynchronously:** `neg[h] msg`. A positive client handle (`h msg`) signals `'nyi`. `neg[h][]` is accepted
  and does nothing, as nothing is held back.
* **Sending does not wait.** Each connection has a queue and a thread that writes it, so a client that reads slowly
  holds up no one else. What a request sends to its own client arrives before the answer. `.z.W` gives the bytes that
  wait for each handle.
* **A client that falls behind is disconnected** when more than 256 MB of messages wait for it (`kix-serve --max-queue
  MB`, 0 for no limit; from Rust, `IpcConfig::queue_limit`), and `.z.pc` is called. Answers to its requests do not
  count. kdb+ has no such limit.
* `hclose` of a client's handle does not call `.z.pc`. A client may be given 10 seconds to take what was queued.
* On Windows the two threads of a connection that the server dropped end only when the client closes it or reads; the
  handle and the queued messages are released at once.
* Any client that is let in can send to any other, which may run what it receives. Define `.z.pg` and `.z.ps` to
  restrict it.
* A cluster's control node does not send to its clients (`'nyi`).

## Putting off an answer

While it answers a client's synchronous request, a server may put the answer off and give it later, from another request
or the timer:

```q
.z.pg: {$[x ~ "wait"; [waiting:: .z.w; -30!(::)]; value x]}
-30!(waiting; 0b; 42)                          / the answer
-30!(waiting; 1b; "gave up")                   / or an error
```

`-30!(::)` outside such a request is `'domain`, as is an answer for a handle that is owed none. A request that signals
an error after `-30!(::)` is answered with the error at once.

## Tickerplant systems

The scripts of KX's [kdb-tick](https://github.com/KxSystems/kdb-tick) (`tick.q`, `tick/u.q`, `tick/r.q`) have been run
unchanged, with a schema in `tick/sym.q`, through publishing, replay and an end of day. kixDB does not include them. In
the directory that holds them, with a directory `sym` for the history:

```
kix tick.q -p 5010 -- sym .                    # the tickerplant (add -t 1000 to publish in batches)
kix sym -p 5012                                # the history
kix tick/r.q -p 5011 -- :5010 :5012            # the real-time database
```

* A script's own arguments come after `--`.
* `kix` goes on serving when its input ends while it listens or its timer runs, so it can be started in the background
  with `</dev/null`.
* The history is opened as a durable workspace, which writes `kix.wal` and `kix.lock` into `sym`.
* Logs are [kixDB's own](storage.md#logs): one that a kdb+ tickerplant wrote cannot be replayed.
* A server cannot call a client and wait for the answer (`h msg` of a client's handle is `'nyi`).

## Shell commands

`\cmd` and `system "cmd"` run what is not one of q's own commands in the system's shell (`cmd /C` on Windows, else
`sh -c`) and give the lines it prints. `\cd` gives the directory commands run in, which is where relative paths start:
the workspace's directory, else the process's, until `\cd dir` chooses another (for every session of the workspace).

`\cd dir` and `\p port` act on the whole process, so they work only from the console of `kix` (or of the desktop app's
`--embedded` workspace) and its startup scripts. Anywhere else they signal `'access`, with `--allow-shell` too: in a
client's messages, the timer, connection callbacks and what a server sends. So `kix-serve` and a cluster never run them.
Without an argument both can be asked for anywhere. What a client defined does set them if the console calls it. From
Rust, `server.connect().at_console()` may.

```q
system "ls data"                    / or \ls data
```

* **A process that serves clients runs none** (`'access`) unless it was started with `--allow-shell` (`kix-serve`,
  `kix`, `kix-gui`; from Rust, `IpcConfig::allow_shell`). This holds for the whole process, its console and timer
  included, because a client can define what they run. A `kix` shell that does not listen, and the desktop app's
  `--embedded` workspace, run them. A library `Server` runs none until `allow_shell_commands(true)`.
* **With `--allow-shell`, every client that is let in can run any program** as the user running the server. Give `-u`,
  and keep strangers off the port.
* **Without it a client is still not confined:** q reads and writes any file that the server's user can, which can be
  turned into running a program (a start-up script, say). Let in only clients you trust.
* A durable workspace keeps functions and callbacks: what a client left there runs shell commands if the directory is
  later opened by a process that allows them.
* A command holds the workspace until it ends, and cannot be stopped from q. To leave a program running, start it in
  the background with its output sent elsewhere (`system "prog >log 2>&1 &"`; on Windows, `start /b`).
* A command that fails signals `'os`; what it prints as errors goes to the process's console. `system` does not pass
  q's own commands to the shell: those are `\ts`, `\cd` and any single character.
* A cluster's nodes run none.

## Concurrency

Statements that only read run side by side, each on the data as it was when it started, and a change does not wait for
them. A statement that changes anything runs alone. So does one that may: it assigns a name, calls a function or a view,
applies a number (a connection handle) or `neg h` to something, or uses a verb such as `insert`, `upsert`, `set`, `get`, `value`,
`eval`, `system`, `show` or `hopen`. A script runs beside others only if all its statements do. A change made
while a read is running first copies the list of global variables, which takes time in proportion to their number (about
0.01 ms with 1,000 and 3 ms with 100,000).

Work on 100,000 items or more (comparisons, arithmetic, `like`, `where`, sums, `min`, `max`, `avg`, grouping with few
groups, and `by` queries that go group by group) is shared among threads. kixDB's programs use one for each processor
core, even where a core runs two: using both took 45-68% more processor time a query and answered no faster
([measurements](benchmark-results.md#threads-and-keyed-tables)). `RAYON_NUM_THREADS` sets how many. (A program that embeds the engine has
rayon's setting, a thread for each that the processor runs, unless it calls `kixdb::engine::par::one_thread_a_core`.) A sum of that many floating point numbers can differ in its last digits from one added in order, but is the same
with any number of threads.

## Client

```q
h: hopen `:localhost:5000
h "select from trade"
h (`upsert; `trade; (09:30:00; `AAPL; 180.0; 100))     / a functional message
(neg h) "x: 1"                                          / asynchronous
hclose h
```

Handles belong to the process, as in q: every session and client of it can use one, with the login that opened it, and
it stays open until `hclose`.

`hopen` takes `` `:host:port ``, `` `:host:port:user:password ``, `` `::port `` or a port number on this machine
(`tcps://` before the host for TLS). Without a user and password it sends the current user name with an empty password. A
name without a port is a file's ([storage.md](storage.md#files-and-tables-on-disk)); earlier versions took
`` `:host `` to be port 5000. From Rust, `kixdb::ipc::IpcClient::connect_addr(addr, user,
password)` connects with one.

### Receiving

`kix` and `kix-serve` run what a server sends while they do nothing else, so a subscriber only has to define what is
called:

```q
trade: ([] sym:`symbol$(); price:`float$())
upd: {[t; rows] t insert rows}
h: hopen `:localhost:5000
h "sub[]"                                             / the publisher then sends (`upd; `trade; rows)
.z.pc: {if[x = h; show "the publisher has gone"]}
```

* A message is run as a client's asynchronous one is: by `.z.ps` if there is one, with `.z.w` the handle, so that
  `neg[.z.w] msg` replies. An error is printed, and the connection stays.
* Messages from one server are run in order, one at a time, between statements and timer calls. When the process falls
  behind they wait at the server, which in kixDB disconnects a client that falls too far behind.
* When the server closes the connection, `.z.pc` is called with the handle, which is then closed.
* `h[]` waits for the next message instead, which allows a deferred synchronous call. **Ask for it in the same
  statement or function**, or the message may be run before `h[]` is:

```q
(neg h) "neg[.z.w] (`result; 6*7)"; h[]                / (`result;42)
```

* `h[]` (or `h (::)`) waits as long as it takes, holding the workspace meanwhile, as any synchronous call does.
* A synchronous request from the server is refused (`'nyi`).
* The desktop app and library `Server`s run nothing by themselves: `h[]` takes the messages. From Rust,
  `Server::start_background` runs them (and the timer), and `IpcClient::receive` takes one.

## TLS

With `--tls-cert` and `--tls-key` (PEM), or `--tls` (a new self-signed certificate, written to `kix-cert.pem`), the
server accepts TLS and plain connections on the same port. It does not refuse plain ones, so a password sent by a
plain client is readable on the network.

A client connects to `tcps://host:port` and trusts the certificates in the file named by `KIX_SSL_CA_CERT_FILE`:

```
KIX_SSL_CA_CERT_FILE=kix-cert.pem kix --connect tcps://127.0.0.1:5000 -U ann -P secret
```
