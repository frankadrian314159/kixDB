# Shell and desktop app

## kix

```
kix [DATA_DIR | SCRIPT.q] [-D dir] [-p port] [--connect host[:port]] [-U user] [-P password] [-Q expr]... [-i script.q]...
```

Without a data directory the workspace is in memory. `-Q` (also `-q`, `-c`) and `-i` (also `-f`) run an expression or a
script and exit. `-p` serves the workspace over IPC on the loopback address, without a password. Input continues over
lines while a bracket, brace or string is open.

q's `\` commands work (see [language.md](language.md#system-commands)), plus `\p [port]` and `\\` to quit.

| Command | Does |
| --- | --- |
| `.tables` `.views` `.functions` `.vars` | list objects |
| `.meta TABLE` | columns, types, keys, attributes |
| `.schema [NAME]` | q definition of an object |
| `.import [--new] FILE TABLE` | load CSV |
| `.export FILE QUERY` | write a query's result as CSV |
| `.read FILE` | run a script |
| `.plan QUERY` | how a query would run |
| `.timer on\|off` | time each command |
| `.storage` `.checkpoint` | storage statistics, write a snapshot |
| `.help` `.quit` | help, exit |

With `--connect` (`tcps://host:port` for TLS), expressions as well as `.import`, `.schema` and `.plan` run against the
remote server or `kix-cluster` gateway.

## kix-gui

```
cargo run --release --features gui --bin kix-gui -- [DATA_DIR] [--embedded]
cargo run --release --features gui --bin kix-gui -- --connect [tcps://]HOST[:PORT] [-U USER] [-P PASSWORD] [--tls] [--tls-ca FILE]
```

Without `--connect` it uses a server already listening on `127.0.0.1:5000` (credentials from `KIX_USER` and
`KIX_PASSWORD`), or starts one in its own process with a generated password. `--embedded` opens no port.

* **Navigation:** namespaces, filter, tables, views, functions, dictionaries and variables.
* **Datasheet:** scrollable grid with a row limit, q filter and column sorting; editing, adding and deleting rows of
  keyed tables.
* **Meta / design:** column types, keys, attributes and a script of the table.
* **Query pad:** `Ctrl+Enter` or `F5` runs the selection or the whole pad in the background; result grids, console
  output and plan view.
* **Data:** CSV import and export, streaming bulk load, and a sample tick database.
