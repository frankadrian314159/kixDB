# IPC and TLS

`kix-serve`, `kix -p`, `kix-gui` and `kix-cluster` speak the kdb+ binary IPC protocol (version 3): the
`user:password` handshake, synchronous, asynchronous and response messages, error replies, and message compression.
Compatibility is by the published format; it has not been tested against kdb+ itself.

## Server

```
kix-serve [DATA_DIR] [-p PORT] [--bind ADDR] [-u USER:PASSWORD] [--tls-cert FILE --tls-key FILE | --tls] [--no-sync]
```

It listens on `127.0.0.1:5000` by default. **Without `-u` (or `KIX_AUTH=user:password`) any client is accepted**, and a
client can run any q, including reading and writing files. Set a password before binding to another address.

Compressed messages are always accepted. Replies over 2,000 bytes are compressed for clients on other machines.

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
