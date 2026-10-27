# Rewind

Rewind is a Go project for recording, storing and replaying AI calls. Its storage
engine is a learning project: data formats, indexes, recovery and their costs will
be implemented and measured explicitly.

The CLI currently provides help and version output. An internal ingest coordinator
exercises immutable writes, ownership and read permissions with controlled test
backends. Internal adapters cover PostgreSQL ownership registration and immutable R2
objects; record publication still uses controlled stores. These adapters are not
yet joined into a remotely durable service. Real authentication, trace queries and replay are not implemented.
The design requires remote durability before
acknowledging accepted data and distinguishes recorded playback from running an external model again.

## Getting started

Install Go 1.27 or newer and Git, then run:

```sh
go run ./cmd/rewind version
make check
make build
```

The first command prints `rewind dev`. The build produces `bin/rewind`; it currently
supports only help and version output. It does not start a server or contact a
model, database or object store.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contributor setup and checks, and the
[architecture direction](engineering/ARCHITECTURE.md) for planned boundaries.
