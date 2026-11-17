# go-micro-aerospike

Aerospike-backed `store.Store` plugin for [Go Micro](https://go-micro.dev) v6,
built on `aerospike-client-go/v8`. Works against **Aerospike Community Edition**
— no Enterprise-only feature is required. TLS and authentication are optional.

```go
import (
	microstore "go-micro.dev/v6/store"
	"github.com/aerospike/go-micro-aerospike"
)

s := aerospike.NewStore(
	microstore.Nodes("aerospike-1:3000", "aerospike-2:3000"),
	microstore.Database("agent_state"), // an existing Aerospike namespace
)
if err := s.Init(); err != nil { ... }
defer s.Close()

// Per-component tables via store.Scope — the recommended pattern.
sess := microstore.Scope(s, "", "session/"+sessionID)
sess.Write(&microstore.Record{Key: "memory", Value: data})
```

Flag-based selection (`MICRO_STORE=aerospike`, `MICRO_STORE_ADDRESS=host:port`)
is available by blank-importing the `register` subpackage:

```go
import _ "github.com/aerospike/go-micro-aerospike/register"
```

## Mapping

| Go Micro          | Aerospike                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| `Database`        | namespace (must already exist; validated on `Init`)                                                    |
| `Table`           | bin `t` on every record — **not** a set; a string secondary index on `t` serves per-table queries      |
| `Record.Key`      | bin `k` plus a length-prefixed user key `len(table):table:key`, so `(a,bc)` and `(ab,c)` never collide |
| `Record.Value`    | bin `v` inline, or chunked into the chunk set                                                          |
| `Record.Metadata` | bin `m` (map)                                                                                          |

All head records live in one data set (default `micro`); chunk records live in a
separate chunk set (default `micro_chunks`) and are never returned by listing.

## Server requirements

- A pre-existing **namespace**. The store never creates namespaces and fails on
  `Init` if the configured namespace is absent.
- A **string secondary index** on bin `t` of the data set. `Init` creates it by
  default and waits for readiness; disable with `WithCreateIndex(false)` when
  operators pre-create it. Measured index memory on Community Edition: about
  **21 bytes per record** (on top of ~64 bytes primary index). On CE the
  secondary index lives in volatile memory and rebuilds on restart — expect
  added cold-start time proportional to the set size — and index queries fan
  out to all nodes.
- `nsup-period > 0` on the namespace for **TTL expiry** to take effect.

## Listing cost model — query large tables by exact key

`List` and prefix/suffix `Read` run an index query `t == table`, then evaluate
the prefix/suffix as a server-side regex filter against **every record in the
table**. Cost is proportional to the table size, not to the number of matches.
`Offset`/`Limit` additionally collect and sort all matching keys client-side.
Selecting only the `k` bin for `List` trims network transfer but **not** storage
reads — the whole record is still read from disk, which matters for tables with
fat inline values.

> **Large tables SHALL be queried by exact key.** Prefix and suffix listing is
> intended for small control-plane tables (tasks, schedules, run indexes, auth
> records). `WithMaxListKeys` (default 100,000) bounds memory and returns
> `ErrTooManyListKeys` rather than a partial result.

## Size limits and chunking

Aerospike records are bounded by the namespace maximum record size (`max-record-size`,
**1 MiB** by default on the 8.x server line; older docs call it `write-block-size`).
The store enforces configurable caps:

| Cap                    | Default | Error                 |
| ---------------------- | ------- | --------------------- |
| key                    | 1 KiB   | `ErrKeyTooLarge`      |
| table name             | 255 B   | `ErrTableTooLarge`    |
| metadata (serialised)  | 64 KiB  | `ErrMetadataTooLarge` |
| inline value threshold | 512 KiB | —                     |
| total value            | 16 MiB  | `ErrValueTooLarge`    |

Values above the inline threshold are **chunked by default**: fixed-size chunks
(512 KiB) in the chunk set, written with a batch write under a random version id,
then the head record is committed (REPLACE). Readers see only whole committed
versions; a missing chunk triggers a bounded retry (default 2) that re-reads the
head — useful when a writer commits a new version mid-read. Overwrites delete
the previous version's chunks best-effort; `Delete` removes the head first, then
chunks. A crash can leak chunks — `Sweep()` deletes chunks no head references;
it never runs automatically. `NewStore` returns the `store.Store` interface, so
assert to the exported `Sweeper` interface to call it:

```go
if sw, ok := s.(aerospike.Sweeper); ok {
	n, err := sw.Sweep() // options apply to this call only
	...
}
```

Disable chunking with `WithChunking(false)` to get `ErrValueTooLarge` above the
inline threshold instead (leaving existing values untouched). Above the total
cap every write is rejected before anything is stored.

`Sweep()` only reclaims chunk records whose **server-side age** exceeds the
sweep grace (`WithSweepGrace`, default 60 s, measured from each chunk's last
write by the server — `0` disables the protection). Chunks belonging to an
in-flight write are therefore never deleted mid-commit; a writer's bounded
window is ~2 s under default client timeouts, so the default margin is ~30×.
Caveat: a writer stalled longer than the grace (e.g. a suspended process, or
client timeouts raised via `WithClientPolicy`) could still race a sweep —
raise `WithSweepGrace` accordingly if you increase write timeouts.

Per-turn rewrite cost grows linearly with value size — a Go Micro agent memory
value rewritten every turn pays bandwidth proportional to its full size each
turn. Keep conversation histories bounded; the 16 MiB cap exists for this reason.

## TTL / expiry

Precedence: `WriteTTL` > `WriteExpiry` > `Record.Expiry`. The resolved duration
maps to record TTL rounded up to whole seconds (minimum 1 s). **No expiry set →
TTL `-1` (never expire)** so the namespace `default-ttl` cannot silently expire
state. `Read` reports remaining TTL in `Record.Expiry`. Chunk TTL is the head
TTL plus a 60-second grace.

## Options

`store.Nodes` supplies seed hosts (`host:port`, default port 3000). Everything
else goes through typed options (carried in `Options.Context`):

`WithDataSet`, `WithChunkSet`, `WithMaxKeySize`, `WithMaxTableSize`,
`WithMaxMetadataSize`, `WithInlineThreshold`, `WithChunkSize`,
`WithMaxValueSize`, `WithChunking`, `WithMaxListKeys`, `WithCreateIndex`,
`WithAuth(user, password)`, `WithTLS(*tls.Config)`, `WithClientPolicy`,
`WithReaderRetries`, `WithSweepGrace`.

Auth and TLS are off by default (plaintext, unauthenticated). When TLS is set,
every connection uses it — no plaintext fallback. Passwords are never logged.

## Correctness notes

- Writes are single atomic `REPLACE` puts; no read-modify-write; no shared
  metadata record (table membership is the index, not a record).
- `Delete` is idempotent; exact-key `Read` returns `store.ErrNotFound`.
- Multi-record operations are **not** atomic — chunk batches save round trips
  only; correctness comes from the versioned head commit, not the batch.
  Head records carry `layout_version` so future layouts can be told apart;
  unknown versions fail reads with `ErrUnknownLayout`.
- If the index cannot be created or is missing, calls return a
  `MissingIndexError` naming the `asinfo` command to create it — never a silent
  namespace scan.

## Development

```bash
make fixture-up        # Aerospike CE via docker compose (namespaces test, test2)
make test              # unit tests
make test-integration  # full suite against the fixture
                       #   (AEROSPIKE_ADDR=127.0.0.1:3100 — compose publishes 3100)
make test-unit-race         # unit tests under -race
make test-integration-race  # integration suite under -race
make coverage               # labeled per-lane coverage reports (.coverage/)
scripts/ci.sh               # build + vet + unit + integration
```

Requires Go 1.26.5 or later and Docker Compose v2 for integration tests.
The Makefile defaults `AEROSPIKE_ADDR` to `127.0.0.1:3100`; override it to use
another disposable test server. The example also uses the Compose port 3100.

GitHub Actions runs formatting, dependency verification, builds, vet, unit
and race tests, and Community Edition integration tests. The template's
artifact-signing/publishing workflow remains disabled; Go consumers install
this library directly by module path.

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository tooling and standards
and [SECURITY.md](SECURITY.md) for vulnerability reporting.

### Secured lane (optional, Enterprise eval)

```bash
make secured-up      # EE TLS/auth fixture: generates certs, provisions users
make test-secured    # TLS/mTLS/auth tests (fails if fixture is missing)
make secured-down    # teardown
```

The secured lane uses `aerospike/aerospike-server-enterprise:8.2.0.1` (eval
feature key baked into the image — no entitlement material committed) on
127.0.0.1:4533. Normal `make test` runs do not require it and skip secured
tests with an explicit reason.

Verified against Aerospike Community Edition 8.1.2.4 (single node, Docker).

## License

Apache 2.0 — see `LICENSE`.
