# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

This is a fork of `github.com/mikespook/gearman-go`. Several defects in the
`client` package are known and unfixed — do not "discover" them again or work
around them silently:

- `client.Client` shares `conn`, `rw` and `ErrorHandler` across goroutines with
  almost no synchronisation. `client/race_test.go` reproduces this; the three
  `TestRace*` cases fail every run under `-race`, though the number of reports
  varies, so trust pass/fail rather than a count.
- `Pool.Do`/`DoBg`/`Status`/`Echo` deadlock on the first call — they take the
  embedded `Client.Mutex`, then call through to `do`, which takes it again.
- `Client.Status` and `Client.Echo` write without the mutex and block forever
  if no response arrives.

## Building and testing

`go.mod` declares `module github.com/sergle/gearman-go`, matching `origin`
(`git@github.com:sergle/gearman-go.git`), **not** the upstream `mikespook`
path. Upstream has no `go.mod` at all — it is a GOPATH-era repo — so do not
delete it or every `go` command goes back to failing with "directory prefix .
does not contain main module".

Consumers can either `require github.com/sergle/gearman-go` directly, or keep
their existing `mikespook/...` imports via
`replace github.com/mikespook/gearman-go => github.com/sergle/gearman-go <ver>`.
That module-form replace only works because go.mod declares the fork's own
path; declaring the upstream path would restrict consumers to a filesystem
`replace`.

The library module has no dependencies and no `go.sum`; `example/` carries its
own module for that reason (see below).

Use the Makefile; `make help` lists everything.

```sh
make check                           # build + vet + fmt-check + test + examples
make test                            # default suite, no gearmand
make knownbugs                       # known unfixed defects — expected to FAIL
make reproducers                     # the three race reproducers — expected to FAIL
go test -run TestClientDo ./client    # single test
```

`make check` is the gate that must pass today. `make race`, `make knownbugs`
and `make reproducers` are expected to fail until the client races are fixed —
that is the point of them, so do not "fix" them by weakening the test.

`go vet ./...` reports two pre-existing `unreachable code` findings
(in `client/client.go` and `client/pool_test.go`) and therefore exits
non-zero. `make vet` filters exactly those and fails on anything new, so prefer
it over calling `go vet` directly.

### Two modules

`example/` is a **separate module** (`github.com/sergle/gearman-go/example`),
so root `./...` resolves to just `gearman-go`, `client` and `worker`. That is
deliberate: `example/worker` needs `github.com/mikespook/golib`, which drags in
`mgo.v2`, `yaml.v2` and `check.v1`. Keeping it out means the library module has
**zero dependencies and no `go.sum`**, and root `go mod tidy` is a no-op.

Consequences:

- Changing the examples means `cd example` first; they are invisible to every
  `go` command run from the root.
- `example/go.mod` has `replace github.com/sergle/gearman-go => ../`, so the
  examples always compile against the working tree. They are the only
  compile-time check that a public API change did not break a caller — run
  `make examples` after touching exported signatures.
- If the library itself ever gains a real dependency, it goes in the root
  go.mod; nothing else should.

**29 of the 36 test functions are skipped by default.** Only 7 run without a
gearmand, and one of those (`TestWorkerRace`) passes vacuously. The `client`
package has no behavioural coverage in a default run — every test of `Do`,
`DoBg`, `Status`, `Echo`, `Close` and `Pool` is gated. Assume `-race` alone
proves nothing about whether job submission still works.

A gearmand is not required to test this code: the wire format is 4-byte magic,
4-byte type, 4-byte big-endian length, body, and an in-process fake server
answering `JOB_CREATED`, `ECHO_RES`, `STATUS_RES` and `WORK_COMPLETE` is enough
to drive the real client through `Do`, `DoBg` and `Echo`.

Two custom test-binary flags gate tests that are not part of the default run.
**Both must come after the package list** (`make integration` / `make
knownbugs` get this right for you):

```sh
go test ./client ./worker -integration  # needs gearmand on 127.0.0.1:4730
go test ./client -knownbugs             # known unfixed defects; expected to FAIL
```

`go test -integration ./client` (flag first) is a silent no-op — go treats the
unrecognised flag and everything after it as arguments for the test binary of
the *current directory*, so it tests the root package, reports "no test files"
and exits 0. It looks like a pass and verifies nothing.

The gates are package-level bools set in `TestMain`: `runIntegrationTests`
(`client/client_test.go`, `worker/worker_test.go`) and `runKnownBugTests`
(`client/client_test.go`). New tests must check the relevant one explicitly, or
they will run in CI with no job server.

`client/knownbugs_test.go` holds tests describing behaviour the client *should*
have; they fail today by design. Nothing there may call a blocking client
method on the test goroutine — each goes through `mustReturnWithin`, so a
hanging defect fails one test instead of wedging the binary until its global
timeout kills every other result too.

`worker/worker_racy_test.go` is the exception: it is not gated, but passes
vacuously without a server because `AddServer` does not dial and `Ready`'s
error is only printed.

Tests in each package share mutable package-level state — `client` in
`client/client_test.go:19`, `pool` in `client/pool_test.go:8` — and depend on
declaration order (`TestClientAddServer` constructs the client every later test
uses). Do not reuse those names for new tests; do not assume tests are
independent.

## Architecture

Two packages that do not share code: `client/` (submit jobs) and `worker/`
(execute jobs). The root `gearman.go` is documentation only, no code.

The Gearman wire constants (`dtCanDo`, `dtWorkComplete`, packet framing,
`minPacketLength`) are **duplicated** in `client/common.go` and
`worker/common.go`. A protocol change means editing both.

### Client

`New` dials, then starts two goroutines that run for the client's lifetime:

- `readLoop` — reads bytes off `rw`, re-frames them into packets (it buffers a
  partial tail in `leftdata` and re-parses, because a TCP read does not align
  to packet boundaries), and pushes `*Response` onto the `in` channel.
- `processLoop` — drains `in` and dispatches by `DataType`.

Responses are correlated by a **fixed key**, not by request identity:
`innerHandler` is keyed `"c"` for the next job-created, `"e"` for echo, and
`"s"+handle` for status. `processLoop` moves the caller's handler into its own
`rhandlers` map keyed by the real job handle once the server assigns one
(`handleInner`, `client.go:205`).

That single `"c"` slot is why `do` holds `client.Mutex` across the **entire**
round trip — write, then block on the result channel until `processLoop`
delivers or `ResponseTimeout` fires. The invariant this creates governs the
whole file:

> `readLoop` and `processLoop` must never take `client.Mutex`. If they do, they
> cannot deliver the response that `do` is holding the lock waiting for.

`client.go:139` already breaks it (`readLoop` calls `Close()`, which locks).
Anything that needs to synchronise connection state therefore needs its own
lock, held only around load/store and never across I/O — reusing
`client.Mutex` deadlocks.

`readLoop` also re-dials internally on error (`client.go:136-147`), which can
resurrect a connection the caller deliberately closed.

`Pool` wraps N clients with a `SelectionHandler` for server selection. It is
not a connection pool to one server.

### Worker

`Worker` owns one `agent` per job server. `AddServer` only constructs the
agent; the dial happens in `Ready()`. Each connected agent runs its own `work()`
goroutine and fans decoded packets into the single `worker.in` channel, which
`Work()` drains in a blocking loop — so `Work()` is the only place job dispatch
happens, and it must run in its own goroutine.

Concurrency is capped by the `limit` buffered channel: `New(OneByOne)` gives
capacity 0, `New(Unlimited)` leaves it nil (uncapped). A token is pushed in
`handleInPack` and popped in `exec`'s defer.

Unlike the client, `agent.read` (`worker/agent.go:174`) frames by reading the
declared length out of the header, so it does not need the client's `leftdata`
dance.

Reconnect is caller-driven: a dropped connection surfaces as
`*WorkerDisconnectError` passed to `ErrorHandler`, and the handler calls
`.Reconnect()` on it. `reconnect` re-registers all functions
(`reRegisterFuncsForAgent`) and starts a fresh `work()` goroutine.

## Conventions

Pre-modules Go, kept deliberately: named return values assigned then bare
`return`, receivers named `client` / `worker` rather than a letter, exported
`sync.Mutex` embedded in `Client` and `Worker` (part of the API — `pool.go`
calls `client.Lock()` externally). Match it in existing files rather than
modernising.

`getBuffer` in both `common.go` files is a plain `make([]byte, l)` with a
`TODO` about pooling; `getRequest`/`getOutPack` likewise. They are hooks for a
pool that was never written, not actual pools.
