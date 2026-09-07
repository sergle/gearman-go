# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

This is a fork of `github.com/mikespook/gearman-go`

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
make bench                           # benchmarks vs the fake servers, no gearmand
go test -run TestClientDo ./client    # single test
```

`make check` is the gate that must pass. `make race`, `make knownbugs` and
`make reproducers` exist to fail: each one runs tests that describe behaviour
the code does not have yet, so a red run is the expected state and making one
green by weakening its test defeats the point. Which cases fail changes as
fixes land, so read the per-test output rather than the exit code. Each test
carries the defect it describes in its own comment.

`make bench` is the same bargain in benchmark form: a case whose defect makes
it wedge fails its watchdog instead of reporting a number. Every case reports a
number today — `BenchmarkClientMixedDoAndEcho` was the one that did not, until
`Status` and `Echo` got the write lock and a timeout — so the watchdogs are now
regression guards. Benchmarks run
against the in-process fake servers, need no gearmand, and are not part of
`make check` — `go test` skips them unless `-bench` is given, so the default
suite pays only the compile. `BENCH`, `BENCHTIME`, `BENCHCOUNT` and
`BENCH_TIMEOUT` override what runs; `BENCHTIME` is a fixed iteration count
rather than a duration so two runs stay comparable when the code gets faster.
Compare medians across `-count` runs: single runs of anything concurrent here
vary by more than the effects being measured.

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

**A large minority of the test functions are skipped by default** — count them
with `go test ./... -v | grep -c SKIP` rather than trusting a number written
here. Everything gated behind `-integration` is the *original* upstream suite:
every one of its tests of `Do`, `DoBg`, `Status`, `Echo`, `Close` and `Pool`
needs a live gearmand. Assume `-race` alone proves nothing about whether job
submission still works.

A gearmand is not required to test this code: the wire format is 4-byte magic,
4-byte type, 4-byte big-endian length, body, and an in-process fake server
answering `OPTION_RES`, `JOB_CREATED`, `ECHO_RES`, `STATUS_RES` and
`WORK_COMPLETE` is enough to drive the real client through `Do`, `DoBg` and
`Echo`.

The worker side has its own, `worker/fakeserver_test.go`: it answers
`GRAB_JOB{,_UNIQ}` with `JOB_ASSIGN_UNIQ`, stays silent on `PRE_SLEEP` until
`Serve()` wakes the worker with a `NOOP`, and counts the `WORK_COMPLETE`s that
come back. The two cannot be merged — the wire constants are duplicated per
package and each side speaks the opposite half of the protocol. Its
`JOB_ASSIGN_UNIQ` body must carry exactly four NUL-separated fields
(`handle\0fn\0uniq\0data`): with three, `decodeInPack` drops the contents,
`fn` comes out empty and the worker silently dispatches nothing.

`OPTION_RES` is not optional. `DefaultExceptions` is true, so `connect()` sends
an `OPTION_REQ` as the **first packet on every connection**, redials included.
A fake server that ignores it leaves `processLoop` treating the first real
response as the option's answer, marking the connection `exceptionsRefused` —
the tests still pass, but they are exercising the degraded handshake instead of
the default one. `client/fakeserver_test.go` answers it and counts it in
`OptionReqs()` rather than in `Requests()`, so the handshake does not shift the
index of the job packets tests assert on.

Two custom test-binary flags gate tests that are not part of the default run.
**Both must come after the package list** (`make integration` / `make
knownbugs` get this right for you):

```sh
go test ./client ./worker -integration  # needs gearmand on 127.0.0.1:4730
go test ./client ./worker -knownbugs    # known unfixed defects; expected to FAIL
```

`go test -integration ./client` (flag first) is a silent no-op — go treats the
unrecognised flag and everything after it as arguments for the test binary of
the *current directory*, so it tests the root package, reports "no test files"
and exits 0. It looks like a pass and verifies nothing.

The gates are package-level bools set in `TestMain`: `runIntegrationTests` and
`runKnownBugTests`, both defined in `client/client_test.go` and
`worker/worker_test.go`. New tests must check the relevant one explicitly, or
they will run in CI with no job server.

A `TestMain` that defines such a flag must call `flag.Parse()` *before*
dereferencing it. Without that the gate reads the zero value, every gated test
in the package skips even when the flag is given, and the run looks green while
verifying nothing.

`client/knownbugs_test.go` and `worker/knownbugs_test.go` hold tests describing
behaviour the packages *should* have; they fail by design. Nothing there may
call a blocking method on the test goroutine — each goes through
`mustReturnWithin` or an equivalent deadline, so a
hanging defect fails one test instead of wedging the binary until its global
timeout kills every other result too.

When a defect is fixed, its test moves into the default suite as a regression
test and the gate comes off. `client/liveness_test.go` is where the
`Status`/`Echo` locking and timeout tests went; they still use
`mustReturnWithin` from `knownbugs_test.go`, same package, because a regression
hangs rather than fails. `worker/framing_test.go` is the worker's equivalent —
the `agent.read` framing tests — with its own `readWithin` for the same reason.

`worker/knownbugs_test.go` is therefore **empty of tests** right now, and stays
on disk deliberately: `make knownbugs` passes `-knownbugs` to `./client` and
`./worker` both, so `worker/worker_test.go` must keep defining the flag or that
run dies with "flag provided but not defined", and `requireWorkerKnownBugs` is
the hook the next worker-side defect uses.

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

`New` calls `connect()` — dial, then (when `DefaultExceptions` is true) write
`OPTION_REQ` onto the fresh `rw` *before* `setConn` publishes it, which is what
guarantees it is the first packet on the wire; do not move that write after
`setConn`. `connect()` is also what `readLoop` re-dials with, so the option is
re-requested on every reconnect — gearmand keeps it per connection.

`New` then starts two goroutines that run for the client's lifetime:

- `readLoop` — reads bytes off `rw`, re-frames them into packets (it buffers a
  partial tail in `leftdata` and re-parses, because a TCP read does not align
  to packet boundaries), and pushes `*Response` onto the `in` channel.
- `processLoop` — drains `in` and dispatches by `DataType`.

Responses are correlated by a **fixed key**, not by request identity:
`innerHandler` is keyed `"c"` for the next job-created, `"e"` for echo, and
`"s"+handle` for status. `processLoop` moves the caller's handler into its own
`rhandlers` map keyed by the real job handle once the server assigns one
(`handleInner`, `client.go:368`).

Those fixed slots are why `do`, `Status` and `Echo` each hold `client.Mutex`
across the **entire** round trip — write, then block on the result channel
until `processLoop` delivers or `ResponseTimeout` fires. The same mutex
serialises the one shared `bufio.Writer`:

> Every `client.write` call site holds `client.Mutex`. `connect()` is the only
> exception, and only because its `rw` is not published by `setConn` yet.

So a `Do` and an `Echo` on one `Client` cannot overlap at all — deliberately.
The invariant this creates governs the whole file:

> `readLoop` and `processLoop` must never take `client.Mutex`. If they do, they
> cannot deliver the response the caller is holding the lock waiting for.

Connection state therefore has its own lock, `connMu`, reached through
`getConn` / `getRW` / `setConn` and held only around load/store, never across
I/O. Do not reuse `client.Mutex` for it — that deadlocks. `Close` takes
`connMu` only, via `closeConn`, and `readLoop` closes through `closeConn`
rather than `Close`, for the same reason: `client.Mutex` there would stall the
re-dial for a whole `ResponseTimeout`, and the caller's response can only
arrive over the connection being rebuilt.

`readLoop` also re-dials internally on error, so a connection the caller closed
can come back: it closes, reconnects, loops — and the loop condition passes
because it just reconnected. There is no `closed` flag. Known and still open;
a clean `-race` run does not mean this is resolved.

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

Unlike the client, `agent.read` (`worker/agent.go:179`) frames each packet
itself rather than re-parsing a buffer: `io.ReadFull` for the 12-byte header,
then `io.ReadFull` of exactly the body length the header declares. It returns
one whole packet or an error with `nil` data — never a fragment, never bytes
belonging to the next packet — so `work()` has no `leftdata` tail to carry
between iterations and must not grow one. It also rejects a body over
`maxPacketLength` (64 MiB, `worker/common.go`) and a header whose magic is not
`\x00RES`; both are how a desynced stream fails fast now that the body is
pre-allocated from the declared length. Those go through `work()`'s generic
branch — report, `Close`, redial, `continue` — because a desynced stream cannot
be resynchronised in place.

`io.EOF` and `io.ErrUnexpectedEOF` must **not**: `work()` routes both to
`disconnect_error`, and the distinction matters because the generic branch
redials without calling `reRegisterFuncsForAgent` or `grab()`. An agent sent
there comes back with a live socket, no announced abilities and nothing
outstanding — silently idle. Only `*WorkerDisconnectError` reaches
`ErrorHandler`, and only the caller's `.Reconnect()` re-registers.
`io.ReadFull` reports a connection that died mid-packet as
`io.ErrUnexpectedEOF` where a bare `Read` reported `io.EOF`, so framing by
declared length has to name it explicitly.
`worker/framing_test.go`'s `TestAgentWorkTruncatedPacketDisconnects` guards
that routing; the rest of the file guards `read` itself.

This was not always true: `read` used to take the length from a single `Read`,
which returned a fragment when that read was shorter than a header and then
framed from four payload bytes for the rest of the connection. The worker
silently stopped grabbing. `worker/framing_test.go` guards it.

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
