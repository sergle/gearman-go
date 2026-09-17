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
make knownbugs                       # tests describing unfixed defects, if any exist
make reproducers                     # the three race reproducers — all green since §1
make bench                           # benchmarks vs the fake servers, no gearmand
go test -run TestClientDo ./client    # single test
```

`make check` is the gate that must pass. `make race`, `make knownbugs` and
`make reproducers` were all written to fail: each runs tests describing
behaviour the code did not have yet, so a red run was the expected state and
making one green by weakening its test defeats the point. **Which cases fail
changes as fixes land, so read the per-test output rather than the exit code.**
Each test carries the defect it describes in its own comment.

As of §1 that has inverted for two of them: `make reproducers` is **green** —
all three race reproducers now pass and stand as regression tests — and
`make race` is green apart from §7's probabilistic flake
(`TestCloseIsIdempotentAndSubsequentCallsFail`, a few subtest assertions per 50
runs). Do not read a green run of either as a broken target. `make knownbugs`
passing means only that no failing test is currently written for a known defect,
not that none are open: `docs/todo.md` is the defect list, and this file does not
track it.

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

**A `$` in `BENCH` is eaten by make.** `make bench BENCH='ClientDo$|ClientEcho'`
expands `$|` as an empty variable and runs `-bench 'ClientDoClientEcho'`, which
matches nothing and exits 0 — the same shape of silent no-op as `go test
-integration ./client` above. Anchor with `$$` (`ClientDo$$|ClientEcho`) or call
`go test -bench` directly.

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

It also **frames what the worker sends**: a packet whose magic is not `\x00REQ`,
or whose declared body length exceeds `maxPacketLength`, is recorded and readable
through `FramingErrors()`, and `serve` drops the connection. That is the
assertion §25's regression test rests on — two unsynchronised writers inside the
agent's one `bufio.Writer` desync the stream, and correct locking cannot. The
check is not optional and every existing test on this harness passes with it.

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

### More than one job server

`make integration` brings up `GEARMAND_COUNT` job servers (default 3) on
`GEARMAND_PORT`, +1, +2, reusing any port already answering and tearing down
only what it started, and hands both test binaries the list in the
**`GEARMAND_POOL_ADDRS`** env var. `client/pool_integration_test.go` pools them
to check server selection against real gearmands; it skips when given fewer than
two, so `GEARMAND_COUNT=1` is the old single-server run. `make gearmand` /
`gearmand-stop` manage the same set.

An env var rather than a third test flag, because a flag must be defined in the
`TestMain` of **every** package it is passed to or the run dies with "flag
provided but not defined". The rest — what the tests assert, why nothing is
asserted about `Rate`, and the traps in the target — is in
`docs/ai/pool_multiserver_integration.md`.

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

Either known-bug file can end up **empty of tests** as fixes land, and both stay
on disk when they do: `make knownbugs` passes `-knownbugs` to `./client` and
`./worker` both, so each package's `TestMain` must keep defining the flag or
that run dies with "flag provided but not defined". `requireKnownBugs` and
`requireWorkerKnownBugs` are the hooks the next defect on either side uses, and
the client's file also holds `mustReturnWithin`, which the regression tests it
graduated still call — so an empty file is not dead code.

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

`New` is variadic (`opts ...Option`), applied before `connect()`. There is no
exported `ErrorHandler` field — `readLoop`/`processLoop` read it from their own
goroutines, which `New` starts, so a plain field could never be assigned
race-free. `WithErrorHandler` installs it as an `Option`, before those
goroutines start; `SetErrorHandler` changes it later, safe to call
concurrently. Both go through an internal `atomic.Pointer[ErrorHandler]`.

`New` then starts two goroutines that run for the client's lifetime:

- `readLoop` — calls `readPacket` for one whole packet at a time and pushes
  `*Response` onto the `in` channel. `readPacket` frames by declared length, as
  `worker/agent.go` does: `io.ReadFull` the 12-byte header, reject a magic that
  is not `\x00RES` or a body over `maxPacketLength`, then `io.ReadFull` exactly
  that many bytes into one slice holding header and body. It returns a whole
  packet or an error, never a fragment, so there is no `leftdata` tail — do not
  reintroduce one.

  A packet per allocation is what keeps `decodeResponse`'s aliasing safe:
  `Response.Data` is a subslice of that packet, so each response owns its bytes.
  Reusing one read buffer would make every `Data` a view into recycled memory.

  A `decodeResponse` failure no longer means "not enough bytes yet" — the packet
  is whole and length-checked, so it is a malformed body and the stream is still
  in sync. `readLoop` reports it and takes the next packet.

  The loop condition itself tests `client.isClosed()`, not `getConn() != nil`:
  `conn == nil` no longer implies `Close()` now that `do` and `Echo`
  can null it out too (see below), and the old condition read a `closeConn`
  from one of them, landing between two packets, as a shutdown — the loop
  exited for good, no error, no redial, nothing left to ever read this
  connection's replacement again.

  In the error path, `readLoop` closes the connection — unless the error is
  `ErrLostConn`, meaning it was already closed — and then breaks or redials by
  the same direct test, `client.isClosed()`. That replaced an inference from
  the error itself: a permanent `*net.OpError` used to be treated as proof
  that `Close()` caused it, which stopped holding once `do` and
  `Echo` started closing the connection on their own `ResponseTimeout` too
  (see below) — a permanent `OpError` no longer implies `Close()`. The same
  test covers the `ErrLostConn` case: `readPacket` found `rw` already nil,
  which now has two causes, not one — `Close()`, or a timeout in
  `do`/`Echo` racing this loop between packets — and only
  `isClosed()` tells them apart; the timeout case redials exactly as any other
  closed connection does. `connect()` refuses to dial once `closed` is set,
  and `setConn` refuses to publish even a dial that was already in flight when
  `Close` landed — checked in both places, since a check only before the dial
  would race it. Both refusals report `ErrLostConn`, closing off both routes
  into the resurrection §7 used to describe.
- `processLoop` — drains `in` and dispatches by `DataType`.

Responses are correlated by **position, not by request identity**, and
`client.handlers` (`responseHandlers`) says so in its shape: `created` and
`echo` are each a `handlerSlot` — a bounded FIFO queue, in
`client/handler_slot.go` — because `JOB_CREATED` and `ECHO_RES` carry no
correlation id; `status` is a map keyed by the job handle, which `STATUS_RES`
does carry. Each member locks itself — no operation
spans two.

A queue rather than one entry is what closed the gap where a timed-out
caller's late reply satisfied whichever caller issued the next request:
`do`, `Status` and `Echo` each hold `client.Mutex` across their whole round
trip (see below), so at most one entry is ever *live* — its caller still
waiting — and it is always the newest one in the queue. `set` returns a token
identifying the entry it just queued. A write failure calls `cancel(token)`,
removing exactly that entry, because no reply is coming and leaving it would
wrongly absorb a later one. A timeout calls `abandon(token)` instead, which
zeroes the entry's handler but leaves it queued, because its reply may still
be in flight and has to be absorbed by *something* — removing it would recreate
the original bug one entry later. `handlerSlotCap` (`queueSize`) bounds the
queue against a server that never answers at all: past the cap, `set` evicts
the oldest entry -- but only when it checks that entry is already spent (nil
handler), rather than trusting the invariant above unconditionally. Every
caller today upholds it, so eviction is the normal case; the check is what
stops a future caller that doesn't from silently losing a live handler instead
of just growing the queue past its cap.

`take` pops the front entry and returns it, or the zero value when the queue is
empty; `deliver` then runs the handler **outside** the slot lock, and moves the
caller's external handler into `processLoop`'s own `rhandlers` map keyed by the
real job handle once the server assigns one. An abandoned entry's handler is
already the zero value, so `deliver` no-ops on it exactly as it did on an empty
slot before. Never call a handler while holding the slot lock.

A queued entry left with no expiry starves every later caller if its own
reply never arrives at all (not just late): each no-op absorbs the *next*
caller's reply instead, that caller abandons its own entry the same way, and
the debt just moves rather than draining. `abandon` stamps `ttl` (the
client's `ResponseTimeout`) as the entry's deadline, and `take` drops a
front entry past its deadline before correlating. `closeConn` calls `reset` on
both queues when a connection drops -- nothing on a closed connection will
ever answer what was queued on it.

`do` and `Echo` now call `closeConn` themselves when their own
`ResponseTimeout` fires, right after `abandon`. **`Status` deliberately does
not**: `STATUS_RES` carries the job handle, so `statusHandlers` matches by key
and `cancel` is enough — closing there would cost an unrelated in-flight `Do`
its `WORK_COMPLETE` and buy nothing. That was the
remaining gap: without it, a caller retrying immediately after a timeout, with
no gap for `ttl` to pass, never recovered -- the debt above moved from caller
to caller forever, because every retry's own reply arrived well inside the
still-live abandoned entry's window. Closing the connection removes the
ambiguity outright rather than bounding it: `reset` empties the queue in the
same call, so nothing is left on the old connection to misdirect, and the
redial starts every caller fresh. `abandon`'s `ttl` still guards the one case
`closeConn` cannot reach here -- a concurrent, unrelated disconnect that beat
this call's own `closeConn` to `reset`-ing the queue, leaving nothing for it to
close -- and is otherwise superseded by the unconditional reset a few lines
later in the same call.

This does not touch `rhandlers`, `processLoop`'s own map of a foreground
`Do`'s external handler keyed by job handle -- `closeConn` only resets
`created` and `echo`. A `Do` that already got its `JOB_CREATED` and returned
has nothing left in either queue for a later timeout to disturb; its callback
is waiting purely on a `WORK_COMPLETE`/`WORK_FAIL`/`WORK_EXCEPTION` that may
now never arrive, because a *different*, later call's timeout dropped the
connection it was going to arrive on. The callback then never fires -- no
error either, since nothing calls it -- and its `rhandlers` entry outlives the
client. Not new: the same loss already happened on `readLoop`'s transport-error
redial; closing on timeout widens how often it can happen rather than
introducing it. It is also why `Status` does not close: polling a slow job
would otherwise take that job's own result delivery down with it.

`do`, `Status` and `Echo` each hold `client.Mutex` across the **entire** round
trip — write, then block on the result channel until `processLoop` delivers or
`ResponseTimeout` fires. The same mutex serialises the one shared
`bufio.Writer`:

> Every `client.write` call site holds `client.Mutex`. `connect()` is the only
> exception, and only because its `rw` is not published by `setConn` yet.

That one-at-a-time property is also what lets all three share a single
`time.Timer` on the `Client` rather than allocating one per call (three
allocations, which is why). `startTimer` / `stopTimer` are only ever called
under `client.Mutex`, and `stopTimer` is deferred *inside* it so the next
caller's `Reset` cannot run first. The timer is created on first use, not in
`New` — the zero `Client` is constructible and tests build one.

**This is why `go.mod` says `go 1.23`.** Before that version `Reset` did not
clear a value already sent on the channel, so a response that raced the fire
left a stale tick and the *next* caller read it as an immediate timeout —
`ErrLostConn` from a healthy server. `TestSharedResponseTimerResetClearsAPendingFire`
is the guard and fails under `GODEBUG=asynctimerchan=1`; `example/go.mod` tracks
the same floor because a module cannot require a dependency whose directive is
higher than its own. Do not lower either.

So a `Do` and an `Echo` on one `Client` cannot overlap at all — deliberately.
The invariant this creates governs the whole file:

> `readLoop` and `processLoop` must never take `client.Mutex`. If they do, they
> cannot deliver the response the caller is holding the lock waiting for.

Connection state therefore has its own lock, `connMu`, reached through
`getConn` / `getRW` / `setConn` and held only around load/store, never across
I/O. Do not reuse `client.Mutex` for it — that deadlocks. `Close` takes
`connMu` only — once directly, to set `closed`, and again via `closeConn`.
`readLoop`, and now `do`/`Echo` on their own `ResponseTimeout`, all
close through `closeConn` rather than `Close` too, for the same reason:
`client.Mutex` there would stall the re-dial for a whole `ResponseTimeout`,
and the caller's response can only arrive over the connection being rebuilt --
`do`/`Echo` already hold `client.Mutex` at that point, so this is the
one `closeConn` call site that is *not* also avoiding a deadlock, just
following the same rule for consistency.

`readLoop` re-dials internally on error, so a connection the caller closed
could come back: it closes, reconnects, loops, and the loop condition passes
because it just reconnected. `Client.closed` (see above) is what stops that,
and `readLoop` now checks it directly — `client.isClosed()`, right after its
own `closeConn` call in the error path — rather than inferring it from the
error's shape. `connect()`/`setConn()` refuse once `closed` is set as a second,
independent check, since `isClosed()` in the loop and the dial that follows it
are not atomic. A residual, bounded window remains and is deliberate, not a
resurrection: `connect()` can pass the `closed` check, dial, and write one
`OPTION_REQ` before `setConn` refuses and closes that connection, so one
accepted connection and one packet can still reach the job server after
`Close` returns. The client never publishes or uses that connection —
`getConn()` stays nil throughout — so no caller can reach it through `Do`,
`Status` or `Echo`.

`Pool` wraps N clients with a `SelectionHandler` for server selection. It is
not a connection pool to one server.

### Worker

`Worker` owns one `agent` per job server. `AddServer` only constructs the
agent; the dial happens in `Ready()`, and `Connect` is idempotent — an agent
that already has a connection returns immediately rather than dialing again.
That matters because `Ready()` is safe to retry: a partial failure on one
agent, or `Work()`'s own auto-`Ready()` after an explicit one that errored,
must not tear down agents that already connected and may have jobs grabbed on
them. Each connected agent runs its own `work()` goroutine and fans decoded
packets into the single `worker.in` channel, which `Work()` drains in a
blocking loop — so `Work()` is the only place job dispatch happens, and it
must run in its own goroutine.

`agent.conn`/`agent.rw` are guarded by `connMu`, a dedicated `sync.RWMutex` —
the worker-side analogue of the client's `connMu` — published together via
`setConn` so no observer ever sees an `rw` that belongs to a different `conn`.
Deliberately not the embedded `agent.Mutex`: `work()` cannot take that one
without stalling every writer it serves (`Grab`, `PreSleep`, `SendData`, ...
all hold it for a whole round trip). `write()` takes a single `rw` snapshot via
`getRW` and does its I/O on the local, exactly like `client.write`; `connMu` is
never held across that I/O. `read()` does **not** call `getRW` — it takes the
`rw` as a parameter, because a snapshot taken per call is one read too late:
a stale `work()` loop calling `read()` again after a newer `setConn` would
snapshot the *new* `rw` and read it alongside the loop that owns it, two
readers on one `bufio.Reader`, which is §10's desync class. `work()` carries
the `rw` it was started with as a local and re-points it only when it redials
itself. Lock
order: a caller already holding `agent.Mutex` may then take `connMu` to
snapshot or publish the pair — `Connect`, `reconnect` and `write` all do
exactly that — and nothing may take `connMu` and then try to take
`agent.Mutex`. `Close` and `disconnect_error` read/clear `conn` through the
same lock, snapshotting and releasing before `disconnect_error` calls
`worker.err` with no lock held. `Close` leaves `rw` alone on purpose: a write
against it after `Close` hits a dead socket and returns an ordinary error,
same as before this lock existed — nilling it would make `write()` dereference
a nil `*bufio.ReadWriter` instead.

`setConn` also bumps a per-agent `epoch`. Every `go a.work(epoch, rw)` call site
captures the epoch its connection was published under and `work()` rechecks it
immediately after every `read()`, before any error routing: a stale loop —
one superseded by a newer `Connect`/`reconnect` while it was still blocked
reading its own, now-abandoned connection — exits instead of falling through
to `disconnect_error` or the redial branch against a connection some other
goroutine now owns. With `rw` passed in rather than re-fetched, that is *all*
the epoch does: cross-connection reads are ruled out structurally, and the
check only stops the superseded loop from acting on someone else's
connection. This is what makes `Connect`'s idempotency guard safe
against every path, not just the retried-`Ready()` one it targets directly.

Concurrency is capped by the `limit` buffered channel: `New(OneByOne)` gives
capacity 0, `New(Unlimited)` leaves it nil (uncapped). A token is pushed in
`handleInPack` and popped in `exec`'s defer.

Unlike the client, `agent.read` frames each packet itself rather than
re-parsing a buffer: `io.ReadFull` for the 12-byte header, then `io.ReadFull`
of exactly the body length the header declares. It returns one whole packet or
an error with `nil` data — never a fragment, never bytes belonging to the next
packet — so `work()` has no `leftdata` tail to carry between iterations and
must not grow one. It also rejects a body over `maxPacketLength` (64 MiB,
`worker/common.go`) and a header whose magic is not `\x00RES`; both are how a
desynced stream fails fast now that the body is pre-allocated from the
declared length. Those go through `work()`'s generic branch — report, dial the
replacement, publish it via `setConn` (re-capturing `epoch` for this same,
continuing loop), close the old connection, `continue` — because a desynced
stream cannot be resynchronised in place. The old connection is closed *after*
the new pair is published, not before: closing first would leave a window
where `getConn()` reports nil, which a concurrent `Connect()` would read as
"not connected" and dial its own second connection.

`io.EOF` and `io.ErrUnexpectedEOF` must **not**: `work()` routes both to
`disconnect_error`, and the distinction matters because the generic branch
redials without re-registering abilities (`funcOutpacks`) or calling `grab()`.
An agent sent there comes back with a live socket, no announced abilities and nothing
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
`*WorkerDisconnectError` passed to the error handler, and the handler calls
`.Reconnect()` on it. `reconnect` dials and publishes the new conn/rw pair via
`setConn` first, *then* builds the registration packets (`funcOutpacks`),
writes them through the locked `Write`/`Grab` wrappers, and starts a fresh
`work()` goroutine with the epoch `setConn` returned. It does not hold
`agent.Mutex` across any of this — each `Write`/`Grab` call takes and releases
it on its own, so `agent.Mutex` and `worker.Mutex` are never nested in either
order.

Publishing before building the snapshot is what closes a registration gap
that used to exist here: with the snapshot built first, an `AddFunc` landing
between it and the publish wrote its `CAN_DO` to the connection being
replaced and was missing from the new one's snapshot. Publishing first means
a concurrent `AddFunc` now writes onto the same connection `reconnect` just
published, so the function reaches it either way — in the snapshot or as its
own write. Two concurrent `reconnect()` calls for one agent are not
serialised against each other any more (nothing calls that from two
goroutines for the same agent today, since a `work()` loop reports at most
one `*WorkerDisconnectError` before it exits); if that ever changes, the
loser's dialed connection leaks — `setConn` overwrites without closing it —
and its `work()` goroutine simply exits on its next epoch check.

`Worker.ready` is folded into the embedded `worker.Mutex`, read through the
unexported `isReady()` rather than the field directly — `Work()`'s own
auto-`Ready()` check goes through it, and so must anything else that reads it
from a different goroutine than the one that called `Ready()`.

## Conventions

Pre-modules Go, kept deliberately: named return values assigned then bare
`return`, receivers named `client` / `worker` rather than a letter, exported
`sync.Mutex` embedded in `Client` and `Worker` (part of the API — `pool.go`
calls `client.Lock()` externally). Match it in existing files rather than
modernising.

`getBuffer` in both `common.go` files is a plain `make([]byte, l)` with a
`TODO` about pooling; the worker's `getOutPack` likewise. They are hooks for a
pool that was never written, not actual pools. The client has no `getRequest`:
`request.go` frames each packet in one buffer through `newPacket`, with no
intermediate struct.

A pool behind the client's `getBuffer` would now be safe on the **encode** side
— `newPacket` fills the header and every encoder writes the whole body,
separators included. It is still unsafe on the **read** side: `decodeResponse`
aliases the read buffer, so `Response.Data` would become a view into recycled
memory.
