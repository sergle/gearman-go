Gearman-Go
==========

This module is a [Gearman](http://gearman.org/) API for the [Go Programming Language](http://golang.org).
The protocols were written in pure Go. It contains two sub-packages:

The client package is used for sending jobs to the Gearman job server,
and getting responses from the server.

	"github.com/sergle/gearman-go/client"

The worker package will help developers in developing Gearman worker
service easily.

	"github.com/sergle/gearman-go/worker"

Fork
====

This is a fork of [mikespook/gearman-go](https://github.com/mikespook/gearman-go),
which is a GOPATH-era repository with no `go.mod`. This one is a proper module,
and it fixes more than two dozen defects upstream still has — concurrency bugs
that crash or wedge a process, packet framing that silently stops a worker, and
a `Pool` that could spin, panic or leak.

### What it fixes

Every item below is fixed here and reproduced by a test in this repository.
None of it is upstream.

**Crashes and hangs.** A worker whose job functions were added or removed while
it ran hit a concurrent map read/write — a runtime throw no `recover` catches.
`Pool` did the same on its `Clients` map, and could spin at 100% CPU or panic
outright when reconfigured. `Close` on a worker panicked by closing a channel
under a live sender. A malformed `STATUS_RES` panicked the client. One dropped
connection could freeze an entire worker: the disconnect handler ran while
holding the lock both of its documented responses need, so the handler blocked
forever, in-flight jobs blocked behind it, and the dispatcher wedged. A
reconnect racing `AddFunc` deadlocked outright — two goroutines, one lock each,
opposite order.

**Silent wrong behaviour**, the worse kind. A worker that read a packet split
across two reads framed every later packet from the wrong offset and simply
stopped picking up jobs, with no error anywhere. A second `Ready()` opened a
duplicate connection and left two goroutines reading one buffer. Job progress
reports (`SendData`, `UpdateStatus`) were written without the lock every other
writer takes, interleaving bytes inside a single packet on the wire. A
timed-out `Echo`'s late reply was delivered to the *next* caller. `Pool.Remove`
dropped a client without closing it, leaking a socket and two goroutines per
call, so a service reloading its server list leaked on every reload.
`Pool.ErrorHandler` was a field nothing ever read: every connection error
inside a pooled client went to the floor.

**Blocking forever.** `Status` and `Echo` had no timeout — against a server
that stopped answering they blocked their caller permanently. They now fail
with `ErrLostConn` after `ResponseTimeout`.

**Data races.** Both packages are clean under `-race`, including against live
job servers. Upstream is not: the worker's connection pointers, the client's
and worker's handler fields, the function map and the ready flag were all read
from one goroutine while another wrote them.

### Allocations

Measured against upstream at the point this fork merged it, with this
repository's benchmarks ported onto that tree. Allocations, not timings: on
this hardware `ns/op` drifts by 30% between runs of identical code, so it is
not a number worth quoting.

- **Submitting a job costs 13 allocations instead of 20**, and 560 bytes
  instead of ~9 KB. `Do` costs 20 instead of 27, `Status` 15 instead of 20.
  Most of the bytes are one line: upstream allocates a fresh 8 KB scratch
  buffer on every read, where this fork keeps one per connection.
- **Running a job through a worker costs 8 allocations instead of 11**, ~265
  bytes instead of ~1.1 KB. The packet decoder scans in place rather than
  splitting, `read` allocates a single buffer sized to the packet instead of a
  1 KB scratch plus a `bytes.Buffer`, and `write` reuses a pooled buffer and
  allocates nothing at all — including for a 32 KB payload.
- **A 64 KB job stalls upstream in about half of benchmark runs**, which is the
  split-read framing defect rather than a slow path. When it does complete it
  costs 20 allocations against this fork's 9.
- Two places this fork is not cheaper. `Echo` allocates the same 9 times — it
  just moves 8 KB less — and a foreground `Do` pays one allocation more,
  because packets are framed one at a time instead of sharing a buffer when
  two arrive in the same read. That is the cost of the framing fix.

**Requires Go 1.23 or later**: before it, `Timer.Reset` did not clear an
already-delivered tick, and the client's shared timer would hand the next
caller an instant, bogus timeout.

**The client API is not compatible with upstream.** `Client.ErrorHandler` was an
exported field that could not be assigned without racing the client's own read
of it; it is gone, replaced by the option and setter shown below. Worker
exceptions also arrive differently — see *Upgrading*.

**Neither is the worker API.** `Worker.ErrorHandler` and `Worker.JobHandler`
had the same problem — plain fields read from every agent's connection
goroutine — and are gone the same way, replaced by `SetErrorHandler` and
`SetJobHandler`, shown below.

**Nor `Pool`'s.** `Pool.ErrorHandler` was a field nothing in `pool.go` ever
read, so setting it did nothing. It is gone too, replaced by
`Pool.SetErrorHandler`, which installs the handler on every client already in
the pool and on every one `Add` brings in later.

Install
=======

> $ go get github.com/sergle/gearman-go

Existing code importing the upstream path can keep its imports by replacing the
module instead:

```
require github.com/mikespook/gearman-go v0.0.0

replace github.com/mikespook/gearman-go => github.com/sergle/gearman-go v0.2.0
```

Usage
=====

## Worker

```go
// Limit how many jobs are dispatched at once (not how many run: a job
// function that misses its AddFunc timeout keeps running after the worker
// gives up on it, so actual concurrency can exceed this).
// Use worker.Unlimited (0) if you want no limitation.
w := worker.New(worker.OneByOne)
w.SetErrorHandler(func(e error) {
	log.Println(e)
})
w.AddServer("tcp4", "127.0.0.1:4730")
// Use worker.Unlimited (0) if you want no timeout
w.AddFunc("ToUpper", ToUpper, worker.Unlimited)
// This will give a timeout of 5 seconds
w.AddFunc("ToUpperTimeOut5", ToUpper, 5)

if err := w.Ready(); err != nil {
	log.Fatal(err)
	return
}
go w.Work()
```

## Client

```go
// ...
c, err := client.New("tcp4", "127.0.0.1:4730", client.WithErrorHandler(func(e error) {
	log.Println(e)
}))
// ... error handling
defer c.Close()
echo := []byte("Hello\x00 world")
echomsg, err := c.Echo(echo)
// ... error handling
log.Println(string(echomsg))
jobHandler := func(resp *client.Response) {
	log.Printf("%s", resp.Data)
}
handle, err := c.Do("ToUpper", echo, client.JobNormal, jobHandler)
// ...	
```

## Worker exceptions

A worker that fails *and* returns data raises a Gearman exception. The job
server only forwards those to a client that asked for them, by sending an
`OPTION_REQ exceptions` on the connection; without it gearmand rewrites the
exception into a plain `WORK_FAIL` and throws the worker's payload away.

The client requests that option by default, on the initial connection and on
every reconnect, so a worker exception arrives intact:

```go
jobHandler := func(resp *client.Response) {
	switch resp.DataType {
	case client.WorkComplete:
		data, _ := resp.Result()
		log.Printf("done: %s", data)
	case client.WorkException:
		// err is client.ErrWorkException; data is the worker's payload.
		data, err := resp.Result()
		log.Printf("exception: %s (%v)", data, err)
	case client.WorkFail:
		log.Printf("failed without a payload")
	}
}
```

`WORK_EXCEPTION` is terminal: the job is finished and no `WORK_FAIL` or
`WORK_COMPLETE` follows it.

### Upgrading

Before this option existed, a worker exception reached the client as a
`WORK_FAIL`. It now arrives as a `WORK_EXCEPTION` instead:

| | before | now |
|---|---|---|
| `resp.DataType` | `client.WorkFail` | `client.WorkException` |
| `resp.Data` | `nil` | the worker's payload |
| `resp.Result()` | `nil, ErrWorkFail` | `payload, ErrWorkException` |

So check your job handlers for two things: a `switch resp.DataType` without a
`case client.WorkException` now drops those responses silently, and retry logic
keyed on `err == client.ErrWorkFail` no longer fires for exceptions.

To keep the old behaviour, opt out before creating any client:

```go
client.DefaultExceptions = false
```

`(*Client).ExceptionsEnabled()` reports whether the job server actually
acknowledged the option. A server that refuses or ignores it is not an error:
the client degrades to the old `WORK_FAIL` behaviour and stays usable.

Note the worker side of this: `gearman-go` sends a `WORK_EXCEPTION` only when
the job function returns a non-empty `data` *and* an error. `return nil, err`
still produces a `WORK_FAIL`, and the text of `err` never goes over the wire —
put anything the client needs to see in `data`.

Versioning
==========

Version 0.x means: _It is far far away from stable._

__Use at your own risk!__

Below v1 a minor bump may break the API, and this one already has three times —
each to remove an exported field that could not be used safely. Pin an exact
version.

The defect list this fork was built from is closed, but `Pool` failover remains
untested and the accepted trade-offs above are real. Report anything you hit.

Contributors
============

Great thanks to all of you for your support and interest!

(_Alphabetic order_)
 
 * [Alex Zylman](https://github.com/azylman)
 * [C.R. Kirkwood-Watts](https://github.com/kirkwood)
 * [Damian Gryski](https://github.com/dgryski)
 * [Gabriel Cristian Alecu](https://github.com/AzuraMeta)
 * [Graham Barr](https://github.com/gbarr)
 * [Ingo Oeser](https://github.com/nightlyone)
 * [jake](https://github.com/jbaikge)
 * [Joe Higton](https://github.com/draxil)
 * [Jonathan Wills](https://github.com/runningwild)
 * [Kevin Darlington](https://github.com/kdar)
 * [miraclesu](https://github.com/miraclesu)
 * [Paul Mach](https://github.com/paulmach)
 * [Randall McPherson](https://github.com/rlmcpherson)
 * [Sam Grimee](https://github.com/sgrimee)

Maintainer
==========

This fork: [sergle/gearman-go](https://github.com/sergle/gearman-go).

Original project:

 * [Xing Xing](http://mikespook.com) &lt;<mikespook@gmail.com>&gt; [@Twitter](http://twitter.com/mikespook)

Open Source - MIT Software License
==================================

See LICENSE.
