package client

import (
	"testing"
	"time"
)

// Tests for defects that are known and not yet fixed. They describe the
// behaviour the client *should* have, so they fail against the code as it
// stands — that is the point. They are gated behind -knownbugs so the default
// run stays a usable signal:
//
//	go test ./client              # green; these are skipped
//	go test ./client -knownbugs   # the outstanding defects, red
//
// The flag must come *after* the package list. `go test -knownbugs ./client`
// silently tests the current directory instead and exits 0 — go passes the
// unrecognised flag and everything after it to the test binary. The same trap
// applies to this package's existing -integration flag.
//
// When a fix phase lands, its test starts passing and its gate comes off, so
// it joins the default suite as a regression test.
//
// Every one of these calls something that can block forever today, so nothing
// here is allowed to call a client method directly on the test goroutine.
// mustReturnWithin runs it in a goroutine behind a deadline: a hanging defect
// then fails one test instead of wedging the whole package until the binary's
// 10-minute timeout kills it and takes every other result with it.

func requireKnownBugs(t *testing.T) {
	t.Helper()
	if !runKnownBugTests {
		t.Skip("known unfixed defect; run with: go test ./client -knownbugs")
	}
}

// mustReturnWithin calls fn on its own goroutine and fails if it has not
// returned within d. The goroutine is left blocked on purpose — it belongs to a
// defect that has no way to unblock it.
func mustReturnWithin(t *testing.T, d time.Duration, what string, fn func() error) error {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- fn() }()
	select {
	case err := <-errc:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
		return nil
	}
}

// The tests for Status and Echo -- writing without client.Mutex, and blocking
// forever with no timeout -- moved to liveness_test.go when the fix landed.
// They run ungated now, as regression tests.
//
// The Pool.Do / Pool.DoBg deadlock tests that used to live here were deleted
// when the upstream merge (7341bb3) fixed the defect: upstream's own
// pool_deadlock_test.go covers both calls, asserts the returned handle, and
// runs in the default suite rather than behind -knownbugs — which is where a
// fixed defect's regression test belongs.
//
// Pool.selectServer spinning forever on an empty pool (and SelectRandom
// panicking on the same precondition) was fixed on 2026-09-07:
// TestPoolOnEmptyPoolReturnsNotFound moved to pool_selection_test.go, ungated,
// with the rest of the selection regression tests beside it.
//
// This file therefore has no tests left in it, and stays on disk deliberately.
// `make knownbugs` passes -knownbugs to ./client as well as ./worker, so
// client_test.go must keep defining the flag or that run dies with "flag
// provided but not defined"; requireKnownBugs is the hook the next client-side
// defect uses, and mustReturnWithin is still called from liveness_test.go and
// pool_selection_test.go, which live in this package for exactly that reason.
