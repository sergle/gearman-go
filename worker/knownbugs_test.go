package worker

import (
	"testing"
)

// Tests for defects that are known and not yet fixed, mirroring
// client/knownbugs_test.go. They describe the behaviour the worker *should*
// have, so they fail against the code as it stands -- that is the point.
//
//	go test ./worker              # green; these are skipped
//	go test ./worker -knownbugs   # the outstanding defects, red
//
// The flag must come *after* the package list, exactly as for -integration:
// `go test -knownbugs ./worker` silently tests the current directory and exits
// 0.
//
// No tests here at the moment: the only one, TestAgentReadReturnsWholePackets,
// was fixed and moved to framing_test.go. Do not delete the file for that --
// make knownbugs passes -knownbugs to ./client and ./worker both, so this
// package must keep parsing the flag or the run dies with "flag provided but
// not defined". requireWorkerKnownBugs stays as the hook for the next defect.

func requireWorkerKnownBugs(t *testing.T) {
	t.Helper()
	if !runKnownBugTests {
		t.Skip("known unfixed defect; run with: go test ./worker -knownbugs")
	}
}
