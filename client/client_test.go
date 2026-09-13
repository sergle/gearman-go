package client

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	TestStr = "Hello world"
)

var (
	client              *Client
	runIntegrationTests bool
	runKnownBugTests    bool
	runPerlWorkerTests  bool
)

func TestMain(m *testing.M) {
	integrationsTestFlag := flag.Bool("integration", false, "Run the integration tests (in addition to the unit tests)")
	knownBugsFlag := flag.Bool("knownbugs", false, "Run the tests for known unfixed defects (they are expected to fail)")
	perlWorkerFlag := flag.Bool("perlworker", false, "Run tests that need example/pl/worker_multi.pl running against the job server")
	flag.Parse()
	if integrationsTestFlag != nil {
		runIntegrationTests = *integrationsTestFlag
	}
	if knownBugsFlag != nil {
		runKnownBugTests = *knownBugsFlag
	}
	if perlWorkerFlag != nil {
		runPerlWorkerTests = *perlWorkerFlag
	}
	code := m.Run()
	os.Exit(code)
}

func TestClientAddServer(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	t.Log("Add local server 127.0.0.1:4730")
	var err error
	if client, err = New(Network, "127.0.0.1:4730"); err != nil {
		t.Fatal(err)
	}
	client.SetErrorHandler(func(e error) {
		t.Log(e)
	})
}

func TestClientEcho(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	echo, err := client.Echo([]byte(TestStr))
	if err != nil {
		t.Error(err)
		return
	}
	if string(echo) != TestStr {
		t.Errorf("Echo error, %s expected, %s got", TestStr, echo)
		return
	}
}

func TestClientDoBg(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	handle, err := client.DoBg("ToUpper", []byte("abcdef"), JobLow)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDoBgWithId(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	data := []byte("abcdef")
	hash := md5.Sum(data)
	id := hex.EncodeToString(hash[:])
	handle, err := client.DoBgWithId("ToUpper", data, JobLow, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDoBgWithIdFailsIfNoId(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	data := []byte("abcdef")
	id := ""
	_, err := client.DoBgWithId("ToUpper", data, JobLow, id)
	if err == nil {
		t.Error("Expecting error")
		return
	}
	if err.Error() != "Invalid ID" {
		t.Error(fmt.Sprintf("Expecting \"Invalid ID\" error, got %s.", err.Error()))
		return
	}
}

func TestClientDo(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	handle, err := client.Do("ToUpper", []byte("abcdef"),
		JobLow, jobHandler)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDoWithId(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	data := []byte("abcdef")
	hash := md5.Sum(data)
	id := hex.EncodeToString(hash[:])
	handle, err := client.DoWithId("ToUpper", data,
		JobLow, jobHandler, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDoWithIdFailsIfNoId(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	data := []byte("abcdef")
	id := ""
	_, err := client.DoWithId("ToUpper", data,
		JobLow, jobHandler, id)
	if err == nil {
		t.Error("Expecting error")
		return
	}
	if err.Error() != "Invalid ID" {
		t.Error(fmt.Sprintf("Expecting \"Invalid ID\" error, got %s.", err.Error()))
		return
	}
}

func TestClientDoWithIdCheckSameHandle(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		return
	}
	data := []byte("{productId:123,categoryId:1}")
	id := "123"
	handle1, err := client.DoWithId("PublishProduct", data,
		JobLow, jobHandler, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle1 == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle1)
	}

	handle2, err := client.DoWithId("PublishProduct", data,
		JobLow, jobHandler, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle2 == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle2)
	}

	if handle1 != handle2 {
		t.Error("expecting the same handle when using the same id on the same Job name")
	}
}

func TestClientDoWithIdCheckDifferentHandleOnDifferentJobs(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		return
	}
	data := []byte("{productId:123}")
	id := "123"
	handle1, err := client.DoWithId("PublishProduct", data,
		JobLow, jobHandler, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle1 == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle1)
	}

	handle2, err := client.DoWithId("DeleteProduct", data,
		JobLow, jobHandler, id)
	if err != nil {
		t.Error(err)
		return
	}
	if handle2 == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle2)
	}

	if handle1 == handle2 {
		t.Error("expecting different handles because there are different job names")
	}
}

func TestClientMultiDo(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}

	// This test needs a worker registered for "PerlToUpper" — example/pl/
	// worker_multi.pl — in addition to the job server. A bare gearmand can
	// never satisfy it: the 1000 submits below block waiting for results that
	// nobody will produce, and the package hits its global test timeout, which
	// discards every other result too. Hence its own gate.
	//
	//    example/pl/worker_multi.pl &
	//    go test ./client -integration -perlworker -run '^TestClient(AddServer|MultiDo)$'
	if !runPerlWorkerTests {
		t.Skip("needs example/pl/worker_multi.pl running; use: go test ./client -integration -perlworker")
	}

	// Send 1000 requests to go through all race conditions
	const nreqs = 1000
	errCh := make(chan error)
	gotCh := make(chan string, nreqs)

	olderrh := client.errorHandler.Load() // same package: direct atomic access to save/restore
	client.SetErrorHandler(func(e error) { errCh <- e })
	client.ResponseTimeout = 5 * time.Second
	defer client.errorHandler.Store(olderrh)

	nextJobCh := make(chan struct{})
	defer close(nextJobCh)
	go func() {
		for range nextJobCh {
			start := time.Now()
			handle, err := client.Do("PerlToUpper", []byte("abcdef"), JobNormal, func(r *Response) { gotCh <- string(r.Data) })
			if err == ErrLostConn && time.Since(start) > client.ResponseTimeout {
				errCh <- errors.New("Impossible 'lost conn', deadlock bug detected")
			} else if err != nil {
				errCh <- err
			}
			if handle == "" {
				errCh <- errors.New("Handle is empty.")
			}
		}
	}()

	for i := 0; i < nreqs; i++ {
		select {
		case err := <-errCh:
			t.Fatal(err)
		case nextJobCh <- struct{}{}:
		}
	}

	remaining := nreqs
	for remaining > 0 {
		select {
		case err := <-errCh:
			t.Fatal(err)
		case got := <-gotCh:
			if got != "ABCDEF" {
				t.Error("Unexpected response from PerlDoUpper: ", got)
			}
			remaining--
			t.Logf("%d response remaining", remaining)
		}
	}
}

func TestClientStatus(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	status, err := client.Status("handle not exists")
	if err != nil {
		t.Error(err)
		return
	}
	if status.Known {
		t.Errorf("The job (%s) shouldn't be known.", status.Handle)
		return
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
		return
	}

	handle, err := client.Do("Delay5sec", []byte("abcdef"), JobLow, nil)
	if err != nil {
		t.Error(err)
		return
	}
	status, err = client.Status(handle)
	if err != nil {
		t.Error(err)
		return
	}
	if !status.Known {
		t.Errorf("The job (%s) should be known.", status.Handle)
		return
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
		return
	}
}

func TestClientClose(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
