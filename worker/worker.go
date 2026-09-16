// The worker package helps developers to develop Gearman's worker
// in an easy way.
package worker

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Unlimited = iota
	OneByOne
)

// Worker is the only structure needed by worker side developing.
// It can connect to multi-server and grab jobs.
type Worker struct {
	sync.Mutex
	agents  []*agent
	funcs   jobFuncs
	in      chan *inPack
	running bool
	ready   bool
	closed  bool
	// in is never closed: a work() goroutine can be mid-send on it when Close
	// runs. quit carries the shutdown signal instead, with Close its only
	// writer, guarded by closed above.
	quit chan struct{}

	Id string
	// Atomic, not exported fields: every agent's work() goroutine reads these,
	// so assigning one after Ready() raced the goroutines already running.
	errorHandler atomic.Pointer[ErrorHandler]
	jobHandler   atomic.Pointer[JobHandler]
	limit        chan bool
}

// New returns a worker.
//
// If limit is set to Unlimited(=0), the worker will grab all jobs
// and execute them parallelly.
// If limit is greater than zero, the number of paralled executing
// jobs are limited under the number. If limit is assgined to
// OneByOne(=1), there will be only one job executed in a time.
//
// limit bounds dispatch, not execution: a job function that misses AddFunc's
// timeout keeps running, so more than limit can be in flight at once.
func New(limit int) (worker *Worker) {
	worker = &Worker{
		agents: make([]*agent, 0, limit),
		funcs:  make(jobFuncs),
		in:     make(chan *inPack, queueSize),
		quit:   make(chan struct{}),
	}
	if limit != Unlimited {
		worker.limit = make(chan bool, limit-1)
	}
	return
}

// SetErrorHandler installs or replaces the handler for the worker's internal
// errors. Safe while the worker runs; nil disables it.
func (worker *Worker) SetErrorHandler(h ErrorHandler) {
	if h == nil {
		worker.errorHandler.Store(nil)
		return
	}
	worker.errorHandler.Store(&h)
}

// SetJobHandler installs or replaces the handler for results that are not a
// job dispatch -- ECHO_RES, dtError. Safe while the worker runs; nil disables
// it.
func (worker *Worker) SetJobHandler(h JobHandler) {
	if h == nil {
		worker.jobHandler.Store(nil)
		return
	}
	worker.jobHandler.Store(&h)
}

// inner error handling
func (worker *Worker) err(e error) {
	if h := worker.errorHandler.Load(); h != nil {
		(*h)(e)
	}
}

// AddServer adds a Gearman job server.
//
// addr should be formated as 'host:port'.
func (worker *Worker) AddServer(net, addr string) (err error) {
	// Create a new job server's client as a agent of server
	a, err := newAgent(net, addr, worker)
	if err != nil {
		return err
	}
	worker.agents = append(worker.agents, a)
	return
}

// Broadcast an outpack to all Gearman server.
func (worker *Worker) broadcast(outpack *outPack) {
	for _, v := range worker.agents {
		v.Write(outpack)
	}
}

// AddFunc adds a function.
// Set timeout as Unlimited(=0) to disable executing timeout.
//
// Past the timeout the job is reported failed and ErrTimeOut reaches the
// error handler, but f itself keeps running: it is given up on, not stopped.
func (worker *Worker) AddFunc(funcname string, f JobFunc, timeout uint32) (err error) {
	worker.Lock()
	defer worker.Unlock()
	if _, ok := worker.funcs[funcname]; ok {
		return fmt.Errorf("The function already exists: %s", funcname)
	}
	worker.funcs[funcname] = &jobFunc{f: f, timeout: timeout}
	if worker.running {
		worker.addFunc(funcname, timeout)
	}
	return
}

// inner add
func (worker *Worker) addFunc(funcname string, timeout uint32) {
	outpack := prepFuncOutpack(funcname, timeout)
	worker.broadcast(outpack)
}

func prepFuncOutpack(funcname string, timeout uint32) *outPack {
	outpack := getOutPack()
	if timeout == 0 {
		outpack.dataType = dtCanDo
		outpack.data = []byte(funcname)
	} else {
		outpack.dataType = dtCanDoTimeout
		l := len(funcname)

		timeoutString := strconv.FormatUint(uint64(timeout), 10)
		outpack.data = getBuffer(l + len(timeoutString) + 1)
		copy(outpack.data, []byte(funcname))
		outpack.data[l] = '\x00'
		copy(outpack.data[l+1:], []byte(timeoutString))
	}
	return outpack
}

// RemoveFunc removes a function.
func (worker *Worker) RemoveFunc(funcname string) (err error) {
	worker.Lock()
	defer worker.Unlock()
	if _, ok := worker.funcs[funcname]; !ok {
		return fmt.Errorf("The function does not exist: %s", funcname)
	}
	delete(worker.funcs, funcname)
	if worker.running {
		worker.removeFunc(funcname)
	}
	return
}

// inner remove
func (worker *Worker) removeFunc(funcname string) {
	outpack := getOutPack()
	outpack.dataType = dtCantDo
	outpack.data = []byte(funcname)
	worker.broadcast(outpack)
}

// inner package handling
func (worker *Worker) handleInPack(inpack *inPack) {
	switch inpack.dataType {
	case dtNoJob:
		inpack.a.PreSleep()
	case dtNoop:
		inpack.a.Grab()
	case dtJobAssign, dtJobAssignUniq:
		go func() {
			if err := worker.exec(inpack); err != nil {
				worker.err(err)
			}
		}()
		if worker.limit != nil {
			worker.limit <- true
		}
		inpack.a.Grab()
	case dtError:
		worker.err(inpack.Err())
		fallthrough
	case dtEchoRes:
		fallthrough
	default:
		worker.customeHandler(inpack)
	}
}

// Connect to Gearman server and tell every server
// what can this worker do.
func (worker *Worker) Ready() (err error) {
	if len(worker.agents) == 0 {
		return ErrNoneAgents
	}
	// Snapshot funcs under the lock rather than ranging the map directly:
	// AddFunc/RemoveFunc/Reset can run concurrently with Ready on another
	// goroutine. The *jobFunc values themselves are never mutated in place,
	// only added or removed, so a shallow copy is enough.
	worker.Lock()
	funcs := make(jobFuncs, len(worker.funcs))
	for name, f := range worker.funcs {
		funcs[name] = f
	}
	worker.Unlock()
	if len(funcs) == 0 {
		return ErrNoneFuncs
	}
	for _, a := range worker.agents {
		if err = a.Connect(); err != nil {
			return
		}
	}
	for funcname, f := range funcs {
		worker.addFunc(funcname, f.timeout)
	}
	worker.Lock()
	worker.ready = true
	worker.Unlock()
	return
}

// isReady reports whether Ready has completed. Folded into worker.Mutex
// rather than made atomic, matching running and closed, its neighbours in the
// same struct; no path below takes worker.Mutex and then calls into an agent
// -- that lock order is inverted against agent.reconnect and deadlocks -- so
// isReady is safe to call from anywhere.
func (worker *Worker) isReady() bool {
	worker.Lock()
	defer worker.Unlock()
	return worker.ready
}

// Work start main loop (blocking)
// Most of time, this should be evaluated in goroutine.
func (worker *Worker) Work() {
	if !worker.isReady() {
		// didn't run Ready beforehand, so we'll have to do it:
		err := worker.Ready()
		if err != nil {
			panic(err)
		}
	}

	worker.Lock()
	worker.running = true
	worker.Unlock()

	for _, a := range worker.agents {
		a.Grab()
	}
	for {
		select {
		case inpack := <-worker.in:
			worker.handleInPack(inpack)
		case <-worker.quit:
			// Best-effort drain, not a barrier: a sender's own select can
			// still land a value here. Without it each buffered packet is
			// left to this select's random pick between two ready cases.
			for {
				select {
				case inpack := <-worker.in:
					worker.handleInPack(inpack)
				default:
					return
				}
			}
		}
	}
}

// custome handling warper
func (worker *Worker) customeHandler(inpack *inPack) {
	if h := worker.jobHandler.Load(); h != nil {
		if err := (*h)(inpack); err != nil {
			worker.err(err)
		}
	}
}

// Close connection and exit main loop
func (worker *Worker) Close() {
	worker.Lock()
	if worker.closed {
		worker.Unlock()
		return
	}
	worker.closed = true
	worker.running = false
	agents := worker.agents
	worker.Unlock()

	// Before any socket: a sender parked on worker.in is not doing I/O, so
	// a.Close below cannot reach it.
	close(worker.quit)

	// Outside worker.Mutex, since agent.Close does I/O. Unconditional, so a
	// worker that called Ready() but never Work() does not leak its sockets.
	for _, a := range agents {
		a.Close()
	}
}

// Echo
func (worker *Worker) Echo(data []byte) {
	outpack := getOutPack()
	outpack.dataType = dtEchoReq
	outpack.data = data
	worker.broadcast(outpack)
}

// Reset removes all of functions.
// Both from the worker and job servers.
func (worker *Worker) Reset() {
	outpack := getOutPack()
	outpack.dataType = dtResetAbilities
	worker.broadcast(outpack)
	worker.Lock()
	worker.funcs = make(jobFuncs)
	worker.Unlock()
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	outpack := getOutPack()
	outpack.dataType = dtSetClientId
	outpack.data = []byte(id)
	worker.broadcast(outpack)
}

// inner job executing
func (worker *Worker) exec(inpack *inPack) (err error) {
	defer func() {
		if worker.limit != nil {
			<-worker.limit
		}
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = ErrUnknown
			}
		}
	}()
	// Snapshot under the lock and call out after releasing it: worker.funcs is
	// written unlocked by AddFunc/RemoveFunc/Reset from another goroutine while
	// jobs are in flight, and neither the job function nor the I/O below may run
	// while worker.Mutex is held.
	worker.Lock()
	f, ok := worker.funcs[inpack.fn]
	worker.Unlock()
	if !ok {
		return fmt.Errorf("The function does not exist: %s", inpack.fn)
	}
	var r *result
	if f.timeout == 0 {
		d, e := f.f(inpack)
		r = &result{data: d, err: e}
	} else {
		r = execTimeout(f.f, inpack, time.Duration(f.timeout)*time.Second)
	}
	worker.Lock()
	running := worker.running
	worker.Unlock()
	if running {
		outpack := getOutPack()
		if r.err == nil {
			outpack.dataType = dtWorkComplete
		} else {
			if len(r.data) == 0 {
				outpack.dataType = dtWorkFail
			} else {
				outpack.dataType = dtWorkException
			}
			err = r.err
		}
		outpack.handle = inpack.handle
		outpack.data = r.data
		inpack.a.Write(outpack)
	}
	return
}

// funcOutpacks builds the registration packets for every known function and
// returns them rather than writing them: the caller writes under the agent's
// lock, and holding worker.Mutex across a call into agent is the inversion of
// the order AddFunc/RemoveFunc/Close take.
func (worker *Worker) funcOutpacks() (outpacks []*outPack) {
	worker.Lock()
	defer worker.Unlock()
	outpacks = make([]*outPack, 0, len(worker.funcs))
	for funcname, f := range worker.funcs {
		outpacks = append(outpacks, prepFuncOutpack(funcname, f.timeout))
	}
	return
}

// inner result
type result struct {
	data []byte
	err  error
}

// executing timer
func execTimeout(f JobFunc, job Job, timeout time.Duration) (r *result) {
	rslt := make(chan *result)
	defer close(rslt)
	go func() {
		defer func() { recover() }()
		d, e := f(job)
		rslt <- &result{data: d, err: e}
	}()
	select {
	case r = <-rslt:
	case <-time.After(timeout):
		return &result{err: ErrTimeOut}
	}
	return r
}

// Error type passed when a worker connection disconnects
type WorkerDisconnectError struct {
	err   error
	agent *agent
}

func (e *WorkerDisconnectError) Error() string {
	return e.err.Error()
}

// Responds to the error by asking the worker to reconnect
func (e *WorkerDisconnectError) Reconnect() (err error) {
	return e.agent.reconnect()
}

// Which server was this for?
func (e *WorkerDisconnectError) Server() (net string, addr string) {
	return e.agent.net, e.agent.addr
}
