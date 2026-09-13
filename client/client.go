// The client package helps developers connect to Gearmand, send
// jobs and fetch result.
package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	DefaultTimeout time.Duration = time.Second

	// DefaultExceptions controls whether a client asks the job server for the
	// "exceptions" option on every connection it opens, both the initial one
	// and every reconnect.
	//
	// With the option on, a worker that fails with a payload is delivered as a
	// WORK_EXCEPTION carrying that payload. With it off, gearmand rewrites the
	// very same event into a WORK_FAIL and throws the payload away, which is
	// why this defaults to true.
	//
	// Set it to false before calling New() to keep the old behaviour; it is
	// read once, in New(), and applies process-wide.
	DefaultExceptions = true
)

// Negotiation state of the "exceptions" option, held in Client.exceptionsState.
const (
	exceptionsPending int32 = iota // not negotiated yet, or not requested at all
	exceptionsOn                   // the job server acknowledged the option
	exceptionsRefused              // the job server did not take the option
)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	sync.Mutex

	net, addr string
	handlers  responseHandlers
	in        chan *Response

	// connMu guards the conn/rw pointers below, and nothing else.
	//
	// Deliberately not the embedded Mutex: do() holds that one until the
	// ResponseTimeout while waiting for a response, and that response only
	// arrives if readLoop keeps reading in the meantime. Were readLoop to need
	// the embedded Mutex, every Do would time out.
	//
	// The lock is never held across blocking I/O: read() and write() take a
	// single snapshot of rw and operate outside the lock.
	connMu sync.RWMutex
	conn   net.Conn
	rw     *bufio.ReadWriter

	// wantExceptions is a copy of DefaultExceptions taken in New(). It is never
	// written again, so connect() may read it from readLoop's goroutine without
	// a lock. Not guarded by connMu, which covers conn/rw and nothing else.
	wantExceptions bool
	// exceptionsState is one of the exceptions* constants. processLoop writes
	// it, ExceptionsEnabled() reads it from the caller's goroutine, hence
	// atomic. Also not guarded by connMu.
	exceptionsState int32

	// ResponseTimeout bounds the wait for a response in do(), Status() and
	// Echo(). All three return ErrLostConn when it fires.
	ResponseTimeout time.Duration

	// timer backs that wait. One timer serves all three methods because each
	// holds client.Mutex from its write until the reply, so at most one wait is
	// ever in flight -- the same invariant that makes the fixed handler slots
	// work. A fresh time.NewTimer costs three allocations a call.
	//
	// Created on first use rather than in New(): the zero Client is
	// constructible and the tests build one. The lazy init and every
	// Reset/Stop happen under client.Mutex.
	timer *time.Timer

	// Read by err() from readLoop/processLoop, which New starts before it
	// returns: an exported field could never be assigned race-free, hence
	// atomic and hence the setter.
	errorHandler atomic.Pointer[ErrorHandler]
}

// Option configures a Client in New, before its goroutines start.
type Option func(*Client)

// WithErrorHandler installs the error handler before readLoop can report
// anything. SetErrorHandler changes it later.
func WithErrorHandler(h ErrorHandler) Option {
	return func(client *Client) {
		client.SetErrorHandler(h)
	}
}

// SetErrorHandler installs or replaces the error handler. Safe while the client
// is running; nil disables it.
func (client *Client) SetErrorHandler(h ErrorHandler) {
	if h == nil {
		// Normalised here so "absent" has one representation and err() needs
		// only the pointer check.
		client.errorHandler.Store(nil)
		return
	}
	client.errorHandler.Store(&h)
}

// ExceptionsEnabled reports whether the job server acknowledged the
// "exceptions" option on the current connection, and therefore whether a
// worker's exception will arrive as a WORK_EXCEPTION with its payload rather
// than as a bare WORK_FAIL.
//
// It is false while the handshake is still in flight, when DefaultExceptions
// was false when the client was created, and when the server refused or
// ignored the option.
func (client *Client) ExceptionsEnabled() bool {
	return atomic.LoadInt32(&client.exceptionsState) == exceptionsOn
}

// getConn returns the current connection, or nil after Close().
func (client *Client) getConn() net.Conn {
	client.connMu.RLock()
	defer client.connMu.RUnlock()
	return client.conn
}

// getRW returns the current buffered reader/writer. Callers do their I/O on
// the returned value rather than on client.rw, which keeps the lock out of the
// blocking read or write.
func (client *Client) getRW() *bufio.ReadWriter {
	client.connMu.RLock()
	defer client.connMu.RUnlock()
	return client.rw
}

// setConn swaps connection and reader/writer in one step, so no observer can
// ever see an rw that belongs to a different conn.
func (client *Client) setConn(conn net.Conn, rw *bufio.ReadWriter) {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	client.conn = conn
	client.rw = rw
}

type handledResponse struct {
	internal ResponseHandler // nil in a slot that holds nothing
	external ResponseHandler // handler passed in from (*Client).Do, sometimes nil
}

// responseHandlers holds the handlers waiting for a reply. JOB_CREATED and
// ECHO_RES carry no correlation id, so each gets one slot; STATUS_RES carries
// the job handle, so those are keyed by it.
//
// Zero value ready. Each member locks itself: no operation spans two, so there
// is no order to get wrong.
type responseHandlers struct {
	created handlerSlot
	echo    handlerSlot
	status  statusHandlers
}

// handlerSlot holds at most one handler, which is all do and Echo can have
// outstanding: both hold client.Mutex across the whole round trip.
type handlerSlot struct {
	mu sync.Mutex
	h  handledResponse
}

func (s *handlerSlot) set(internal, external ResponseHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h = handledResponse{internal: internal, external: external}
}

// take empties the slot and returns what was in it.
func (s *handlerSlot) take() (h handledResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, s.h = s.h, handledResponse{}
	return
}

func (s *handlerSlot) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h = handledResponse{}
}

type statusHandlers struct {
	mu sync.Mutex
	m  map[string]handledResponse
}

func (s *statusHandlers) set(handle string, internal ResponseHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]handledResponse, queueSize)
	}
	s.m[handle] = handledResponse{internal: internal}
}

func (s *statusHandlers) take(handle string) (h handledResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h = s.m[handle]
	delete(s.m, handle)
	return
}

func (s *statusHandlers) cancel(handle string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, handle)
}

// New returns a client. Options are applied before connect(), so a handler
// installed through one also sees handshake errors.
func New(network, addr string, opts ...Option) (client *Client, err error) {
	client = &Client{
		net:             network,
		addr:            addr,
		in:              make(chan *Response, queueSize),
		wantExceptions:  DefaultExceptions,
		ResponseTimeout: DefaultTimeout,
	}
	for _, opt := range opts {
		opt(client)
	}
	if err = client.connect(); err != nil {
		return nil, err
	}
	go client.readLoop()
	go client.processLoop()
	return
}

// connect dials the job server and, when the client asks for it, negotiates the
// "exceptions" option. It is the single place where a connection is
// established, so the initial dial in New() and the redial in readLoop() cannot
// drift apart -- the job server keeps the option per connection, so it has to
// be requested again on every reconnect or it silently disappears.
//
// The OPTION_REQ is written to rw *before* setConn publishes it. That is what
// makes it the first packet on the wire: no other goroutine can reach this rw
// yet, so no SUBMIT_JOB can overtake it and no concurrent write can interleave
// with its bytes. Do not move this write after setConn.
func (client *Client) connect() (err error) {
	conn, err := net.Dial(client.net, client.addr)
	if err != nil {
		return
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if client.wantExceptions {
		if err = writeTo(rw, encodeRequestString(dtOptionReq, optionExceptions)); err != nil {
			conn.Close()
			return
		}
		atomic.StoreInt32(&client.exceptionsState, exceptionsPending)
	}
	client.setConn(conn, rw)
	if client.wantExceptions {
		// Tell processLoop that the next packet it sees is the answer to the
		// OPTION_REQ above. client.in is buffered and the only other sender is
		// readLoop, which is either not running yet (New) or is the goroutine
		// executing this very call (redial), so this cannot block behind
		// another packet and it lands after everything from the old
		// connection.
		client.in <- &Response{DataType: dtOptionSent}
	}
	return
}

// write sends one encoded packet on the current connection.
//
// Invariant: every caller holds client.Mutex -- do(), Status(), Echo(). They
// share one bufio.Writer, so an unlocked write interleaves its bytes with a
// concurrent request and destroys the framing. connect() is the one exception:
// its rw is not published by setConn yet, so nothing else can reach it.
func (client *Client) write(buf []byte) (err error) {
	rw := client.getRW()
	if rw == nil {
		return ErrLostConn
	}
	return writeTo(rw, buf)
}

// writeTo writes and flushes one packet onto rw. It takes the reader/writer as
// an argument so connect() can use it on a connection that is not published
// yet.
func writeTo(rw *bufio.ReadWriter, buf []byte) (err error) {
	var n int
	for i := 0; i < len(buf); i += n {
		n, err = rw.Write(buf[i:])
		if err != nil {
			return
		}
	}
	return rw.Flush()
}

// readPacket reads one whole packet: the 12-byte header, then exactly the body
// length it declares. It returns a fragment never, so readLoop carries no tail
// between iterations. hdr is the caller's scratch.
//
// The magic check is the only cheap detector of a desynced stream, since
// decodeResponse ignores data[0:4].
func (client *Client) readPacket(hdr []byte) (packet []byte, err error) {
	rw := client.getRW()
	if rw == nil {
		return nil, ErrLostConn
	}
	if _, err = io.ReadFull(rw, hdr); err != nil {
		return nil, err
	}
	if magic := binary.BigEndian.Uint32(hdr[0:4]); magic != res {
		return nil, fmt.Errorf("bad packet magic %q, want %q", hdr[0:4], resStr)
	}
	// While it is still unsigned: on a 32-bit build int(uint32) above 2^31 is
	// negative and make panics with "len out of range".
	bodyLen := binary.BigEndian.Uint32(hdr[8:12])
	if bodyLen > maxPacketLength {
		return nil, fmt.Errorf("packet body of %d bytes exceeds the %d-byte limit",
			bodyLen, maxPacketLength)
	}

	// One slice, header and body: decodeResponse indexes both off it, and
	// Response.Data aliases it. A packet per allocation is what keeps that
	// aliasing safe.
	packet = getBuffer(minPacketLength + int(bodyLen))
	copy(packet, hdr)
	if bodyLen > 0 {
		if _, err = io.ReadFull(rw, packet[minPacketLength:]); err != nil {
			return nil, err
		}
	}
	return packet, nil
}

func (client *Client) readLoop() {
	defer close(client.in)
	var hdr [minPacketLength]byte
	for client.getConn() != nil {
		packet, err := client.readPacket(hdr[:])
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Timeout() {
					client.err(err)
				}
				if !opErr.Temporary() {
					// Permanent, which includes the socket Close() just took:
					// redialing would resurrect a client the caller shut down.
					break
				}
			} else {
				client.err(err)
			}
			// A half-read packet cannot be resynchronised, so rebuild the
			// connection rather than resume mid-stream. connect() re-negotiates
			// the "exceptions" option, which the server holds per connection.
			//
			// closeConn, not Close: callers hold client.Mutex for a whole round
			// trip, so taking it here would stall the re-dial until their
			// ResponseTimeout fired -- and their response can only arrive over
			// the connection this loop is rebuilding.
			client.closeConn()
			if err = client.connect(); err != nil {
				client.err(err)
				break
			}
			continue
		}
		resp, _, err := decodeResponse(packet)
		if err != nil {
			// The packet was whole and its length checked, so this is a
			// malformed body and the stream is still in sync. Report it and
			// take the next packet.
			client.err(err)
			continue
		}
		client.in <- resp
	}
}

func (client *Client) processLoop() {
	rhandlers := map[string]ResponseHandler{}
	// optionPending is true between the marker connect() pushes and the first
	// packet that arrives on the new connection. Only this goroutine touches
	// it, like rhandlers.
	optionPending := false
	for resp := range client.in {
		if resp.DataType == dtOptionSent {
			optionPending = true
			continue
		}
		if optionPending {
			optionPending = false
			if resp.DataType == dtOptionRes {
				atomic.StoreInt32(&client.exceptionsState, exceptionsOn)
				continue
			}
			// The OPTION_REQ was the first packet on this connection (see
			// connect), so the first reply answers it. Anything other than
			// OPTION_RES means the server did not take the option: fall back
			// to WORK_FAIL semantics for this connection.
			atomic.StoreInt32(&client.exceptionsState, exceptionsRefused)
			if resp.DataType == dtError {
				// This ERROR *is* that refusal, not a fault in the caller's
				// job. Swallowing it keeps a server that does not know the
				// option from firing the user's ErrorHandler for something
				// they cannot act on. ERROR packets that are not this reply
				// still reach the handler below.
				continue
			}
			// A server that ignored the OPTION_REQ outright: this packet
			// belongs to a later request, so let it fall through and be
			// handled normally.
		}
		switch resp.DataType {
		case dtError:
			client.err(getError(resp.Data))
		case dtOptionRes:
			// A late or repeated acknowledgement. The state is already set.
		case dtStatusRes:
			client.deliver(client.handlers.status.take(resp.Handle), resp, nil)
		case dtJobCreated:
			client.deliver(client.handlers.created.take(), resp, rhandlers)
		case dtEchoRes:
			client.deliver(client.handlers.echo.take(), resp, nil)
		case dtWorkData, dtWorkWarning, dtWorkStatus:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
			}
		case dtWorkComplete, dtWorkFail, dtWorkException:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
				delete(rhandlers, resp.Handle)
			}
		}
	}
}

// startTimer arms the shared response timer and returns its channel. The caller
// holds client.Mutex, so the reuse is single-threaded by construction.
//
// Reusing the timer requires `go 1.23` or later in go.mod. Before that, Reset
// did not clear a value already sent on the channel, so a response that raced
// the fire left a stale tick for the next caller to receive as an immediate
// timeout. TestSharedResponseTimerDoesNotFireForTheNextCaller is the guard.
func (client *Client) startTimer() <-chan time.Time {
	if client.timer == nil {
		client.timer = time.NewTimer(client.ResponseTimeout)
	} else {
		client.timer.Reset(client.ResponseTimeout)
	}
	return client.timer.C
}

// stopTimer disarms it. Defer this *inside* client.Mutex -- the next caller's
// Reset must not run before it.
func (client *Client) stopTimer() {
	client.timer.Stop()
}

func (client *Client) err(e error) {
	if h := client.errorHandler.Load(); h != nil {
		(*h)(e)
	}
}

// deliver runs a handler taken out of its slot. Called outside the slot lock:
// the handler is the caller's code and must not run under it.
func (client *Client) deliver(h handledResponse, resp *Response, rhandlers map[string]ResponseHandler) {
	if h.internal == nil {
		return
	}
	if h.external != nil && resp.Handle != "" {
		rhandlers[resp.Handle] = h.external
	}
	h.internal(resp)
}

type handleOrError struct {
	handle string
	err    error
}

func (client *Client) do(funcname string, data []byte, flag uint32, h ResponseHandler, id string) (handle string, err error) {
	if len(id) == 0 {
		return "", ErrInvalidId
	}
	if client.getConn() == nil {
		return "", ErrLostConn
	}
	var result = make(chan handleOrError, 1)
	client.Lock()
	defer client.Unlock()
	// Locals, not the named returns: this runs on processLoop's goroutine, and
	// a response arriving after the timeout would be writing them while do has
	// already returned.
	client.handlers.created.set(func(resp *Response) {
		if resp.DataType == dtError {
			result <- handleOrError{"", getError(resp.Data)}
			return
		}
		result <- handleOrError{resp.Handle, nil}
	}, h)
	if err = client.write(encodeJob(flag, funcname, id, data)); err != nil {
		client.handlers.created.cancel()
		return
	}
	// The shared timer, reset rather than allocated: a round trip is
	// microseconds against a default of a second, and a per-call time.After
	// would both allocate and stay live for all of it.
	timeout := client.startTimer()
	defer client.stopTimer()
	select {
	case ret := <-result:
		return ret.handle, ret.err
	case <-timeout:
		client.handlers.created.cancel()
		return "", ErrLostConn
	}
	return
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) Do(funcname string, data []byte, flag byte, h ResponseHandler) (handle string, err error) {
	handle, err = client.DoWithId(funcname, data, flag, h, IdGen.Id())
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBg(funcname string, data []byte, flag byte) (handle string, err error) {
	handle, err = client.DoBgWithId(funcname, data, flag, IdGen.Id())
	return
}

type statusOrError struct {
	status *Status
	err    error
}

// Status gets job status from job server.
//
// Like do(), it holds client.Mutex for the whole round trip: shared writer,
// and a response keyed "s"+handle rather than correlated with this call. Hence
// Pool.Status must not lock the client too -- sync.Mutex is not reentrant.
//
// A response that never arrives gives up after ResponseTimeout with
// ErrLostConn instead of blocking forever.
func (client *Client) Status(handle string) (status *Status, err error) {
	if client.getConn() == nil {
		return nil, ErrLostConn
	}
	// Buffered: a response arriving after the timeout is dropped rather than
	// parking processLoop on a send nobody will receive.
	var result = make(chan statusOrError, 1)
	client.Lock()
	defer client.Unlock()
	client.handlers.status.set(handle, func(resp *Response) {
		st, serr := resp._status()
		result <- statusOrError{st, serr}
	})
	if err = client.write(encodeRequestString(dtGetStatus, handle)); err != nil {
		client.handlers.status.cancel(handle)
		return nil, err
	}
	timeout := client.startTimer() // shared and reset, see do
	defer client.stopTimer()
	select {
	case ret := <-result:
		// A malformed STATUS_RES goes to the caller, not ErrorHandler: they
		// asked, and reporting both ways fires the handler needlessly.
		return ret.status, ret.err
	case <-timeout:
		client.handlers.status.cancel(handle)
		return nil, ErrLostConn
	}
}

// Echo sends something out and gets the same thing back. Locking and timeout
// as in Status; ECHO_RES carries no correlation id, so the "e" slot holds at
// most one echo in flight.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
	if client.getConn() == nil {
		return nil, ErrLostConn
	}
	var result = make(chan []byte, 1) // buffered, see Status
	client.Lock()
	defer client.Unlock()
	client.handlers.echo.set(func(resp *Response) {
		result <- resp.Data
	}, nil)
	if err = client.write(encodeRequest(dtEchoReq, data)); err != nil {
		client.handlers.echo.cancel()
		return nil, err
	}
	timeout := client.startTimer() // shared and reset, see do
	defer client.stopTimer()
	select {
	case ret := <-result:
		return ret, nil
	case <-timeout:
		client.handlers.echo.cancel()
		return nil, ErrLostConn
	}
}

// closeConn drops the current connection, taking connMu and nothing else:
// readLoop calls it, and client.Mutex is held across whole round trips.
//
// conn and rw are all it touches and both are connMu's property. A concurrent
// write took its rw snapshot through getRW() and either writes to a closed
// connection (an error) or finds nil (ErrLostConn). Idempotent.
func (client *Client) closeConn() (err error) {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	if client.conn != nil {
		err = client.conn.Close()
		client.conn = nil
		client.rw = nil
	}
	return
}

// Close connection. Deliberately not on client.Mutex: Status and Echo hold it
// until their response or ResponseTimeout, so locking here would make Close
// wait out an in-flight request instead of cutting it short.
func (client *Client) Close() (err error) {
	return client.closeConn()
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoWithId(funcname string, data []byte, flag byte, h ResponseHandler, id string) (handle string, err error) {
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLow
	case JobHigh:
		datatype = dtSubmitJobHigh
	default:
		datatype = dtSubmitJob
	}
	handle, err = client.do(funcname, data, datatype, h, id)
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBgWithId(funcname string, data []byte, flag byte, id string) (handle string, err error) {
	if client.getConn() == nil {
		return "", ErrLostConn
	}
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLowBg
	case JobHigh:
		datatype = dtSubmitJobHighBg
	default:
		datatype = dtSubmitJobBg
	}
	handle, err = client.do(funcname, data, datatype, nil, id)
	return
}
