package worker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// The agent of job server.
type agent struct {
	sync.Mutex
	conn      net.Conn
	rw        *bufio.ReadWriter
	worker    *Worker
	in        chan []byte
	net, addr string
}

// Create the agent of job server.
func newAgent(net, addr string, worker *Worker) (a *agent, err error) {
	a = &agent{
		net:    net,
		addr:   addr,
		worker: worker,
		in:     make(chan []byte, queueSize),
	}
	return
}

func (a *agent) Connect() (err error) {
	a.Lock()
	defer a.Unlock()
	a.conn, err = net.Dial(a.net, a.addr)
	if err != nil {
		return
	}
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))
	go a.work()
	return
}

func (a *agent) work() {
	defer func() {
		if err := recover(); err != nil {
			a.worker.err(err.(error))
		}
	}()

	var inpack *inPack
	var err error
	var data []byte
	// No re-framing here on purpose: read already delivers whole packets. The
	// leftdata buffer this loop used to carry between iterations is what let a
	// single mis-framed read desync the connection permanently.
	for {
		if data, err = a.read(); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Temporary() {
					continue
				} else {
					a.disconnect_error(err)
					// else - we're probably dc'ing due to a Close()

					break
				}

			} else if err == io.EOF || err == io.ErrUnexpectedEOF {
				// ErrUnexpectedEOF belongs here rather than in the redial
				// branch below: that branch never re-registers this agent's
				// functions, so a dropped connection routed there comes back
				// with no abilities announced and no grab outstanding, idle
				// and silent. Only .Reconnect() re-registers, and only
				// *WorkerDisconnectError reaches the handler that calls it.
				a.disconnect_error(err)
				break
			}
			a.worker.err(err)
			// If it is unexpected error and the connection wasn't
			// closed by Gearmand, the agent should close the conection
			// and reconnect to job server.
			a.Close()
			a.conn, err = net.Dial(a.net, a.addr)
			if err != nil {
				a.worker.err(err)
				break
			}
			a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
				bufio.NewWriter(a.conn))
			// Falling through here would decode the failed read's data against
			// the connection that just replaced it.
			continue
		}
		if inpack, _, err = decodeInPack(data); err != nil {
			a.worker.err(err)
			continue
		}
		inpack.a = a
		a.worker.in <- inpack
	}
}

func (a *agent) disconnect_error(err error) {
	a.Lock()
	defer a.Unlock()

	if a.conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
}

func (a *agent) Close() {
	a.Lock()
	defer a.Unlock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
}

func (a *agent) Grab() {
	a.Lock()
	defer a.Unlock()
	a.grab()
}

func (a *agent) grab() {
	outpack := getOutPack()
	outpack.dataType = dtGrabJobUniq
	a.write(outpack)
}

func (a *agent) PreSleep() {
	a.Lock()
	defer a.Unlock()
	outpack := getOutPack()
	outpack.dataType = dtPreSleep
	a.write(outpack)
}

func (a *agent) reconnect() error {
	a.Lock()
	defer a.Unlock()
	conn, err := net.Dial(a.net, a.addr)
	if err != nil {
		return err
	}
	a.conn = conn
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))

	a.worker.reRegisterFuncsForAgent(a)
	a.grab()

	go a.work()
	return nil
}

// read returns exactly one whole packet, or an error and nil data. Never a
// fragment, never bytes belonging to the next packet -- callers rely on that,
// and an earlier version that took the length from a single short Read framed
// every later packet from the wrong offset until the connection died.
//
// The magic check is the only cheap detector of a desynced stream, since
// decodeInPack ignores data[0:4]. It rejects \x00REQ-magic packets a job server
// has no business sending.
func (a *agent) read() (data []byte, err error) {
	var hdr [minPacketLength]byte
	if _, err = io.ReadFull(a.rw, hdr[:]); err != nil {
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

	// One slice, not two: decodeInPack indexes the header and the body off the
	// same backing array.
	data = getBuffer(minPacketLength + int(bodyLen))
	copy(data, hdr[:])
	if bodyLen > 0 {
		if _, err = io.ReadFull(a.rw, data[minPacketLength:]); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// Internal write the encoded job.
func (a *agent) write(outpack *outPack) (err error) {
	var n int
	buf := outpack.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.rw.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return a.rw.Flush()
}

// Write with lock
func (a *agent) Write(outpack *outPack) (err error) {
	a.Lock()
	defer a.Unlock()
	return a.write(outpack)
}
