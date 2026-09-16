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

	// connMu guards conn/rw/epoch below, published together by setConn so no
	// observer ever sees an rw that belongs to a different conn.
	//
	// Deliberately not the embedded Mutex: work() cannot take that one
	// without stalling every writer it serves (Grab, PreSleep, SendData, ...
	// all hold it for a whole round trip). The lock is never held across
	// blocking I/O -- read() and write() take a single rw snapshot via getRW
	// and operate outside the lock, exactly as client.read/client.write do.
	//
	// Lock order: a caller already holding a.Mutex may then take connMu, to
	// snapshot or publish conn/rw -- Connect, reconnect and write all do
	// exactly that. Nothing may take connMu and then try to take a.Mutex.
	connMu sync.RWMutex
	conn   net.Conn
	rw     *bufio.ReadWriter
	// epoch counts every publish through setConn. work() captures it at the
	// moment its connection was published and rechecks it after every read;
	// a mismatch means a newer Connect/reconnect has superseded this loop,
	// so it exits instead of reading a connection some other goroutine now
	// owns.
	epoch uint64

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

// getConn returns the current connection, or nil before Connect or after
// Close.
func (a *agent) getConn() net.Conn {
	a.connMu.RLock()
	defer a.connMu.RUnlock()
	return a.conn
}

// getRW returns the current buffered reader/writer. Callers do their I/O on
// the returned value rather than on a.rw, which keeps connMu out of the
// blocking read or write.
func (a *agent) getRW() *bufio.ReadWriter {
	a.connMu.RLock()
	defer a.connMu.RUnlock()
	return a.rw
}

// setConn publishes a new conn/rw pair in one step and bumps the epoch,
// reporting the new value so the caller can hand it to work().
func (a *agent) setConn(conn net.Conn, rw *bufio.ReadWriter) uint64 {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	a.conn = conn
	a.rw = rw
	a.epoch++
	return a.epoch
}

// currentEpoch returns the epoch of the currently published conn/rw pair.
func (a *agent) currentEpoch() uint64 {
	a.connMu.RLock()
	defer a.connMu.RUnlock()
	return a.epoch
}

func (a *agent) Connect() (err error) {
	a.Lock()
	defer a.Unlock()
	// Idempotent: a retried Ready() -- a partial failure on some other agent,
	// or Work()'s own auto-Ready() after an explicit one that errored --
	// must not tear down an agent that already connected. It may have jobs
	// grabbed on it; only an agent with no connection yet needs a dial.
	if a.getConn() != nil {
		return nil
	}
	conn, err := net.Dial(a.net, a.addr)
	if err != nil {
		return
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	epoch := a.setConn(conn, rw)
	go a.work(epoch, rw)
	return
}

// work is the per-connection read loop. epoch and rw are what setConn
// published for the connection this loop was started for. rw is passed
// explicitly rather than re-fetched via getRW() on every call: read() used to
// snapshot a.rw fresh each time it ran, so a stale loop calling read() again
// after a newer Connect/reconnect had already published could snapshot the
// *new* rw and Peek/ReadFull on it concurrently with the new loop's own
// read -- two readers on one bufio.Reader, the epoch check (below) catching it
// only after those bytes were already stolen off the new connection. Holding
// rw as a local means a stale loop can only ever touch the connection it was
// started for; the epoch check now exists solely to make that loop stop
// instead of redialing on its own once its own (dead) connection errors.
func (a *agent) work(epoch uint64, rw *bufio.ReadWriter) {
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
		data, err = a.read(rw)
		// A superseded loop's read(rw) can only ever fail or succeed against
		// its own connection now, never the current one -- so this check no
		// longer guards against cross-connection contamination, only against
		// the loop taking disconnect_error or the redial branch for a
		// connection some other goroutine now owns.
		if a.currentEpoch() != epoch {
			return
		}
		if err != nil {
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
			//
			// The old conn is dialled around and closed only after the new
			// pair is published: closing first (as a bare a.Close() would)
			// leaves a window where getConn() reports nil, which a
			// concurrent Connect() would read as "not connected" and dial
			// its own second connection -- reopening the duplicate-connection
			// defect this guard exists for, one dial-sized window wide.
			oldConn := a.getConn()
			conn, dialErr := net.Dial(a.net, a.addr)
			if dialErr != nil {
				a.worker.err(dialErr)
				if oldConn != nil {
					oldConn.Close()
				}
				break
			}
			// Reassigns the loop's own rw local, not just epoch: this
			// goroutine performed the republish itself, so it is not a stale
			// loop, it is the same loop continuing on its own new connection.
			// Without re-pointing rw here, the next a.read(rw) would keep
			// reading the connection this loop just replaced.
			rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
			epoch = a.setConn(conn, rw)
			if oldConn != nil {
				oldConn.Close()
			}
			// Falling through here would decode the failed read's data against
			// the connection that just replaced it.
			continue
		}
		if inpack, _, err = decodeInPack(data); err != nil {
			a.worker.err(err)
			continue
		}
		inpack.a = a
		// Closing this agent's socket cannot unblock a goroutine parked on a
		// channel send, so shutdown needs its own signal.
		select {
		case a.worker.in <- inpack:
		case <-a.worker.quit:
			return
		}
	}
}

func (a *agent) disconnect_error(err error) {
	// Snapshot, then call worker.err with no lock held: the handler's
	// documented moves, .Reconnect() and w.Close(), both re-enter a.Mutex.
	conn := a.getConn()

	if conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
}

func (a *agent) Close() {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	// a.rw is deliberately left alone: a write against it after Close hits a
	// dead socket and returns an ordinary error, same as before this lock
	// existed. Clearing it would make write() dereference a nil
	// *bufio.ReadWriter instead -- worse, and nothing here needs the signal.
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

// reconnect dials a fresh connection, publishes it, and re-announces this
// worker's functions on it before resuming grabs.
//
// It never holds a.Mutex across the whole sequence, on purpose: publish
// first, through setConn/connMu, then build the registration packets, then
// write each one through the locked Write/Grab wrappers, each taking and
// releasing a.Mutex on its own. a.Mutex and worker.Mutex are therefore never
// nested in either order -- AddFunc/RemoveFunc hold worker.Mutex and reach
// Write, which is the only order left in the package.
//
// Publishing before building the snapshot is what closes the registration
// gap: an AddFunc landing anywhere in this sequence now writes its own CAN_DO
// through the locked Write wrapper onto the same connection reconnect just
// published, so it lands either in funcOutpacks' snapshot or as its own
// write -- never neither. A duplicate CAN_DO is harmless.
func (a *agent) reconnect() error {
	conn, err := net.Dial(a.net, a.addr)
	if err != nil {
		return err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	epoch := a.setConn(conn, rw)

	// Publish before registering: an AddFunc landing in between then
	// announces itself to this connection instead of the dead one.
	outpacks := a.worker.funcOutpacks()

	// Abilities before the grab, as on a fresh connection.
	for _, outpack := range outpacks {
		a.Write(outpack)
	}
	a.Grab()

	go a.work(epoch, rw)
	return nil
}

// read returns exactly one whole packet, or an error and nil data. Never a
// fragment, never bytes belonging to the next packet -- callers rely on that,
// and an earlier version that took the length from a single short Read framed
// every later packet from the wrong offset until the connection died.
//
// rw is the caller's connection, passed in rather than fetched via getRW():
// work() hands it the pair it was started for (or, after redialing itself,
// the pair it just republished) so a stale loop can never read a connection a
// different goroutine now owns -- see work()'s doc comment.
//
// The magic check is the only cheap detector of a desynced stream, since
// decodeInPack ignores data[0:4]. It rejects \x00REQ-magic packets a job server
// has no business sending.
func (a *agent) read(rw *bufio.ReadWriter) (data []byte, err error) {
	// Peek, not ReadFull into a local array: the array escapes through the
	// interface Read, one alloc per packet. A truncated header reports io.EOF
	// here, not io.ErrUnexpectedEOF; work() routes both to disconnect_error.
	hdr, err := rw.Peek(minPacketLength)
	if err != nil {
		return nil, err
	}
	// Peek does not consume, so a rejected header stays buffered. Safe only
	// because work()'s generic branch redials; a bare continue there would
	// re-Peek the same bad header forever.
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
	copy(data, hdr)
	// Cannot short-read: Peek buffered these bytes.
	rw.Discard(minPacketLength)
	if bodyLen > 0 {
		if _, err = io.ReadFull(rw, data[minPacketLength:]); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// encodeBufPool backs write's packet buffer. Pointer-shaped so Put does not
// box a slice header, which would cost the allocation this saves.
var encodeBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, encodeBufSize)
		return &buf
	},
}

const (
	// encodeBufSize covers a WORK_COMPLETE with a small payload.
	encodeBufSize = 256
	// encodeBufKeep bounds what is retained: a packet may legally reach
	// maxPacketLength, and pinning one of those costs more than re-allocating.
	encodeBufKeep = 64 << 10
)

// Internal write the encoded job. Callers hold a.Mutex, which serialises
// write against write -- connMu here is only for the rw snapshot, taken and
// released before any I/O, per the lock order above.
func (a *agent) write(outpack *outPack) (err error) {
	var n int
	p := encodeBufPool.Get().(*[]byte)
	buf := outpack.encodeInto(*p)
	if cap(buf) <= encodeBufKeep {
		*p = buf
	}
	// bufio.Writer does not retain buf past Flush: a large write goes straight
	// to the connection, a small one is copied in.
	defer encodeBufPool.Put(p)
	rw := a.getRW()
	for i := 0; i < len(buf); i += n {
		n, err = rw.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return rw.Flush()
}

// Write with lock
func (a *agent) Write(outpack *outPack) (err error) {
	a.Lock()
	defer a.Unlock()
	return a.write(outpack)
}
