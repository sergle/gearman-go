package client

import (
	"errors"
	"math/rand"
	"sync"
)

const (
	poolSize = 10
)

var (
	ErrNotFound = errors.New("Server Not Found")
)

type PoolClient struct {
	*Client
	Rate  int
	mutex sync.Mutex
}

type SelectionHandler func(map[string]*PoolClient, string) string

// SelectWithRate picks a server with a probability proportional to its rate.
// The total is guarded because rand.Intn panics on a non-positive argument:
// Add(net, addr, 0) is reachable through the exported API, and a pool whose
// rates are all zero would otherwise panic here rather than fall through to
// last.
func SelectWithRate(pool map[string]*PoolClient, last string) (addr string) {
	total := 0
	for _, item := range pool {
		total += item.Rate
		if total > 0 && rand.Intn(total) < item.Rate {
			return item.addr
		}
	}
	return last
}

// SelectRandom picks a server uniformly. An empty pool returns last, which
// selectServer turns into ErrNotFound; rand.Intn(0) panics, so the length
// cannot be passed through unchecked.
func SelectRandom(pool map[string]*PoolClient, last string) (addr string) {
	if len(pool) == 0 {
		return last
	}
	r := rand.Intn(len(pool))
	i := 0
	for k, _ := range pool {
		if r == i {
			return k
		}
		i++
	}
	return last
}

type Pool struct {
	SelectionHandler SelectionHandler
	ErrorHandler     ErrorHandler
	Clients          map[string]*PoolClient

	last string

	// mutex guards Clients, last and every PoolClient.Rate. Never held across
	// a call into a Client: that would stall every other caller for a whole
	// round trip.
	mutex sync.RWMutex
}

// NewPool returns a new pool.
func NewPool() (pool *Pool) {
	return &Pool{
		Clients:          make(map[string]*PoolClient, poolSize),
		SelectionHandler: SelectWithRate,
	}
}

// Add a server with rate.
//
// The dial happens outside pool.mutex. Now that the readers take the lock, one
// unreachable job server would otherwise block every submit in the process for
// a full dial timeout -- during a reload or a failover, which is the case the
// locking exists for in the first place.
//
// The cost is that two concurrent Adds for one address both dial, so the
// second acquisition re-checks and closes the loser rather than leaking it.
func (pool *Pool) Add(net, addr string, rate int) (err error) {
	pool.mutex.Lock()
	if item, ok := pool.Clients[addr]; ok {
		item.Rate = rate
		pool.mutex.Unlock()
		return
	}
	pool.mutex.Unlock()

	var client *Client
	if client, err = New(net, addr); err != nil {
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if item, ok := pool.Clients[addr]; ok {
		item.Rate = rate
		client.Close()
		return
	}
	pool.Clients[addr] = &PoolClient{Client: client, Rate: rate}
	return
}

// Remove a server.
func (pool *Pool) Remove(addr string) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	delete(pool.Clients, addr)
}

// Do submits a job. It deliberately does not lock the client: (*Client).do
// takes the client's mutex itself and holds it until the job server answers,
// and sync.Mutex is not reentrant, so locking here as well would hang forever.
// Contrast with Status and Echo below.
func (pool *Pool) Do(funcname string, data []byte, flag byte, h ResponseHandler) (addr, handle string, err error) {
	var client *PoolClient
	if client, err = pool.selectServer(); err != nil {
		return
	}
	handle, err = client.Do(funcname, data, flag, h)
	addr = client.addr
	return
}

// DoBg submits a background job. See the note on Do about locking.
func (pool *Pool) DoBg(funcname string, data []byte, flag byte) (addr, handle string, err error) {
	var client *PoolClient
	if client, err = pool.selectServer(); err != nil {
		return
	}
	handle, err = client.DoBg(funcname, data, flag)
	addr = client.addr
	return
}

// Status gets job status from job server. Does not lock the client, for the
// same reason as Do: (*Client).Status takes that mutex itself and holds it
// until the job server answers.
// !!!Not fully tested.!!!
func (pool *Pool) Status(addr, handle string) (status *Status, err error) {
	pool.mutex.RLock()
	client, ok := pool.Clients[addr]
	pool.mutex.RUnlock()
	if !ok {
		err = ErrNotFound
		return
	}
	status, err = client.Status(handle)
	return
}

// Send a something out, get the samething back. Does not lock the client; see
// the note on Do.
func (pool *Pool) Echo(addr string, data []byte) (echo []byte, err error) {
	var client *PoolClient
	if addr == "" {
		if client, err = pool.selectServer(); err != nil {
			return
		}
	} else {
		pool.mutex.RLock()
		var ok bool
		client, ok = pool.Clients[addr]
		pool.mutex.RUnlock()
		if !ok {
			err = ErrNotFound
			return
		}
	}
	echo, err = client.Echo(data)
	return
}

// Close closes every client in the pool. It snapshots the map first because
// Client.Close does I/O, and the lock must not be held across that.
func (pool *Pool) Close() (err map[string]error) {
	pool.mutex.RLock()
	clients := make([]*PoolClient, 0, len(pool.Clients))
	for _, c := range pool.Clients {
		clients = append(clients, c)
	}
	pool.mutex.RUnlock()

	err = make(map[string]error)
	for _, c := range clients {
		err[c.addr] = c.Close()
	}
	return
}

// selectServer picks the client for a call that did not name a server, and
// returns ErrNotFound when there is nothing to pick.
//
// It used to retry -- `for client == nil` around the selection -- which never
// terminated on an empty pool: SelectWithRate returns last ("") with nothing to
// choose from, the map lookup misses, and the loop goes round again. A busy
// loop, so it burned a full core for the life of the process. Every Pool.Add
// failing during a job server outage is enough to reach it.
//
// A single attempt replaces the retry rather than a bounded one: SelectWithRate
// returns on the first item it iterates whenever any rate is positive, and
// returns last whenever none is, so a second call on an unchanged pool cannot
// produce a different answer. A custom SelectionHandler that returns an unknown
// address now gets ErrNotFound instead of being called again.
//
// The empty check also keeps SelectionHandler from being called with an empty
// map at all. That is the precondition SelectRandom panicked on, and a custom
// handler written the same way would panic the same way.
//
// It takes the write lock, not RLock, because it writes pool.last -- and holds
// it across the SelectionHandler call, because the handler reads
// PoolClient.Rate, which Add writes. Snapshotting the map and calling the
// handler outside the lock would not help: the copy holds the same pointers.
//
// So a SelectionHandler that calls back into the Pool deadlocks. Handlers are
// expected to be pure functions of their arguments.
func (pool *Pool) selectServer() (client *PoolClient, err error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if len(pool.Clients) == 0 {
		err = ErrNotFound
		return
	}
	addr := pool.SelectionHandler(pool.Clients, pool.last)
	var ok bool
	if client, ok = pool.Clients[addr]; !ok {
		client = nil
		err = ErrNotFound
		return
	}
	pool.last = addr
	return
}
