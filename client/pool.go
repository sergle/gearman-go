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

	mutex sync.Mutex
}

// NewPool returns a new pool.
func NewPool() (pool *Pool) {
	return &Pool{
		Clients:          make(map[string]*PoolClient, poolSize),
		SelectionHandler: SelectWithRate,
	}
}

// Add a server with rate.
func (pool *Pool) Add(net, addr string, rate int) (err error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	var item *PoolClient
	var ok bool
	if item, ok = pool.Clients[addr]; ok {
		item.Rate = rate
	} else {
		var client *Client
		client, err = New(net, addr)
		if err == nil {
			item = &PoolClient{Client: client, Rate: rate}
			pool.Clients[addr] = item
		}
	}
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
	if client, ok := pool.Clients[addr]; ok {
		status, err = client.Status(handle)
	} else {
		err = ErrNotFound
	}
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
		var ok bool
		if client, ok = pool.Clients[addr]; !ok {
			err = ErrNotFound
			return
		}
	}
	echo, err = client.Echo(data)
	return
}

// Close
func (pool *Pool) Close() (err map[string]error) {
	err = make(map[string]error)
	for _, c := range pool.Clients {
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
// Deliberately takes no lock: reading pool.Clients and writing pool.last
// unsynchronised is defect 6b, and pool.mutex is the wrong lock for it -- Add
// holds it across a full dial, so taking it here would stall every submit
// behind an unreachable server. It wants the RWMutex pass, not this fix.
func (pool *Pool) selectServer() (client *PoolClient, err error) {
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
