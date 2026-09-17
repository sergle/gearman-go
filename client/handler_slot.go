package client

import (
	"sync"
	"time"
)

// handlerSlot is a FIFO of handlers waiting for a reply of one packet type.
// Callers hold client.Mutex for a whole round trip, so the only live entry is
// always the newest; everything ahead of it is delivered or abandoned.
//
// A live entry always has a non-nil internal, which is how abandon marks one
// spent and how set knows it is safe to evict.
type handlerSlot struct {
	mu     sync.Mutex
	queue  []slotEntry
	nextID uint64
}

type slotEntry struct {
	id uint64
	h  handledResponse
	// expires is set by abandon; zero means not abandoned. Past it, take
	// gives up on the reply and drops the entry.
	expires time.Time
}

// handlerSlotCap bounds the queue against a server that never answers. On
// overflow set evicts the oldest entry, but only once it has checked that one
// is spent: a caller that broke the one-live-entry invariant should grow the
// queue past its cap, not lose a live handler silently.
const handlerSlotCap = queueSize

func (s *handlerSlot) set(internal, external ResponseHandler) (id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) >= handlerSlotCap && s.queue[0].h.internal == nil {
		s.queue = s.queue[1:]
	}
	id = s.nextID
	s.nextID++
	s.queue = append(s.queue, slotEntry{id: id, h: handledResponse{internal: internal, external: external}})
	return
}

// take correlates the reply that just arrived with the front entry. Expired
// abandoned entries are dropped first: nothing will answer them now, and every
// later caller would starve behind them.
func (s *handlerSlot) take() (h handledResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for len(s.queue) > 0 {
		e := s.queue[0]
		if e.h.internal != nil || e.expires.IsZero() || now.Before(e.expires) {
			break
		}
		s.queue = s.queue[1:]
	}
	if len(s.queue) == 0 {
		return
	}
	h = s.queue[0].h
	s.queue = s.queue[1:]
	return
}

// cancel removes exactly the entry id names. Used when the write failed: no
// reply is coming, so nothing may be left to absorb someone else's.
func (s *handlerSlot) cancel(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.queue {
		if e.id == id {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// abandon marks the entry id names spent but leaves it queued, so a reply
// arriving within ttl is absorbed by it rather than satisfying the next
// caller. Used on timeout, where the reply may still be in flight.
func (s *handlerSlot) abandon(id uint64, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.queue {
		if e.id == id {
			s.queue[i].h = handledResponse{}
			s.queue[i].expires = time.Now().Add(ttl)
			return
		}
	}
}

// reset drops every queued entry: the connection they were written on is
// gone, so nothing will answer them and they would misdirect the next
// connection's replies.
func (s *handlerSlot) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = nil
}
