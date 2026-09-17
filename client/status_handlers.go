package client

import "sync"

// statusHandlers holds the handlers waiting for a STATUS_RES, keyed by the job
// handle that packet carries -- unlike handlerSlot, which has no correlation id
// to key on and matches by arrival order instead.
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
