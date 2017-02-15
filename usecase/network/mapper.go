package network

import "sync"

// TODO SetBitmapAttrs to name=whatever for endpoint frames, hostname, useragent, and whatever else makes sense
// was planning on doing that in IDMappers since they know when these things are first setHeader
// Might want to think more about how to persist this info between runs though since it's pretty useless without that. Also if this is being run from multiple places, they'll need to coordinate.

type StringIDs struct {
	lock    sync.RWMutex
	idMap   map[string]uint64
	strings []string
	cur     uint64
}

func NewStringIDs() *StringIDs {
	return &StringIDs{
		idMap:   make(map[string]uint64),
		strings: make([]string, 0, 1000),
	}
}

func (s *StringIDs) GetID(input string) uint64 {
	s.lock.RLock()
	id, ok := s.idMap[input]
	s.lock.RUnlock()
	if ok {
		return id
	}
	s.lock.Lock()
	s.idMap[input] = s.cur
	s.strings = append(s.strings, input)
	s.cur += 1
	s.lock.Unlock()
	return s.cur - 1
}

func (s *StringIDs) Get(id uint64) string {
	// TODO I think we can get away without locking here - confirm
	return s.strings[id]
}

type Nexter struct {
	id   uint64
	lock sync.Mutex
}

func (n *Nexter) Next() (nextID uint64) {
	n.lock.Lock()
	nextID = n.id
	n.id += 1
	n.lock.Unlock()
	return
}

func (n *Nexter) Last() (lastID uint64) {
	n.lock.Lock()
	lastID = n.id - 1
	n.lock.Unlock()
	return
}
