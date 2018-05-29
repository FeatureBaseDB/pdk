package fake

import (
	"io"
	"math"
	"sync"
	"sync/atomic"
)

// Source is a pdk.Source which generates fake Event data.
type Source struct {
	max    uint64
	events chan *Event
	wg     sync.WaitGroup
	closed chan struct{}
	n      *uint64
}

// NewSource creates a new Source with the given random seed. Using the same
// seed should give the same series of events on a given version of Go.
func NewSource(seed int64, concurrency int, max uint64) *Source {
	if max == 0 {
		max = math.MaxUint64
	}
	var n uint64
	s := &Source{
		events: make(chan *Event, 1000),
		closed: make(chan struct{}),
		n:      &n,
		max:    max,
	}
	for i := 0; i < concurrency; i++ {
		s.wg.Add(1)
		go func(i int) {
			defer s.wg.Done()
			// seed is multiplied by 10 because NewEventGenerator uses several
			// seeds internally (incrementing it by 1) and we'd prefer to avoid
			// re-using seeds between different generators.
			g := NewEventGenerator(seed * 10 * int64(i))
			for {
				select {
				case s.events <- g.Event():
				case <-s.closed:
					return
				}
			}
		}(i)
	}
	return s
}

// Record implements pdk.Source and returns a randomly generated fake.Event.
func (s *Source) Record() (interface{}, error) {
	next := atomic.AddUint64(s.n, 1)
	if next > s.max {
		close(s.closed)
		s.wg.Wait()
		return nil, io.EOF
	}
	return <-s.events, nil
}
