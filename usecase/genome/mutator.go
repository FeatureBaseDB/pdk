package genome

import (
	"errors"
	"math/rand"
)

type Mutator interface {
	mutate(b string) string
}

type NopMutator struct {
}

func NewNopMutator() *NopMutator {
	return &NopMutator{}
}

func (m *NopMutator) mutate(b string) string {
	return b
}

// DeltaMutator mutates nucleotides based on an average rate, minimizing RNG calls
type DeltaMutator struct {
	rate    float64
	counter int
	opts    []string
	optMap  map[string]int
}

func NewDeltaMutator(min, max, denom int) (*DeltaMutator, error) {
	// validate min/max
	if min > max {
		return nil, errors.New("max can't be less than min")
	}
	if min >= denom {
		return nil, errors.New("min must be less than denom")
	}
	if max < 1 || max > denom {
		return nil, errors.New("max must be in the range [1,denom]")
	}

	opts := []string{"A", "C", "G", "T"}
	optMap := make(map[string]int, len(opts))

	for i, opt := range opts {
		optMap[opt] = i
	}

	rate := float64(rand.Intn(max-min)+min) / float64(denom)

	return &DeltaMutator{
		rate:    rate,
		counter: int(rand.ExpFloat64() / rate),
		opts:    opts,
		optMap:  optMap,
	}, nil
}

func (m *DeltaMutator) mutate(b string) string {
	m.counter--
	if m.counter == 0 {
		// Only mutate when counter hits zero.
		// Reset counter with sample from exponential distribution
		// with the desired rate.
		m.counter = int(rand.ExpFloat64() / m.rate)

		rn := rand.Intn(len(m.opts)-1) + 1
		pos := (rn + m.optMap[b]) % len(m.opts)
		return m.opts[pos]
	}
	return b
}
