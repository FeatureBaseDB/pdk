package genome

import (
	"math/rand"
	"time"
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
	rng     *rand.Rand
	opts    []string
	optMap  map[string]int
}

// NewDeltaMutator returns a Mutator with specified mutation rate.
// This rate controls the exponential spacing distribution used in mutate().
func NewDeltaMutator(rate float64) *DeltaMutator {
	opts := []string{"A", "C", "G", "T"}
	optMap := make(map[string]int, len(opts))

	for i, opt := range opts {
		optMap[opt] = i
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &DeltaMutator{
		rate:    rate,
		counter: int(rng.ExpFloat64() / rate),
		rng:     rng,
		opts:    opts,
		optMap:  optMap,
	}
}

func (m *DeltaMutator) mutate(b string) string {
	m.counter--
	if m.counter <= 0 {
		// Only mutate when counter hits zero.
		// Reset counter with rounded sample from exponential
		// distribution with the desired rate.
		m.counter = int(m.rng.ExpFloat64() / m.rate)

		// If b isn't in opts, just return b with no mutation.
		optPos, ok := m.optMap[b]
		if !ok {
			return b
		}

		rn := rand.Intn(len(m.opts)-1) + 1
		pos := (rn + optPos) % len(m.opts)
		return m.opts[pos]
	}
	return b
}
