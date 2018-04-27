package genome

import (
	"errors"
	"math/rand"
)

func NewMutator(min, max, denom int) (*Mutator, error) {
	// validate min/max
	if min > max {
		return nil, errors.New("max can't be less than min")
	}
	if min >= denom {
		return nil, errors.New("min must be less than denom")
	}
	if max == 0 || max > denom {
		return nil, errors.New("max must be in the range [1,denom]")
	}

	opts := []string{"A", "C", "G", "T"}
	optMap := make(map[string]int, len(opts))

	for i, opt := range opts {
		optMap[opt] = i
	}

	matchMin := min % denom
	matchMax := ((max - 1) % denom) + 1

	var match int
	if min == max {
		match = matchMin
	} else {
		match = rand.Intn(matchMax-matchMin) + matchMin
	}

	return &Mutator{
		min:    min,
		max:    max,
		denom:  denom,
		opts:   opts,
		optMap: optMap,
		match:  match,
	}, nil
}

type Mutator struct {
	min   int
	max   int
	denom int

	opts   []string
	optMap map[string]int

	match int
}

// mutate will create a variant based on a probability range.
func (m *Mutator) mutate(b string) string {

	if m.match == 0 {
		return b
	}

	try := rand.Intn(m.denom)
	if try < m.match {
		rn := rand.Intn(len(m.opts)-1) + 1
		pos := (rn + m.optMap[b]) % len(m.opts)
		return m.opts[pos]
	}

	return b
}
