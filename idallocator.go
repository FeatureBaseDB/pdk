package pdk

import (
	"fmt"
	"math/bits"
	"sync"

	"github.com/pkg/errors"
)

type RangeAllocator interface {
	Get() (*IDRange, error)
	Return(*IDRange) error
}

type RangeNexter interface {
	Next() (uint64, error)
	Return() error
}

type LocalRangeAllocator struct {
	shardWidth uint64
	next       uint64
	returned   []*IDRange
	mu         sync.Mutex
}

func NewLocalRangeAllocator(shardWidth uint64) RangeAllocator {
	if shardWidth < 1<<16 || bits.OnesCount64(shardWidth) > 1 {
		panic(fmt.Sprintf("bad shardWidth in NewRangeAllocator: %d", shardWidth))
	}
	return &LocalRangeAllocator{
		shardWidth: shardWidth,
	}
}

// IDRange is inclusive at Start and exclusive at End... like slices.
type IDRange struct {
	Start uint64
	End   uint64
}

type rangeNexter struct {
	a RangeAllocator
	r *IDRange
}

func NewRangeNexter(a RangeAllocator) (RangeNexter, error) {
	r, err := a.Get()
	if err != nil {
		return nil, errors.Wrap(err, "getting range")
	}
	return &rangeNexter{
		a: a,
		r: r,
	}, nil
}

func (n *rangeNexter) Next() (uint64, error) {
	var err error
	if n.r.Start == n.r.End {
		n.r, err = n.a.Get()
		if err != nil {
			return 0, errors.Wrap(err, "getting next range")
		}
	}
	if n.r.Start >= n.r.End {
		panic("Start is greater than End")
	}
	n.r.Start += 1
	return n.r.Start - 1, nil
}

func (n *rangeNexter) Return() error {
	return n.a.Return(n.r)
}

func (a *LocalRangeAllocator) Get() (*IDRange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := len(a.returned)
	if n > 0 {
		ret := a.returned[n-1]
		a.returned = a.returned[:n-1]
		return ret, nil
	}
	ret := &IDRange{
		Start: a.next,
		End:   a.next + a.shardWidth,
	}
	a.next += a.shardWidth
	return ret, nil
}

func (a *LocalRangeAllocator) Return(r *IDRange) error {
	if r.Start == r.End {
		return nil
	}
	if r.Start > r.End {
		return errors.Errorf("attempted to return range with start > end: %v", r)
	}
	a.mu.Lock()
	a.returned = append(a.returned, r)
	a.mu.Unlock()
	return nil
}
