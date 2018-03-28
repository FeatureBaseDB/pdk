package termstat

import (
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Collector collects stats and prints them to the terminal
type Collector struct {
	lock    sync.Mutex
	indexes map[string]int
	names   []string
	stats   []int64
	changed bool
	out     io.Writer
}

// NewCollector initializes and returns a new TermStat.
func NewCollector(out io.Writer) *Collector {
	ts := &Collector{
		indexes: make(map[string]int),
		out:     out,
	}
	go func() {
		tick := time.NewTicker(time.Second * 2)
		for ; ; <-tick.C {
			ts.write()
		}
	}()
	return ts
}

// Count adds value to the named stat at the specified rate.
func (t *Collector) Count(name string, value int64, rate float64) {
	t.lock.Lock()
	t.changed = true
	defer t.lock.Unlock()

	idx, ok := t.indexes[name]
	if !ok {
		idx = len(t.stats)
		t.stats = append(t.stats, 0)
		t.names = append(t.names, name)
		t.indexes[name] = idx
	}
	if rate < 1 {
		if rand.Float64() > rate {
			return
		}
	}
	t.stats[idx] += value
}

func (t *Collector) write() {
	sb := strings.Builder{}
	t.lock.Lock()
	if !t.changed {
		t.lock.Unlock()
		return
	}
	for i := 0; i < len(t.stats); i++ {
		_, _ = sb.WriteString(fmt.Sprintf("%s: %d ", t.names[i], t.stats[i]))
	}
	t.changed = false
	fmt.Fprintf(t.out, "\r"+sb.String())
	t.lock.Unlock()
}
