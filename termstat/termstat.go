// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

// Package termstat provides a stats implementation which periodically logs the
// statistics to the given writer. It is meant to be used for testing and
// debugging at the terminal in lieu of an actual collector writing to an
// external tool like graphite or datadog. It provides stub implementations for
// some functionality.
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
func (t *Collector) Count(name string, value int64, rate float64, tags ...string) {
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

// Gauge does nothing.
func (t *Collector) Gauge(name string, value float64, rate float64, tags ...string) {}

// Histogram does nothing.
func (t *Collector) Histogram(name string, value float64, rate float64, tags ...string) {}

// Set does nothing.
func (t *Collector) Set(name string, value string, rate float64, tags ...string) {}

// Timing does nothing.
func (t *Collector) Timing(name string, value time.Duration, rate float64, tags ...string) {}
