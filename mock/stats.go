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

package mock

import (
	"time"
)

// RecordingStatter is used for testing. Not threadsafe.
type RecordingStatter struct {
	Counts map[string]int64
}

// Count implements Count.
func (r *RecordingStatter) Count(name string, value int64, rate float64, tags ...string) {
	if r.Counts == nil {
		r.Counts = make(map[string]int64)
	}
	r.Counts[name] += value
}

// Gauge implements Gauge.
func (r RecordingStatter) Gauge(name string, value float64, rate float64, tags ...string) {}

// Histogram implements Histogram.
func (r RecordingStatter) Histogram(name string, value float64, rate float64, tags ...string) {}

// Set implements Set.
func (r RecordingStatter) Set(name string, value string, rate float64, tags ...string) {}

// Timing implements Timing.
func (r RecordingStatter) Timing(name string, value time.Duration, rate float64, tags ...string) {}
