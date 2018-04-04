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
