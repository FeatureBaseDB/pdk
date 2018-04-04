package pdk

import (
	"log"
	"time"
)

// Statter is the interface that stats collectors must implement to get stats out of the PDK.
type Statter interface {
	Count(name string, value int64, rate float64, tags ...string)
	Gauge(name string, value float64, rate float64, tags ...string)
	Histogram(name string, value float64, rate float64, tags ...string)
	Set(name string, value string, rate float64, tags ...string)
	Timing(name string, value time.Duration, rate float64, tags ...string)
}

// NopStatter does nothing.
type NopStatter struct{}

// Count does nothing.
func (NopStatter) Count(name string, value int64, rate float64, tags ...string) {}

// Gauge does nothing.
func (NopStatter) Gauge(name string, value float64, rate float64, tags ...string) {}

// Histogram does nothing.
func (NopStatter) Histogram(name string, value float64, rate float64, tags ...string) {}

// Set does nothing.
func (NopStatter) Set(name string, value string, rate float64, tags ...string) {}

// Timing does nothing.
func (NopStatter) Timing(name string, value time.Duration, rate float64, tags ...string) {}

// Logger is the interface that loggers must implement to get PDK logs.
type Logger interface {
	Printf(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// NopLogger logs nothing.
type NopLogger struct{}

// Printf does nothing.
func (NopLogger) Printf(format string, v ...interface{}) {}

// Debugf does nothing.
func (NopLogger) Debugf(format string, v ...interface{}) {}

// StdLogger only prints on Printf.
type StdLogger struct {
	*log.Logger
}

// Printf implements Logger interface.
func (s StdLogger) Printf(format string, v ...interface{}) {
	s.Logger.Printf(format, v)
}

// Debugf implements Logger interface, but prints nothing.
func (StdLogger) Debugf(format string, v ...interface{}) {}

// VerboseLogger prints on both Printf and Debugf.
type VerboseLogger struct {
	*log.Logger
}

// Printf implements Logger interface.
func (s VerboseLogger) Printf(format string, v ...interface{}) {
	s.Logger.Printf(format, v)
}

// Debugf implements Logger interface.
func (s VerboseLogger) Debugf(format string, v ...interface{}) {
	s.Logger.Printf(format, v)
}
