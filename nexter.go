package pdk

import (
	"sync/atomic"
)

type INexter interface {
	Next() uint64
	Last() uint64
}

// Nexter is a threadsafe monotonic unique id generator
type Nexter struct {
	id *uint64
}

// NexterOption can be passed to NewNexter to modify the Nexter's behavior.
type NexterOption func(n *Nexter)

// NexterStartFrom returns an option which makes a Nexter start from integer
// "s".
func NexterStartFrom(s uint64) func(n *Nexter) {
	return func(n *Nexter) {
		*(n.id) = s
	}
}

// NewNexter creates a new id generator starting at 0 - can be modified by
// options.
func NewNexter(opts ...NexterOption) *Nexter {
	var id uint64
	n := &Nexter{
		id: &id,
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Next generates a new id and returns it
func (n *Nexter) Next() (nextID uint64) {
	nextID = atomic.AddUint64(n.id, 1)
	return nextID - 1
}

// Last returns the most recently generated id
func (n *Nexter) Last() (lastID uint64) {
	lastID = atomic.LoadUint64(n.id) - 1
	return
}
