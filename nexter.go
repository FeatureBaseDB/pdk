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

package pdk

import (
	"sync/atomic"
)

// INexter is the horribly named interface for threadsafe, monotonic,
// sequential, unique ID generation.
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
