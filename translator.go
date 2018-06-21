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
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// Translator describes the functionality for mapping arbitrary values in a
// given Pilosa frame to row ids and back. Implementations should be threadsafe
// and generate ids monotonically.
type Translator interface {
	Get(frame string, id uint64) (interface{}, error)
	GetID(frame string, val interface{}) (uint64, error)
}

// FrameTranslator works like a Translator, but the methods don't take frames as
// arguments. Typically a Translator will include a FrameTranslator for each
// frame.
type FrameTranslator interface {
	Get(id uint64) (interface{}, error)
	GetID(val interface{}) (uint64, error)
}

// MapTranslator is an in-memory implementation of Translator using maps.
type MapTranslator struct {
	lock   sync.RWMutex
	frames map[string]*MapFrameTranslator
}

// NewMapTranslator creates a new MapTranslator.
func NewMapTranslator() *MapTranslator {
	return &MapTranslator{
		frames: make(map[string]*MapFrameTranslator),
	}
}

func (m *MapTranslator) getFrameTranslator(frame string) *MapFrameTranslator {
	m.lock.RLock()
	if mt, ok := m.frames[frame]; ok {
		m.lock.RUnlock()
		return mt
	}
	m.lock.RUnlock()
	m.lock.Lock()
	defer m.lock.Unlock()
	if mt, ok := m.frames[frame]; ok {
		return mt
	}
	m.frames[frame] = NewMapFrameTranslator()
	return m.frames[frame]
}

// Get returns the value mapped to the given id in the given frame.
func (m *MapTranslator) Get(frame string, id uint64) (interface{}, error) {
	val, err := m.getFrameTranslator(frame).Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "frame '%v', id %v", frame, id)
	}
	return val, nil
}

// GetID returns the integer id associated with the given value in the given
// frame. It allocates a new ID if the value is not found.
func (m *MapTranslator) GetID(frame string, val interface{}) (id uint64, err error) {
	return m.getFrameTranslator(frame).GetID(val)
}

// MapFrameTranslator is an in-memory implementation of FrameTranslator using
// sync.Map and a slice.
type MapFrameTranslator struct {
	m sync.Map

	n *Nexter

	l sync.RWMutex
	s []interface{}
}

// NewMapFrameTranslator creates a new MapFrameTranslator.
func NewMapFrameTranslator() *MapFrameTranslator {
	return &MapFrameTranslator{
		n: NewNexter(),
		s: make([]interface{}, 0),
	}
}

// Get returns the value mapped to the given id.
func (m *MapFrameTranslator) Get(id uint64) (interface{}, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	if uint64(len(m.s)) < id {
		return nil, fmt.Errorf("requested unknown id in MapTranslator")
	}
	return m.s[id], nil
}

// GetID returns the integer id associated with the given value. It allocates a
// new ID if the value is not found.
func (m *MapFrameTranslator) GetID(val interface{}) (id uint64, err error) {
	// TODO - this is a janky way to support byte slice value - revisit would be
	// nice to support values of any type, but currently only things that are
	// acceptable map keys are supported.(and byte slices because of this hack)
	valMap, valSlice := fmt.Sprintf("%s", val), val
	if idv, ok := m.m.Load(valMap); ok {
		if id, ok = idv.(uint64); !ok {
			return 0, errors.Errorf("Got non uint64 value back from MapTranslator: %v", idv)
		}
		return id, nil
	}
	m.l.Lock()
	if idv, ok := m.m.Load(valMap); ok {
		m.l.Unlock()
		if id, ok = idv.(uint64); !ok {
			return 0, errors.Errorf("Got non uint64 value back from MapTranslator: %v", idv)
		}
		return id, nil
	}
	nextid := m.n.Next()
	m.s = append(m.s, valSlice)
	if uint64(len(m.s)) != nextid+1 {
		panic(fmt.Sprintf("unexpected length of slice, nextid: %d, len: %d", nextid, len(m.s)))
	}
	m.m.Store(valMap, nextid)
	m.l.Unlock()
	return nextid, nil
}

// NexterFrameTranslator satisfies the FrameTranslator interface, but simply
// allocates a new contiguous id every time GetID(val) is called. It does not
// store any mapping and Get(id) always returns an error. Pilosa requires column
// ids regardless of whether we actually require tracking what each individual
// column represents, and the NexterFrameTranslator is useful in the case that
// we don't.
type NexterFrameTranslator struct {
	n *Nexter
}

// NewNexterFrameTranslator creates a new NexterFrameTranslator
func NewNexterFrameTranslator() *NexterFrameTranslator {
	return &NexterFrameTranslator{
		n: NewNexter(),
	}
}

// GetID for the NexterFrameTranslator increments the internal id counter
// atomically and returns the next id - it ignores the val argument entirely.
func (n *NexterFrameTranslator) GetID(val interface{}) (id uint64, err error) {
	return n.n.Next(), nil
}

// Get always returns nil, and a non-nil error for the NexterFrameTranslator.
func (n *NexterFrameTranslator) Get(id uint64) (interface{}, error) {
	return nil, errors.New("the NexterFrameTranslator \"Get\" method should not be used - cannot map ids back to values")
}
