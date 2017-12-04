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
	Get(frame string, id uint64) interface{}
	GetID(frame string, val interface{}) (uint64, error)
}

type FrameTranslator interface {
	Get(id uint64) (interface{}, error)
	GetID(val interface{}) (uint64, error)
}

type MapTranslator struct {
	lock   sync.RWMutex
	frames map[string]*MapFrameTranslator
}

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

func (m *MapTranslator) Get(frame string, id uint64) interface{} {
	val, err := m.getFrameTranslator(frame).Get(id)
	if err != nil {
		panic(err) // TODO change to returning an error after fixing Translator interface
	}
	return val
}

func (m *MapTranslator) GetID(frame string, val interface{}) (id uint64, err error) {
	return m.getFrameTranslator(frame).GetID(val)
}

type MapFrameTranslator struct {
	m sync.Map

	n *Nexter

	l sync.RWMutex
	s []interface{}
}

func NewMapFrameTranslator() *MapFrameTranslator {
	return &MapFrameTranslator{
		n: NewNexter(),
		s: make([]interface{}, 0),
	}
}

func (m *MapFrameTranslator) Get(id uint64) (interface{}, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	if uint64(len(m.s)) < id {
		return nil, fmt.Errorf("requested unknown id in MapTranslator")
	}
	return m.s[id], nil
}

func (m *MapFrameTranslator) GetID(val interface{}) (id uint64, err error) {
	// TODO - this is a janky way to support byte slice value - revisit would be
	// nice to support values of any type, but currently only things that are
	// acceptable map keys are supported.(and byte slices because of this hack)
	var valMap interface{}
	var valSlice interface{}
	if val_b, ok := val.([]byte); ok {
		valMap = string(val_b)
		valSlice = val_b
	} else {
		valMap, valSlice = val, val
	}
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
