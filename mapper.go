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
	"time"

	"github.com/pkg/errors"
)

// CollapsingMapper processes Entities into PilosaRecords by walking the tree of
// properties and collapsing every path down to a concrete value into a single
// property name.
type CollapsingMapper struct {
	Translator    Translator
	ColTranslator FrameTranslator
	Framer        Framer
	Nexter        INexter
}

// NewCollapsingMapper returns a CollapsingMapper with basic implementations of
// its components. In order to track mapping of Pilosa columns to records, you
// must replace the ColTranslator with something other than a
// NexterFrameTranslator which just allocates ids and does not store a mapping.
func NewCollapsingMapper() *CollapsingMapper {
	return &CollapsingMapper{
		Translator:    NewMapTranslator(),
		ColTranslator: NewNexterFrameTranslator(),
		Framer:        &DashFrame{},
		Nexter:        NewNexter(),
	}
}

// Map implements the RecordMapper interface.
func (m *CollapsingMapper) Map(e *Entity) (PilosaRecord, error) {
	pr := PilosaRecord{}
	var col uint64
	var err error
	if m.ColTranslator != nil {
		col, err = m.ColTranslator.GetID(string(e.Subject))
		if err != nil {
			return pr, errors.Wrap(err, "getting column id from subject")
		}
	} else {
		col = m.Nexter.Next()
	}
	pr.Col = col
	return pr, m.mapObj(e, &pr, []string{})
}

func (m *CollapsingMapper) mapObj(val Object, pr *PilosaRecord, path []string) error {
	if objs, ok := val.(Objects); ok {
		// treat lists as sets
		//
		// should add an option to add index as a path component when order
		// matters. Actually, mapper should have a context which has this
		// information on a per-list basis.
		for _, obj := range objs {
			err := m.mapObj(obj, pr, path)
			if err != nil {
				return errors.Wrap(err, "mapping obj from list")
			}
		}
		return nil
	}
	if ent, ok := val.(*Entity); ok {
		for prop, obj := range ent.Objects {
			err := m.mapObj(obj, pr, append(path, string(prop)))
			if err != nil {
				return errors.Wrapf(err, "mapping entity")
			}
		}
		return nil
	}
	if lit, ok := val.(Literal); ok {
		err := m.mapLit(lit, pr, path)
		return errors.Wrapf(err, "mapping literal '%v'", lit)
	}
	panic(fmt.Sprintf("in mapper: %#v of type %T should be an \"Objects\", a *Entity, or a Literal.", val, val))
}

func (m *CollapsingMapper) mapLit(val Literal, pr *PilosaRecord, path []string) error {
	switch tval := val.(type) {
	case F32, F64, I, I8, I16, I32, I64, U, U8, U16, U32, U64:
		frame, field, err := m.Framer.Field(path)
		if err != nil {
			return errors.Wrapf(err, "getting frame/field from %v", path)
		}
		if field == "" {
			return nil
		}
		if frame == "" {
			frame = "default"
		}
		pr.AddVal(frame, field, Int64ize(tval))
	case S:
		frame, err := m.Framer.Frame(path)
		if err != nil {
			return errors.Wrapf(err, "gettting frame from %v", path)
		}
		if frame == "" {
			return nil
		}
		id, err := m.Translator.GetID(frame, tval)
		if err != nil {
			return errors.Wrapf(err, "getting id from %v", val)
		}
		pr.AddRow(frame, id)
	case B:
		// for bools, use field as the row name - only set if val is true
		if !tval {
			return nil
		}
		frame, _, err := m.Framer.Field(path)
		field := path[0] // in the case of booleans, the path becomes the row id
		// and therefore should not be downcased/trimmed.
		if err != nil {
			return errors.Wrapf(err, "getting frame/field from %v", path)
		}
		if field == "" {
			return nil
		}
		if frame == "" {
			frame = "default"
		}
		id, err := m.Translator.GetID(frame, field)
		if err != nil {
			return errors.Wrapf(err, "getting bool id from %v", field)
		}
		pr.AddRow(frame, id)
	}
	return nil
}

func Int64ize(val Literal) int64 {
	switch tval := val.(type) {
	case F32:
		return int64(tval)
	case F64:
		return int64(tval)
	case I:
		return int64(tval)
	case I8:
		return int64(tval)
	case I16:
		return int64(tval)
	case I32:
		return int64(tval)
	case I64:
		return int64(tval)
	case U:
		return int64(tval)
	case U8:
		return int64(tval)
	case U16:
		return int64(tval)
	case U32:
		return int64(tval)
	case U64:
		return int64(tval)
	default:
		panic("don't call Int64ize on non-numeric Literals")
	}

}

// PilosaRecord represents a number of set bits and values in a single Column
// in Pilosa.
type PilosaRecord struct {
	Col  uint64
	Rows []Row
	Vals []Val
}

// AddVal adds a new value to be range encoded into the given field to the
// PilosaRecord.
func (pr *PilosaRecord) AddVal(frame, field string, value int64) {
	pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: value})
}

// AddRow adds a new bit to be set to the PilosaRecord.
func (pr *PilosaRecord) AddRow(frame string, id uint64) {
	pr.Rows = append(pr.Rows, Row{Frame: frame, ID: id})
}

// AddRowTime adds a new bit to be set with a timestamp to the PilosaRecord.
func (pr *PilosaRecord) AddRowTime(frame string, id uint64, ts time.Time) {
	pr.Rows = append(pr.Rows, Row{Frame: frame, ID: id, Time: ts})
}

// Row represents a bit to set in Pilosa sans column id (which is held by the
// PilosaRecord containg the Row).
type Row struct {
	Frame string
	ID    uint64

	// Time is the timestamp for the bit in Pilosa which is the intersection of
	// this row and the Column in the PilosaRecord which holds this row.
	Time time.Time
}

// Val represents a BSI value to set in a Pilosa field sans column id (which is
// held by the PilosaRecord containing the Val).
type Val struct {
	Frame string
	Field string
	Value int64
}
