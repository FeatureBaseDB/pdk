package pdk

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

type GenericMapper struct {
	Translator    Translator
	ColTranslator FrameTranslator
}

func NewDefaultGenericMapper() *GenericMapper {
	return &GenericMapper{
		Translator:    NewMapTranslator(),
		ColTranslator: NewMapFrameTranslator(),
	}
}

func (m *GenericMapper) Map(e *Entity) (PilosaRecord, error) {
	pr := PilosaRecord{}
	col, err := m.ColTranslator.GetID(e.Subject)
	pr.Col = col
	if err != nil {
		return pr, errors.Wrap(err, "getting column id from subject")
	}
	for prop, val := range e.Objects {
		fmt.Println(prop, val)
	}
	return pr, nil
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
func (pr PilosaRecord) AddVal(frame, field string, value int64) {
	pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: value})
}

// AddRow adds a new bit to be set to the PilosaRecord.
func (pr PilosaRecord) AddRow(frame string, id uint64) {
	pr.Rows = append(pr.Rows, Row{Frame: frame, ID: id})
}

// AddRowTime adds a new bit to be set with a timestamp to the PilosaRecord.
func (pr PilosaRecord) AddRowTime(frame string, id uint64, ts time.Time) {
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
// held by the PilosaRecord containg the Val).
type Val struct {
	Frame string
	Field string
	Value int64
}
