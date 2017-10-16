package pdk

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Mapper is the interface for taking parsed records from the Parser and
// figuring out what bits and values to set in Pilosa. Mappers usually have a
// Translator and a Nexter for converting arbitrary values to monotonic integer
// ids and generating column ids respectively.
type Mapppper interface {
	Map(parsedRecord interface{}) (PilosaRecord, error)
}

type GenericMapper struct {
	Nexter     INexter
	Translator Translator
	Framer     Framer
}

func NewDefaultGenericMapper() *GenericMapper {
	return &GenericMapper{
		Nexter:     NewNexter(),
		Translator: NewMapTranslator(),
		Framer:     DotFrame,
	}
}

func (m *GenericMapper) Map(parsedRecord interface{}) (PilosaRecord, error) {
	pr := &PilosaRecord{}
	err := m.mapInterface([]string{}, parsedRecord, pr)
	if err != nil {
		return *pr, errors.Wrap(err, "mapping record")
	}
	pr.Col = m.Nexter.Next()
	return *pr, nil
}

func (m *GenericMapper) mapInterface(path []string, rec interface{}, pr *PilosaRecord) error {
	switch vt := rec.(type) {
	case map[string]interface{}:
		err := m.mapMap(path, vt, pr)
		if err != nil {
			return err
		}
	case string:
		err := m.mapString(path, vt, pr)
		if err != nil {
			return errors.Wrap(err, "mapping string")
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		err := m.mapInt(path, vt, pr)
		if err != nil {
			return errors.Wrap(err, "mapping int")
		}
	default:
		return errors.Errorf("unsupported type %T, value: %#v", vt, vt)
	}
	return nil
}

func (m *GenericMapper) mapInt(path []string, val interface{}, pr *PilosaRecord) error {
	frame, field, err := m.Framer.Field(path)
	if err != nil {
		return errors.Wrapf(err, "getting frame from path: '%v'", path)
	}
	if frame == "" || field == "" {
		return nil // err == nil and frame or field == "" means skip silently
	}
	switch vt := val.(type) {
	case int:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case int8:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case int16:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case int32:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case int64:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case uint:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case uint8:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case uint16:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case uint32:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case uint64:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	default:
		panic(fmt.Sprintf("mapInt called with non integer type: %T, val: %v", vt, vt))
	}
	return nil
}

func (m *GenericMapper) mapMap(path []string, mapRec map[string]interface{}, pr *PilosaRecord) error {
	for k, v := range mapRec {
		newpath := append(path, k)
		err := m.mapInterface(newpath, v, pr)
		if err != nil {
			return errors.Wrapf(err, "key '%s'", k)
		}
	}
	return nil
}

func (m *GenericMapper) mapString(path []string, val string, pr *PilosaRecord) error {
	frame, err := m.Framer.Frame(path)
	if err != nil {
		return errors.Wrapf(err, "getting frame from path: '%v'", path)
	}
	if frame == "" {
		return nil // err == nil and frame == "" means skip silently
	}
	id, err := m.Translator.GetID(frame, val)
	if err != nil {
		return errors.Wrap(err, "getting id from translator")
	}
	pr.Bits = append(pr.Bits, SetBit{
		Frame: frame,
		Row:   id,
	})
	return nil
}

// PilosaRecord represents a number of set bits and values in a single Column
// in Pilosa.
type PilosaRecord struct {
	Col  uint64
	Bits []SetBit
	Vals []Val
}

// SetBit represents a bit to set in Pilosa sans column id (which is held by the
// PilosaRecord containg the SetBit).
type SetBit struct {
	Frame string
	Row   uint64
	Time  time.Time
}

// Val represents a BSI value to set in a Pilosa field sans column id (which is
// held by the PilosaRecord containg the Val).
type Val struct {
	Frame string
	Field string
	Value int64
}
