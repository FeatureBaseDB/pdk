package pdk

import (
	"fmt"
	"reflect"
	"strconv"
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

// GenericMapper tries to make no assumptions about the value passed to its Map
// method. Look at the type switch in the `mapInterface` method to see what
// types it supports.
type GenericMapper struct {
	Nexter     INexter
	Translator Translator
	Framer     Framer

	// if expandSlices is true, the slice index for any value in a slice will be
	// added to the path for that value (which will generate a unique frame name
	// for each index in the slice). Otherwise, each value in the slice will get
	// have the same path, and so the same frame will have multiple values set
	// for this particular column.
	expandSlices bool
}

// NewDefaultGenericMapper returns a GenericMapper with a LocalNexter, in-memory
// MapTranslator, and the simple DotFrame Framer.
func NewDefaultGenericMapper() *GenericMapper {
	return &GenericMapper{
		Nexter:     NewNexter(),
		Translator: NewMapTranslator(),
		Framer:     DashFrame,
	}
}

// Map of the GenericMapper tries to map any value into a PilosaRecord.
func (m *GenericMapper) Map(parsedRecord interface{}) (PilosaRecord, error) {
	pr := &PilosaRecord{}
	err := m.mapInterface([]string{}, parsedRecord, pr)
	if err != nil {
		return *pr, errors.Wrap(err, "mapping record")
	}
	pr.Col = m.Nexter.Next()
	return *pr, nil
}

// isArbitrarySlice returns true if rec is a slice of any type except []byte
// (aka []uint8). Byte slices are handled specially by mapInterface and
// mapByteSlice, and not by the generic mapSlice method.
func isArbitrarySlice(rec interface{}) bool {
	v := reflect.ValueOf(rec)
	if v.Kind() == reflect.Slice {
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return true
		}
	}
	return false
}

func (m *GenericMapper) mapInterface(path []string, rec interface{}, pr *PilosaRecord) error {
	if isArbitrarySlice(rec) {
		return m.mapSlice(path, rec, pr)
	}
	switch vt := rec.(type) {
	case map[string]interface{}:
		return m.mapMap(path, vt, pr)
	case string:
		err := m.mapString(path, vt, pr)
		return errors.Wrap(err, "mapping string")
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		err := m.mapInt(path, vt, pr)
		return errors.Wrap(err, "mapping int")
	case bool:
		err := m.mapBool(path, vt, pr)
		return errors.Wrap(err, "mapping bool")
	case []byte:
		err := m.mapByteSlice(path, vt, pr)
		return errors.Wrap(err, "mapping byte slice")
	case float32, float64:
		err := m.mapFloat(path, vt, pr)
		return errors.Wrap(err, "mapping float")
	default:
		return errors.Errorf("unsupported type %T, value: %#v", vt, vt)
	}
}

func (m *GenericMapper) mapSlice(path []string, val interface{}, pr *PilosaRecord) error {
	v := reflect.ValueOf(val)
	for i := 0; i < v.Len(); i++ {
		element := v.Index(i).Interface()
		ipath := path
		if m.expandSlices {
			ipath = append(path, strconv.Itoa(i))
		}
		err := m.mapInterface(ipath, element, pr)
		if err != nil {
			return errors.Wrap(err, "mapping value in slice")
		}
	}
	return nil
}

func (m *GenericMapper) mapByteSlice(path []string, val []byte, pr *PilosaRecord) error {
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
	pr.Rows = append(pr.Rows, Row{
		Frame: frame,
		ID:    id,
	})
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
	pr.Rows = append(pr.Rows, Row{
		Frame: frame,
		ID:    id,
	})
	return nil
}

func (m *GenericMapper) mapBool(path []string, val bool, pr *PilosaRecord) error {
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
	pr.Rows = append(pr.Rows, Row{
		Frame: frame,
		ID:    id,
	})
	return nil
}

func (m *GenericMapper) mapFloat(path []string, val interface{}, pr *PilosaRecord) error {
	frame, field, err := m.Framer.Field(path)
	if err != nil {
		return errors.Wrapf(err, "getting frame from path: '%v'", path)
	}
	if frame == "" || field == "" {
		return nil // err == nil and frame or field == "" means skip silently
	}
	switch vt := val.(type) {
	case float32:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	case float64:
		pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: int64(vt)})
	default:
		panic(fmt.Sprintf("mapFloat called with non float type: %T, val: %v", vt, vt))
	}
	return nil
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
