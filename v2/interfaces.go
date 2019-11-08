package pdk

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

// Source is an interface implemented by sources of data which can be
// ingested into Pilosa. Each Record returned from Record is described
// by the slice of Fields returned from Source.Schema directly after
// the call to Source.Record. If the error returned from Source.Record
// is nil, then the call to Schema which applied to the previous
// Record also applies to this Record. Source implementations are
// fundamentally not threadsafe (due to the interplay between Record
// and Schema).
type Source interface {

	// Record returns a data record, and an optional error. If the
	// error is ErrSchemaChange, then the record is valid, but one
	// should call Source.Schema to understand how each of its fields
	// should be interpreted.
	Record() (Record, error)

	// Schema returns a slice of Fields which applies to the most
	// recent Record returned from Source.Record. Every Field has a
	// name and a type, and depending on the concrete type of the
	// Field, may have other information which is relevant to how it
	// should be indexed.
	Schema() []Field
}

type Error string

func (e Error) Error() string { return string(e) }

// ErrSchemaChange is returned from Source.Record when the returned
// record has a different schema from the previous record.
const ErrSchemaChange = Error("this record has a different schema from the previous record (or is the first one delivered). Please call Source.Schema() to fetch the schema in order to properly decode this record")

type Record interface {
	// Commit notifies the Source which produced this record that it
	// and any record which came before it have been completely
	// processed. The Source can then take any necessary action to
	// record which records have been processed, and restart from the
	// earliest unprocessed record in the event of a failure.
	Commit() error

	Data() []interface{}
}

type Field interface {
	Name() string
	PilosafyVal(val interface{}) (interface{}, error) // TODO rename this
}

type IDField struct {
	NameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool
}

func (id IDField) Name() string { return id.NameVal }
func (id IDField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toUint64(val)
}

type BoolField struct {
	NameVal string
}

func (b BoolField) Name() string { return b.NameVal }
func (b BoolField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toBool(val)
}

type StringField struct {
	NameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool
}

func (s StringField) Name() string { return s.NameVal }
func (s StringField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toString(val)
}

type IntField struct {
	NameVal string
	Min     *int64
	Max     *int64
}

func (i IntField) Name() string { return i.NameVal }
func (i IntField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toInt64(val)
}

type DecimalField struct {
	NameVal string
	Scale   uint
}

func (d DecimalField) Name() string { return d.NameVal }
func (i DecimalField) PilosafyVal(val interface{}) (interface{}, error) {
	var tmp [8]byte
	if val == nil {
		return nil, nil
	}
	switch vt := val.(type) {
	case float32:
		v64 := float64(vt) * math.Pow(10, float64(i.Scale))
		return int64(v64), nil
	case float64:
		vt = vt * math.Pow(10, float64(i.Scale))
		return int64(vt), nil
	case []byte:
		if len(vt) == 8 {
			return int64(binary.BigEndian.Uint64(vt)), nil
		} else if len(vt) < 8 {
			copy(tmp[8-len(vt):], vt)
			return binary.BigEndian.Uint64(tmp[:]), nil
		} else {
			return nil, errors.Errorf("can't support decimals of greater than 8 bytes, got %d for %s", len(vt), i.Name())
		}
	default:
		return toInt64(val)
	}
}

type StringArrayField struct {
	NameVal string
}

func (s StringArrayField) Name() string { return s.NameVal }
func (i StringArrayField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toStringArray(val)
}

func toUint64(val interface{}) (uint64, error) {
	switch vt := val.(type) {
	case uint:
		return uint64(vt), nil
	case uint8:
		return uint64(vt), nil
	case uint16:
		return uint64(vt), nil
	case uint32:
		return uint64(vt), nil
	case uint64:
		return vt, nil
	case int:
		return uint64(vt), nil
	case int8:
		return uint64(vt), nil
	case int16:
		return uint64(vt), nil
	case int32:
		return uint64(vt), nil
	case int64:
		return uint64(vt), nil
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to uint64", vt)
	}
}

func toBool(val interface{}) (bool, error) {
	switch vt := val.(type) {
	case bool:
		return vt, nil
	case byte:
		return vt != 0, nil
	default:
		return false, errors.Errorf("couldn't convert %v of %[1]T to bool", vt)
	}
}

func toString(val interface{}) (string, error) {
	switch vt := val.(type) {
	case string:
		return vt, nil
	case []byte:
		return string(vt), nil
	default:
		return "", errors.Errorf("couldn't convert %v of %[1]T to string", vt)
	}
}

func toInt64(val interface{}) (int64, error) {
	switch vt := val.(type) {
	case uint:
		return int64(vt), nil
	case uint8:
		return int64(vt), nil
	case uint16:
		return int64(vt), nil
	case uint32:
		return int64(vt), nil
	case uint64:
		return int64(vt), nil
	case int:
		return int64(vt), nil
	case int8:
		return int64(vt), nil
	case int16:
		return int64(vt), nil
	case int32:
		return int64(vt), nil
	case int64:
		return vt, nil
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to int64", vt)
	}
}

func toStringArray(val interface{}) ([]string, error) {
	switch vt := val.(type) {
	case []string:
		return vt, nil
	case []interface{}:
		ret := make([]string, len(vt))
		for i, v := range vt {
			vs, ok := v.(string)
			if !ok {
				return nil, errors.Errorf("couldn't convert []interface{} to []string, value %v of type %[1]T at %d", v, i)
			}
			ret[i] = vs
		}
		return ret, nil
	default:
		return nil, errors.Errorf("couldn't convert %v of %[1]T to []string", vt)
	}
}
