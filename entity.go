package pdk

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

// IRI is either a full IRI, or will map to one when the record in which it is
// contained is processed in relation to a context:
// (https://json-ld.org/spec/latest/json-ld/#the-context)
type IRI string

// Property represents a Predicate, and can be turned into a Predicate IRI by a context
type Property string
type Context map[string]interface{}

// Entity is the "root" node of a graph branching out from a certain resource
// denoted by the Subject. This is a convenience vs just handling a list of
// Triples as we expect to structure indexing around a particular class of thing
// which we ingest many instances of as records.
type Entity struct {
	Subject IRI `json:"@id"`
	Objects map[Property]Object
}

func (e *Entity) Equal(e2 *Entity) error {
	if e.Subject != e2.Subject {
		return errors.Errorf("subject '%v' != '%v", e.Subject, e2.Subject)
	}
	return equal(e, e2)
}

func equal(o, o2 Object) error {
	if reflect.TypeOf(o) != reflect.TypeOf(o2) {
		return errors.Errorf("objs are different types: %T and %T", o, o2)
	}
	switch o.(type) {
	case *Entity:
		e, e2 := o.(*Entity), o2.(*Entity)
		if len(e.Objects) != len(e2.Objects) {
			return errors.Errorf("entities have different number of objects, %d and %d", len(e.Objects), len(e2.Objects))
		}
		for pred, obj := range e.Objects {
			obj2, ok := e2.Objects[pred]
			if !ok {
				return errors.Errorf("object 2 has no value at %v", pred)
			}
			eqErr := equal(obj, obj2)
			if eqErr != nil {
				return errors.Wrapf(eqErr, "%v", pred)
			}
		}
	case Objects:
		os, os2 := o.(Objects), o2.(Objects)
		if len(os) != len(os2) {
			return errors.Errorf("object slices have different lengths, %d and %d", len(os), len(os2))
		}
		for i := 0; i < len(os); i++ {
			obj1, obj2 := os[i], os2[i]
			eqErr := equal(obj1, obj2)
			if eqErr != nil {
				return errors.Wrapf(eqErr, "index %d", i)
			}
		}
	default:
		_, ok := o.(Literal)
		_, ok2 := o2.(Literal)
		if !(ok && ok2) {
			return errors.Errorf("expected two literals, but got '%v' and '%v' of %T and %T", o, o2, o, o2)
		}
		if o != o2 {
			return errors.Errorf("'%v' and '%v' not equal", o, o2)
		}
	}
	return nil
}

func NewEntity() *Entity {
	return &Entity{
		Objects: make(map[Property]Object),
	}
}

// EntityWithContext associates a Context
// (https://json-ld.org/spec/latest/json-ld/#the-context) with an Entity so that
// it can be Marshaled to valid and useful JSON-LD.
type EntityWithContext struct {
	Entity
	Context Context `json:"@context"`
}

// Object is an interface satisfied by all things which may appear as objects in
// RDF triples. All literals are objects, but not all objects are literals.
type Object interface {
	isObj()
}

type Objects []Object

func (o Objects) isObj() {}
func (e *Entity) isObj() {}

// MarshalJSON is a custom JSON marshaler for Entity objects to ensure that they
// serialize to valid JSON-LD (https://json-ld.org/ spec/latest/json-ld/). This
// allows for easy (if not particularly performant) interoperation with other
// variants of RDF linked data.
func (e *Entity) MarshalJSON() ([]byte, error) {
	// TODO - this implementation does a lot of in-memory copying for simplicity, can probably be optimized.
	ret := make(map[Property]interface{})
	if e.Subject != "" {
		ret["@id"] = e.Subject
	}
	for k, v := range e.Objects {
		if val, exists := ret[k]; exists {
			return nil, errors.Errorf("invalid entity for json: '%v' already exists at '%v', can't add '%v'", val, k, v)
		}
		ret[k] = v
	}
	for k, v := range e.Objects {
		if val, exists := ret[k]; exists {
			return nil, errors.Errorf("invalid entity for json: '%v' already exists at '%v', can't add '%v'", val, k, v)
		}
		ret[k] = v
	}
	return json.Marshal(ret)
}

// Literal interface is implemented by types which correspond to RDF Literals.
type Literal interface {
	literal()
}

type B bool

func (B) literal() {}

func (B B) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:boolean",
		"@value": B,
	}
	return json.Marshal(ret)
}

type S string

func (S) literal() {}

// TODO define and specifically support these things
// type Location struct {
// 	Latitude  float64 `json:"latitude"`
// 	Longitude float64 `json:"longitude"`
// }

// func (l *Location) MarshalJSON() ([]byte, error) {
// 	ret := make(map[string]interface{})
// 	ret["@type"] = "http://schema.org/GeoCoordinates"
// 	ret["latitude"] = l.Latitude
// 	ret["longitude"] = l.Longitude
// 	return json.Marshal(ret)
// }

// type IPv4 net.IP

// func (ip IPv4) MarshalJSON() ([]byte, error) {
// 	ret := map[string]interface{}{
// 		"@type":  "http://schema.pilosa.com/v0.1/ipv4",
// 		"@value": fmt.Sprintf("%s", ip),
// 	}
// 	return json.Marshal(ret)
// }

// type IPv6 net.IP

// func (ip IPv6) MarshalJSON() ([]byte, error) {
// 	ret := map[string]interface{}{
// 		"@type":  "http://schema.pilosa.com/v0.1/ipv6",
// 		"@value": fmt.Sprintf("%s", ip),
// 	}
// 	return json.Marshal(ret)
// }

type Time time.Time

func (Time) literal() {}

type F32 float32

func (F32) literal() {}

func (F F32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:float",
		"@value": F,
	}
	return json.Marshal(ret)
}

type F64 float64

func (F64) literal() {}

func (F F64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:double",
		"@value": F,
	}
	return json.Marshal(ret)
}

type I int

func (I) literal() {}

func (I I) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:long",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I8 int8

func (I8) literal() {}

func (I I8) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:byte",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I16 int16

func (I16) literal() {}

func (I I16) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:short",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I32 int32

func (I32) literal() {}

func (I I32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:int",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I64 int64

func (I64) literal() {}

func (I I64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:long",
		"@value": I,
	}
	return json.Marshal(ret)
}

type U uint

func (U) literal() {}

func (U U) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedLong",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U8 uint8

func (U8) literal() {}

func (U U8) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedByte",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U16 uint16

func (U16) literal() {}

func (U U16) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedShort",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U32 uint32

func (U32) literal() {}

func (U U32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedInt",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U64 uint64

func (U64) literal() {}

func (U U64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedLong",
		"@value": U,
	}
	return json.Marshal(ret)
}

func (B) isObj()    {}
func (S) isObj()    {}
func (Time) isObj() {}
func (F32) isObj()  {}
func (F64) isObj()  {}
func (I) isObj()    {}
func (I8) isObj()   {}
func (I16) isObj()  {}
func (I32) isObj()  {}
func (I64) isObj()  {}
func (U) isObj()    {}
func (U8) isObj()   {}
func (U16) isObj()  {}
func (U32) isObj()  {}
func (U64) isObj()  {}

// Note: if we start adding lots more literals, reserve numbers 128 and greater.
// Right now, ToBytes and FromBytes use a single byte at the start of the value
// to denote the type - if we grow to 128, we can use first bit as a marker to
// signify that the type is now two bytes.
const (
	bID = iota + 1 // reserve 0 for some future use.
	sID
	f32ID
	f64ID
	iID
	i8ID
	i16ID
	i32ID
	i64ID
	uID
	u8ID
	u16ID
	u32ID
	u64ID
)

// ToBytes converts a literal into a typed byte slice representation.
func ToBytes(l Literal) []byte {
	switch l := l.(type) {
	case B:
		if l {
			return []byte{bID, 1}
		}
		return []byte{bID, 0}
	case S:
		return append([]byte{sID}, l[:]...)
	case F32:
		ret := make([]byte, 5)
		ret[0] = f32ID
		binary.BigEndian.PutUint32(ret[1:], math.Float32bits(float32(l)))
		return ret
	case F64:
		ret := make([]byte, 9)
		ret[0] = f64ID
		binary.BigEndian.PutUint64(ret[1:], math.Float64bits(float64(l)))
		return ret
	case I:
		ret := make([]byte, 9)
		ret[0] = iID
		binary.BigEndian.PutUint64(ret[1:], uint64(l))
		return ret
	case I8:
		return []byte{i8ID, byte(l)}
	case I16:
		ret := make([]byte, 3)
		ret[0] = i16ID
		binary.BigEndian.PutUint16(ret[1:], uint16(l))
		return ret
	case I32:
		ret := make([]byte, 5)
		ret[0] = i32ID
		binary.BigEndian.PutUint32(ret[1:], uint32(l))
		return ret
	case I64:
		ret := make([]byte, 9)
		ret[0] = i64ID
		binary.BigEndian.PutUint64(ret[1:], uint64(l))
		return ret
	case U:
		ret := make([]byte, 9)
		ret[0] = uID
		binary.BigEndian.PutUint64(ret[1:], uint64(l))
		return ret
	case U8:
		return []byte{u8ID, byte(l)}
	case U16:
		ret := make([]byte, 3)
		ret[0] = u16ID
		binary.BigEndian.PutUint16(ret[1:], uint16(l))
		return ret
	case U32:
		ret := make([]byte, 5)
		ret[0] = u32ID
		binary.BigEndian.PutUint32(ret[1:], uint32(l))
		return ret
	case U64:
		ret := make([]byte, 9)
		ret[0] = u64ID
		binary.BigEndian.PutUint64(ret[1:], uint64(l))
		return ret
	default:
		panic("should have covered all literal types in ToBytes switch")
	}
}

// ToString converts a Literal into a string with a type byte prepended.
func ToString(l Literal) string {
	return string(ToBytes(l))
}

// FromString converts a Literal encoded with ToString back to a Literal.
func FromString(s string) Literal {
	return FromBytes([]byte(s))
}

// FromBytes converts an encoded byte slice (from ToBytes) back to a Literal.
// DEV: May add an error and bounds checking.
func FromBytes(bs []byte) Literal {
	// if len(bs) < 2 {
	// 	return nil, errors.Errorf("byte slice too short: %x", bs)
	// }
	switch bs[0] {
	case bID:
		return B(bs[1] > 0)
	case sID:
		return S(string(bs[1:]))
	case f32ID:
		return F32(math.Float32frombits(binary.BigEndian.Uint32(bs[1:])))
	case f64ID:
		return F64(math.Float64frombits(binary.BigEndian.Uint64(bs[1:])))
	case iID:
		return I(binary.BigEndian.Uint64(bs[1:]))
	case i8ID:
		return I8(bs[1])
	case i16ID:
		return I16(binary.BigEndian.Uint16(bs[1:]))
	case i32ID:
		return I32(binary.BigEndian.Uint32(bs[1:]))
	case i64ID:
		return I64(binary.BigEndian.Uint64(bs[1:]))
	case uID:
		return U(binary.BigEndian.Uint64(bs[1:]))
	case u8ID:
		return U8(bs[1])
	case u16ID:
		return U16(binary.BigEndian.Uint16(bs[1:]))
	case u32ID:
		return U32(binary.BigEndian.Uint32(bs[1:]))
	case u64ID:
		return U64(binary.BigEndian.Uint64(bs[1:]))
	default:
		panic("should have covered all literal types in FromBytes switch")
	}
}
