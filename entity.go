package pdk

import (
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
)

type IRI string
type Predicate string
type Context map[string]interface{}

// Entity is the "root" node of a graph branching out from a certain resource
// denoted by the Subject. This is a convenience vs just handling a list of
// Triples as we expect to structure indexing around a particular class of thing
// which we ingest many instances of as records. The properties of an Entity are
// split into two groups one of which may contain other Entities (nested), and
// the other which contains only Literals.
type Entity struct {
	Subject IRI `json:"@id"`
	Objects map[Predicate]Object
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
		if o != o2 {
			return errors.Errorf("'%v' and '%v' not equal", o, o2)
		}
	}
	return nil
}

func NewEntity() *Entity {
	return &Entity{
		Objects: make(map[Predicate]Object),
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
	ret := make(map[Predicate]interface{})
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

type B bool

func (B B) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:boolean",
		"@value": B,
	}
	return json.Marshal(ret)
}

type S string

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

type F32 float32

func (F F32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:float",
		"@value": F,
	}
	return json.Marshal(ret)
}

type F64 float64

func (F F64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:double",
		"@value": F,
	}
	return json.Marshal(ret)
}

type I int

func (I I) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:long",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I8 int8

func (I I8) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:byte",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I16 int16

func (I I16) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:short",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I32 int32

func (I I32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:int",
		"@value": I,
	}
	return json.Marshal(ret)
}

type I64 int64

func (I I64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "xsd:long",
		"@value": I,
	}
	return json.Marshal(ret)
}

type U uint

func (U U) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedLong",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U8 uint8

func (U U8) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedByte",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U16 uint16

func (U U16) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedShort",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U32 uint32

func (U U32) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedInt",
		"@value": U,
	}
	return json.Marshal(ret)
}

type U64 uint64

func (U U64) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{
		"@type":  "unsignedLong",
		"@value": U,
	}
	return json.Marshal(ret)
}

func (b B) isObj()   {}
func (s S) isObj()   {}
func (f F32) isObj() {}
func (f F64) isObj() {}
func (i I) isObj()   {}
func (i I8) isObj()  {}
func (i I16) isObj() {}
func (i I32) isObj() {}
func (i I64) isObj() {}
func (u U) isObj()   {}
func (u U8) isObj()  {}
func (u U16) isObj() {}
func (u U32) isObj() {}
func (u U64) isObj() {}
