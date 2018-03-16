package pdk

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

// GenericParser tries to make no assumptions about the value passed to its
// Parse method. At the top level it accepts a map or struct (or pointer or
// interface holding one of these).
type GenericParser struct {
	Subjecter       Subjecter
	EntitySubjecter EntitySubjecter
	Framer          Framer
	SubjectAll      bool

	// IncludeUnexportedFields controls whether unexported struct fields will be
	// included when parsing.
	IncludeUnexportedFields bool // TODO: I don't think this actually works
}

// EntitySubjecter is an alternate interface for getting the Subject of a
// record, which operates on the parsed Entity rather than the unparsed data.
type EntitySubjecter interface {
	Subject(e *Entity) (string, error)
}

// SubjectPath is an EntitySubjecter which extracts a subject by walking the
// Entity properties denoted by the strings in SubjectPath.
type SubjectPath []string

// Subject implements EntitySubjecter.
func (p SubjectPath) Subject(e *Entity) (string, error) {
	var next Object = e
	var prev *Entity
	var item string
	for _, item = range p {
		ent, ok := next.(*Entity)
		if !ok {
			return "", errors.Errorf("can't get '%v' out of non-entity %#v.", item, next)
		}
		prev = ent
		next = ent.Objects[Property(item)]
	}
	delete(prev.Objects, Property(item)) // remove the subject from the entity so that it isn't indexed
	switch next.(type) {
	case I, I8, I16, I32, I64, U, U8, U16, U32, U64:
		return fmt.Sprintf("%d", next), nil
	case F32, F64:
		return fmt.Sprintf("%f", next), nil
	case S:
		return string(next.(S)), nil
	default:
		return "", fmt.Errorf("can't make %v of type %T an IRI", next, next)
	}
}

// Subjecter is an interface for getting the Subject of a record.
type Subjecter interface {
	Subject(d interface{}) (string, error)
}

// SubjectFunc is a wrapper like http.HandlerFunc which allows you to use a bare
// func as a Subjecter.
type SubjectFunc func(d interface{}) (string, error)

// Subject implements Subjecter.
func (s SubjectFunc) Subject(d interface{}) (string, error) { return s(d) }

// BlankSubjecter is a Subjecter which always returns an empty subject.
// Typically this means that a sequential ID will be generated for each record.
type BlankSubjecter struct{}

// Subject implements Subjecter, and always returns an empty string and nil
// error.
func (b BlankSubjecter) Subject(d interface{}) (string, error) { return "", nil }

// NewDefaultGenericParser returns a GenericParser with basic implementations of
// its components. In order to track the mapping of Pilosa columns to records,
// you must replace the Subjecter with something other than a BlankSubjecter.
func NewDefaultGenericParser() *GenericParser {
	return &GenericParser{
		Subjecter: BlankSubjecter{},
		Framer:    &DashFrame{},
	}
}

// Parse of the GenericParser tries to parse any value into a pdk.Entity.
func (m *GenericParser) Parse(data interface{}) (e *Entity, err error) {
	val := reflect.ValueOf(data)
	// dereference pointers, and get concrete values from interfaces
	val = deref(val)
	// Map and Struct are the only valid Kinds at the top level.
	switch val.Kind() {
	case reflect.Map:
		e, err = m.parseMap(val)
	case reflect.Struct:
		e, err = m.parseStruct(val)
	default:
		e, err = nil, errors.Errorf("unsupported kind, '%v' in GenericParser: %v", val.Kind(), data)
	}
	if err != nil {
		return e, err
	}
	var subj string
	if !m.SubjectAll {
		if m.EntitySubjecter != nil {
			subj, err = m.EntitySubjecter.Subject(e)
		} else {
			subj, err = m.Subjecter.Subject(data)
		}
		e.Subject = IRI(subj)
	}
	return e, err
}

func deref(val reflect.Value) reflect.Value {
	knd := val.Kind()
	i := 0
	for knd == reflect.Ptr || knd == reflect.Interface {
		val = val.Elem()
		knd = val.Kind()
		i++
		if i > 100 {
			panic(fmt.Sprintf("deref loop with: %#v", val.Interface()))
		}
	}
	return val
}

func (m *GenericParser) parseMap(val reflect.Value) (*Entity, error) {
	ent := NewEntity()
	if m.SubjectAll {
		subj, err := m.Subjecter.Subject(val.Interface())
		if err != nil {
			return nil, errors.Wrap(err, "getting subject")
		}
		ent.Subject = IRI(subj)
	}

	for _, kval := range val.MapKeys() {
		prop, err := m.getProperty(kval)
		if err != nil {
			return nil, errors.Wrapf(err, "getting property from '%v'", kval)
		}
		vval := val.MapIndex(kval)
		vval = deref(vval)
		if _, ok := ent.Objects[prop]; ok {
			return nil, errors.Errorf("property collision in objects at '%v', val '%v'", kval, vval)
		}
		obj, err := m.parseValue(vval)
		if err != nil {
			return ent, errors.Wrapf(err, "parsing value '%v' at '%v':", vval, kval)
		}
		ent.Objects[prop] = obj
	}
	return ent, nil
}

func (m *GenericParser) parseStruct(val reflect.Value) (*Entity, error) {
	ent := NewEntity()
	subj, err := m.Subjecter.Subject(val.Interface())
	if err != nil {
		return nil, errors.Wrapf(err, "getting subject from '%v", val.Interface())
	}
	ent.Subject = IRI(subj)

	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		if field.PkgPath != "" && !m.IncludeUnexportedFields {
			continue // this field is unexported, so we ignore it.
		}
		fieldv := val.Field(i)
		fieldv = deref(fieldv)
		obj, err := m.parseValue(fieldv)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing field:%v value:%v", field, fieldv)
		}
		if _, ok := ent.Objects[Property(field.Name)]; ok {
			return nil, errors.Errorf("unexpected name collision with struct field '%v", field.Name)
		}
		ent.Objects[Property(field.Name)] = obj
	}
	return ent, nil
}

func (m *GenericParser) parseValue(val reflect.Value) (Object, error) {
	switch val.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		lit, err := m.parseLit(val)
		if err != nil {
			return nil, errors.Wrap(err, "parsing literal")
		}
		return lit, nil
	case reflect.Map, reflect.Struct:
		return m.parseObj(val)
	case reflect.Array, reflect.Slice:
		return m.parseContainer(val)
	case reflect.Invalid, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return nil, errors.Errorf("unsupported kind: %v", val.Kind())
	case reflect.Ptr, reflect.Interface:
		return nil, errors.Errorf("shouldn't be called with pointer or interface, got: %v of kind %v", val, val.Kind())
	default:
		panic("all kinds should have been covered in parseValue")
	}

}

// parseContainer parses arrays and slices.
func (m *GenericParser) parseContainer(val reflect.Value) (Object, error) {
	if dtype := val.Type(); dtype.Elem().Kind() == reflect.Uint8 {
		if dtype.Kind() == reflect.Slice {
			return S(val.Bytes()), nil
		} else if dtype.Kind() == reflect.Array {
			ret := make([]byte, dtype.Len())
			for i := 0; i < dtype.Len(); i++ {
				ret[i] = val.Index(i).Interface().(byte)
			}
			return S(ret), nil
		} else {
			panic("should only be slice or array")
		}
	}

	ret := make(Objects, val.Len())
	for i := 0; i < val.Len(); i++ {
		ival := val.Index(i)
		ival = deref(ival)
		iobj, err := m.parseValue(ival)
		if err != nil {
			return nil, errors.Wrap(err, "parsing value")
		}
		ret[i] = iobj
	}
	return ret, nil
}

// parseLit parses literal values (the leaves of the tree).
func (m *GenericParser) parseLit(val reflect.Value) (Object, error) {
	switch val.Kind() {
	case reflect.Bool:
		return B(val.Bool()), nil
	case reflect.Int:
		return I(val.Int()), nil
	case reflect.Int8:
		return I8(val.Int()), nil
	case reflect.Int16:
		return I16(val.Int()), nil
	case reflect.Int32:
		return I32(val.Int()), nil
	case reflect.Int64:
		return I64(val.Int()), nil
	case reflect.Uint:
		return U(val.Uint()), nil
	case reflect.Uint8:
		return U8(val.Uint()), nil
	case reflect.Uint16:
		return U16(val.Uint()), nil
	case reflect.Uint32:
		return U32(val.Uint()), nil
	case reflect.Uint64:
		return U64(val.Uint()), nil
	case reflect.Float32:
		return F32(val.Float()), nil
	case reflect.Float64:
		return F64(val.Float()), nil
	case reflect.Complex64:
		return nil, errors.New("unsupported kind of literal Complex64")
	case reflect.Complex128:
		return nil, errors.New("unsupported kind of literal Complex128")
	case reflect.Array, reflect.Slice:
		return nil, errors.New("nested slices/arrays of literals are not supported - parseLit should not be called with these kinds of values")
	case reflect.String:
		return S(val.String()), nil
	default:
		return nil, errors.Errorf("kind %v is not supported", val.Kind())
	}
}

func (m *GenericParser) parseObj(val reflect.Value) (Object, error) {
	switch val.Kind() {
	case reflect.Map:
		return m.parseMap(val)
	case reflect.Struct:
		return m.parseStruct(val)
	default:
		return nil, errors.Errorf("should only be called with maps and structs, not %v", val.Kind())
	}
}

// Properteer is the interface which should be implemented by types which want
// to explicitly define how they should be interpreted as a string for use as a
// Property when they are used as a map key.
type Properteer interface {
	Property() Property
}

// anyImplements determines whether an interface is implemented by a value, or a
// pointer to that value. It returns a potentially new value which can be cast
// to the interface, and a boolean. If the boolean is false, the value will be
// the zero value, and cannot be cast to the interface.
func anyImplements(thing reflect.Value, iface reflect.Type) (reflect.Value, bool) {
	if thing.Type().Implements(iface) {
		return thing, true
	} else if thing.CanAddr() && reflect.PtrTo(thing.Type()).Implements(iface) {
		return thing.Addr(), true
	}
	return reflect.Value{}, false
}

// getProperty turns anything that can be a map key into a string. Exceptions
// are channels, interface values whose dynamic types are channels, structs
// which contain channels, and pointers to any of these things.
func (m *GenericParser) getProperty(mapKey reflect.Value) (Property, error) {
	properteerType := reflect.TypeOf(new(Properteer)).Elem()
	stringerType := reflect.TypeOf(new(fmt.Stringer)).Elem()

	// if mapKey implements the pdk.Properteer interface, use that.
	if k2, ok := anyImplements(mapKey, properteerType); ok {
		return k2.Interface().(Properteer).Property(), nil
	}
	// if mapKey implements fmt.Stringer, use that.
	if k2, ok := anyImplements(mapKey, stringerType); ok {
		return Property(k2.Interface().(fmt.Stringer).String()), nil
	}

	// Otherwise, handle all comparable types (see https://golang.org/ref/spec#Comparison_operators):
	mapKey = deref(mapKey)
	switch mapKey.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String:
		return Property(fmt.Sprintf("%v", mapKey)), nil
	case reflect.Chan:
		return "", errors.New("channel cannot be converted to property")
	case reflect.Array:
		return "", errors.New("array cannot be converted to property")
	case reflect.Struct:
		return "", errors.New("struct cannot be converted to property")
	case reflect.Complex128, reflect.Complex64:
		return "", errors.New("complex value cannot be converted to property")
	default:
		return "", errors.Errorf("unexpected kind: %v mapKey: %v", mapKey.Kind(), mapKey)
	}
}
