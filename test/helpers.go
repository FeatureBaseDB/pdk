package test

import (
	"reflect"
	"testing"
)

// MustBe uses reflect.DeepEqual to assert that thing1 and thing2 are equal, and
// fails otherwise.
func MustBe(t *testing.T, thing1, thing2 interface{}, context ...string) {
	var ctx string
	if len(context) == 0 {
		ctx = ""
	} else {
		ctx = context[0] + ": "
	}
	if !reflect.DeepEqual(thing1, thing2) {
		t.Fatalf("%v'%#v' != '%#v'", ctx, thing1, thing2)
	}
}

// ErrNil asserts that the err is nil and fails otherwise.
func ErrNil(t *testing.T, err error, ctx string) {
	if err != nil {
		t.Fatalf("%v: %v", ctx, err)
	}
}

// Uint64Slice implements the sorting interface on []uint64.
type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
