package pdk_test

import (
	"fmt"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"

	"testing"
)

func TestToAndFromBytes(t *testing.T) {
	tests := []pdk.Literal{
		pdk.B(false),
		pdk.B(true),
		pdk.S(""),
		pdk.S("hel+lorésumé."),
		pdk.F32(0),
		pdk.F32(32.34),
		pdk.F64(99.3374),
		pdk.F64(0),
		pdk.I(0),
		pdk.I(190),
		pdk.I(3123456789),
		pdk.I(5123456789),
		pdk.I(8446744100000000000),
		pdk.U(0),
		pdk.U(190),
		pdk.U(3123456789),
		pdk.U(5123456789),
		pdk.U(6744100000000000),
		pdk.I8(0),
		pdk.I8(127),
		pdk.I8(-127),
		pdk.I16(0),
		pdk.I16(100),
		pdk.I16(-32000),
		pdk.I32(0),
		pdk.I32(100),
		pdk.I32(1123456789),
		pdk.I32(-1123456789),
		pdk.I64(0),
		pdk.I64(1234567890),
		pdk.I64(-8446744100000000000),
		pdk.U8(0),
		pdk.U8(100),
		pdk.U8(255),
		pdk.U16(0),
		pdk.U16(100),
		pdk.U16(65535),
		pdk.U32(0),
		pdk.U32(100),
		pdk.U32(3123456789),
		pdk.U64(0),
		pdk.U64(1234567890),
		pdk.U64(18446744000000000000),
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d: ", i), func(t *testing.T) {
			bs := pdk.ToBytes(tst)
			nl := pdk.FromBytes(bs)
			if nl != tst {
				t.Fatalf("expected: %#v, actual: %#v", tst, nl)
			}
		})
	}
}

func TestSetString(t *testing.T) {
	tests := []struct {
		name   string
		path   []string
		entity *pdk.Entity
		expErr error
	}{
		{
			name:   "Empty Entity, 2 path",
			path:   []string{"one", "two"},
			entity: pdk.NewEntity(),
		},
		{
			name:   "Empty Entity, 1 path",
			path:   []string{"one"},
			entity: pdk.NewEntity(),
		},
		{
			name: "no path",
			path: []string{"one", "two"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("VALUE")},
			},
			expErr: pdk.ErrPathNotFound,
		},
		{
			name: "overwrite",
			path: []string{"one"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("ZALUE")},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.entity.SetString("VALUE", test.path...)
			if errors.Cause(err) != test.expErr {
				t.Fatalf("got err %v, expected %v", err, test.expErr)
			}
			if err != nil {
				return
			}
			val, err := test.entity.Literal(test.path...)
			if err != nil {
				t.Fatalf("reading value from %#v: %v", test.entity, err)
			}
			valS, ok := val.(pdk.S)
			if !ok {
				t.Fatalf("set value is not a string")
			}
			if valS != pdk.S("VALUE") {
				t.Fatalf("set value is not VALUE: %v", valS)
			}
		})
	}
}

func TestGetLiteral(t *testing.T) {
	tests := []struct {
		name   string
		path   []string
		entity *pdk.Entity
		expErr error
		expLit pdk.Literal
	}{
		{
			name: "single item path",
			path: []string{"one"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("VALUE")},
			},
			expLit: pdk.S("VALUE"),
		},
		{
			name: "two item path",
			path: []string{"one", "two"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": &pdk.Entity{
					Objects: map[pdk.Property]pdk.Object{"two": pdk.S("VALUE")},
				}},
			},
			expLit: pdk.S("VALUE"),
		},
		{
			name: "empty path",
			path: []string{},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("VALUE")},
			},
			expErr: pdk.ErrEmptyPath,
		},
		{
			name: "non-entity in path",
			path: []string{"one", "two"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("VALUE")},
			},
			expErr: pdk.ErrPathNotFound,
		},
		{
			name: "non-existent path",
			path: []string{"zip", "two"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("VALUE")},
			},
			expErr: pdk.ErrPathNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lit, err := test.entity.Literal(test.path...)
			if errors.Cause(err) != test.expErr {
				t.Fatalf("got %v, expected %v", err, test.expErr)
			}
			if err != nil {
				return
			}
			if test.expLit != lit {
				t.Fatalf("got %v, expected %v", lit, test.expLit)
			}
		})
	}
}

func TestGetF64(t *testing.T) {
	tests := []struct {
		name   string
		path   []string
		entity *pdk.Entity
		expErr error
		exp    pdk.F64
	}{
		{
			name: "single item path",
			path: []string{"one"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.F64(1.1)},
			},
			exp: pdk.F64(1.1),
		},
		{
			name: "two item path",
			path: []string{"one", "two"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": &pdk.Entity{
					Objects: map[pdk.Property]pdk.Object{"two": pdk.F64(1.1)},
				}},
			},
			exp: pdk.F64(1.1),
		},
		{
			name: "empty path",
			path: []string{},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.F64(1.1)},
			},
			expErr: pdk.ErrEmptyPath,
		},
		{
			name: "unexpected type",
			path: []string{"one"},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"one": pdk.S("blah")},
			},
			expErr: pdk.ErrUnexpectedType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f64, err := test.entity.F64(test.path...)
			if errors.Cause(err) != test.expErr {
				t.Fatalf("got %v, expected %v", err, test.expErr)
			}
			if err != nil {
				return
			}
			if test.exp != f64 {
				t.Fatalf("got %v, expected %v", f64, test.exp)
			}
		})
	}
}
