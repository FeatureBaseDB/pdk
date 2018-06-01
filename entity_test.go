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

package pdk_test

import (
	"encoding/json"
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

func TestEntityMarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		val  pdk.Object
		exp  string
	}{
		{
			name: "b",
			val:  pdk.B(true),
			exp:  `{"@type":"xsd:boolean","@value":true}`,
		},
		{
			name: "string",
			val:  pdk.S("VALUE"),
			exp:  `"VALUE"`,
		},
		{
			name: "f32",
			val:  pdk.F32(1.1),
			exp:  `{"@type":"xsd:float","@value":1.1}`,
		},
		{
			name: "f64",
			val:  pdk.F64(1.1),
			exp:  `{"@type":"xsd:double","@value":1.1}`,
		},
		{
			name: "i",
			val:  pdk.I(1),
			exp:  `{"@type":"xsd:long","@value":1}`,
		},
		{
			name: "i8",
			val:  pdk.I8(1),
			exp:  `{"@type":"xsd:byte","@value":1}`,
		},
		{
			name: "i16",
			val:  pdk.I16(1),
			exp:  `{"@type":"xsd:short","@value":1}`,
		},
		{
			name: "i32",
			val:  pdk.I32(1),
			exp:  `{"@type":"xsd:int","@value":1}`,
		},
		{
			name: "i64",
			val:  pdk.I64(1),
			exp:  `{"@type":"xsd:long","@value":1}`,
		},
		{
			name: "u",
			val:  pdk.U(1),
			exp:  `{"@type":"unsignedLong","@value":1}`,
		},
		{
			name: "u8",
			val:  pdk.U8(1),
			exp:  `{"@type":"unsignedByte","@value":1}`,
		},
		{
			name: "u16",
			val:  pdk.U16(1),
			exp:  `{"@type":"unsignedShort","@value":1}`,
		},
		{
			name: "u32",
			val:  pdk.U32(1),
			exp:  `{"@type":"unsignedInt","@value":1}`,
		},
		{
			name: "u64",
			val:  pdk.U64(1),
			exp:  `{"@type":"unsignedLong","@value":1}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entity := &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{"k": pdk.Object(test.val)},
			}
			b, err := json.Marshal(entity)
			if err != nil {
				return
			}
			exp := fmt.Sprintf(`{"k":%s}`, test.exp)
			if string(b) != exp {
				t.Fatalf("got %v, expected %v", string(b), exp)
			}
		})
	}
}
