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

package pdk

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/pilosa/pdk/fake"
	"github.com/pilosa/pdk/mock"
)

func TestEntitySubjecter(t *testing.T) {
	es := SubjectPath([]string{"id"})
	event := fake.GenEvent()
	bytes, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("mashalling event: %v", err)
	}
	var eventjson interface{}
	err = json.Unmarshal(bytes, &eventjson)
	if err != nil {
		t.Fatalf("unmarshaling json: %v", err)
	}

	gp := NewDefaultGenericParser()
	ent, err := gp.Parse(eventjson)
	if err != nil {
		t.Fatalf("parsing evenjson: %v", err)
	}
	subj, err := es.Subject(ent)
	if err != nil {
		t.Fatalf("getting subject: %v", err)
	}
	if subj != event.ID {
		t.Fatalf("exp %v != %v", event.ID, subj)
	}
	if val, ok := ent.Objects[Property("id")]; ok {
		t.Fatalf("should not have found 'id', but got %v", val)
	}

	es = SubjectPath([]string{"geo", "timezone"})
	subj, err = es.Subject(ent)
	if err != nil {
		t.Fatalf("getting subject: %v", err)
	}
	if subj != event.Geo.TimeZone {
		t.Fatalf("exp %v != %v", event.Geo.TimeZone, subj)
	}
	if val, ok := ent.Objects[Property("geo")].(*Entity).Objects[Property("time_zone")]; ok {
		t.Fatalf("should not have found 'geo.time_zone', but got %v", val)
	}

	es = SubjectPath([]string{"thisdoesntexist"})
	subj, err = es.Subject(ent)
	if err == nil {
		t.Fatalf("should have gotten an error looking for 'thisdoesntexist, but got %v", subj)
	}
}

type blaher struct {
	a bool
}

func (b blaher) Blah() {
}

type ptrBlaher struct {
	b bool
}

func (i *ptrBlaher) Blah() {
}

type Blaher interface {
	Blah()
}

type Thing struct {
	v interface{}
}

func TestParseWithNilVals(t *testing.T) {
	rs := &mock.RecordingStatter{}
	gp := NewDefaultGenericParser()
	gp.Stats = rs
	e, err := gp.Parse(map[string]interface{}{"hello": nil, "foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected parse: %v", err)
	}
	if e.Objects["foo"] != S("bar") {
		t.Fatalf("unexpected result: %#v", e)
	}
	if rs.Counts["parser.parseValue.invalid"] != 1 {
		t.Fatalf("unexpected stats with invalid value")
	}

	gp.Strict = true
	_, err = gp.Parse(map[string]interface{}{"hello": nil, "foo": "bar"})
	if err == nil {
		t.Fatalf("expected error, but is nil")
	}
}

func TestAnyImplements(t *testing.T) {
	tests := []Thing{
		Thing{v: blaher{}},
		Thing{v: &blaher{}},
		Thing{v: &ptrBlaher{}},
		// Thing{v: ptrBlaher{}}, // ptrBlaher is not addressable in this case, so Blah cannot be called
	}

	blaherType := reflect.TypeOf(new(Blaher)).Elem()
	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d: %v", i, tst), func(t *testing.T) {
			vtst := reflect.ValueOf(tst.v)
			var v2 reflect.Value
			var ok bool
			if v2, ok = anyImplements(vtst, blaherType); !ok {
				t.Fatalf("v2: %v does not implement blaherType", v2)
			}
			if vb, ok := v2.Interface().(Blaher); ok {
				vb.Blah()
			} else {
				t.FailNow()
			}
		})
	}
}

func TestGenericParserWithEvent(t *testing.T) {
	testRec := fake.GenEvent()
	gp := NewDefaultGenericParser()
	_, err := gp.Parse(testRec)
	if err != nil {
		t.Fatalf("parsing Event as struct: %v", err)
	}

	bytes, err := json.Marshal(testRec)
	if err != nil {
		t.Fatalf("marshalling event: %v", err)
	}

	var thing interface{}
	err = json.Unmarshal(bytes, &thing)
	if err != nil {
		t.Fatalf("unmarshalling event: %v", err)
	}

	_, err = gp.Parse(thing)
	if err != nil {
		t.Fatalf("parsing Event as unmarshalled json: %v", err)
	}
}

func TestGenericParserWithMap(t *testing.T) {
	gp := NewDefaultGenericParser()
	testRec := map[string]interface{}{
		"stringKey": "value",
		"intkey":    32,
		"boolkey":   true,
		"mapkey": map[string]interface{}{
			"innerstring": "innerval",
			"interkey":    int8(8),
		},
		"slicekey": []map[string]interface{}{
			map[string]interface{}{"skey": "value0"},
			map[string]interface{}{"skey": "value1"},
			map[string]interface{}{"s2key": uint64(127)},
		},
		"bs": []byte("hello"),
	}

	actual, err := gp.Parse(testRec)
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if actual.Subject != "" {
		t.Fatalf("expected subject '', but got %v of type %[1]T", actual.Subject)
	}

	expected := &Entity{
		Objects: map[Property]Object{
			Property("stringKey"): S("value"),
			Property("intkey"):    I(32),
			Property("boolkey"):   B(true),
			Property("mapkey"): &Entity{Subject: "", Objects: map[Property]Object{
				Property("innerstring"): S("innerval"),
				Property("interkey"):    I8(8),
			}},
			Property("slicekey"): Objects{
				&Entity{
					Subject: "",
					Objects: map[Property]Object{
						Property("skey"): S("value0"),
					},
				},
				&Entity{
					Subject: "",
					Objects: map[Property]Object{
						Property("skey"): S("value1"),
					},
				},
				&Entity{
					Subject: "",
					Objects: map[Property]Object{
						Property("s2key"): U64(127),
					},
				},
			},
			Property("bs"): S("hello"),
		},
	}
	if err := expected.Equal(actual); err != nil {
		t.Fatal(err)
	}
}

type T struct {
	A int8
	b int16

	C string
	D O
	E []O
	F []map[int]string
	G [6]byte
}

type O struct {
	Q []int
}

func TestGenericParserWithStruct(t *testing.T) {
	gp := NewDefaultGenericParser()
	rec := T{
		A: 42,
		b: 31000,
		C: "sloth",
		D: O{Q: []int{99, 645373}},
		E: []O{{Q: []int{1, 2}}, O{Q: []int{9, 9, 9}}},
		F: []map[int]string{map[int]string{42: "answer", 99: "question"}},
		G: [6]byte{83, 76, 79, 84, 72, 83},
	}

	exp := &Entity{
		Objects: map[Property]Object{
			"A": I8(42),
			"C": S("sloth"),
			"D": &Entity{
				Objects: map[Property]Object{
					"Q": Objects{I(99), I(645373)},
				},
			},
			"E": Objects{
				&Entity{
					Objects: map[Property]Object{
						"Q": Objects{I(1), I(2)},
					},
				},
				&Entity{
					Objects: map[Property]Object{
						"Q": Objects{I(9), I(9), I(9)},
					},
				},
			},
			"F": Objects{
				&Entity{
					Objects: map[Property]Object{
						"42": S("answer"),
						"99": S("question"),
					},
				},
			},
			"G": S("SLOTHS"),
		},
	}
	actual, err := gp.Parse(rec)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	if err := exp.Equal(actual); err != nil {
		t.Fatal(err)
	}
}
