package pdk

import (
	"fmt"
	"reflect"
	"testing"
)

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
