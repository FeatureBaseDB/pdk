package pdk_test

import (
	"testing"

	"github.com/pilosa/pdk"
)

func TestGenericMapper(t *testing.T) {
	gm := pdk.NewDefaultGenericMapper()
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
	for i := 0; i < 3; i++ {
		if i == 2 {
			testRec["stringKey"] = "value2"
		}
		pr, err := gm.Map(testRec)
		if err != nil {
			t.Fatalf("mapping test record: %v", err)
		}

		// Check Bits
		expBits := map[pdk.SetBit]struct{}{
			{Frame: "stringKey", Row: 0}:          struct{}{},
			{Frame: "mapkey-innerstring", Row: 0}: struct{}{},
			{Frame: "slicekey-skey", Row: 0}:      struct{}{},
			{Frame: "slicekey-skey", Row: 1}:      struct{}{},
			{Frame: "bs", Row: 0}:                 struct{}{},
			{Frame: "boolkey", Row: 0}:            struct{}{},
		}
		if i == 2 {
			delete(expBits, pdk.SetBit{Frame: "stringKey", Row: 0})
			expBits[pdk.SetBit{Frame: "stringKey", Row: 1}] = struct{}{}
		}
		for _, sb := range pr.Bits {
			if _, ok := expBits[sb]; !ok {
				t.Fatalf("%v not expected in set bits", sb)
			} else {
				delete(expBits, sb)
			}
		}
		if len(expBits) != 0 {
			t.Fatalf("leftover expected bits not found: %v", expBits)
		}

		// Check Vals
		expVals := map[pdk.Val]struct{}{
			{Frame: "intkey", Field: "intkey", Value: 32}:           struct{}{},
			{Frame: "mapkey-interkey", Field: "interkey", Value: 8}: struct{}{},
			{Frame: "slicekey-s2key", Field: "s2key", Value: 127}:   struct{}{},
		}
		for _, v := range pr.Vals {
			if _, ok := expVals[v]; !ok {
				t.Fatalf("%v not expected in vals", v)
			} else {
				delete(expVals, v)
			}
		}
		if len(expVals) != 0 {
			t.Fatalf("leftover expected vals not found: %v", expVals)
		}
		if pr.Col != uint64(i) {
			t.Fatalf("column not set properly, i=%v, col=%v", i, pr.Col)
		}
	}
}
