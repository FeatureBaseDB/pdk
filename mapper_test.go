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
		"mapkey": map[string]interface{}{
			"innerstring": "innerval",
			"interkey":    int8(8),
		},
	}
	pr, err := gm.Map(testRec)
	if err != nil {
		t.Fatalf("mapping test record: %v", err)
	}
	expBits := map[pdk.SetBit]struct{}{
		{Frame: "stringKey", Row: 0}:          struct{}{},
		{Frame: "mapkey.innerstring", Row: 0}: struct{}{},
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

	expVals := map[pdk.Val]struct{}{
		{Frame: "intkey", Field: "intkey", Value: 32}:           struct{}{},
		{Frame: "mapkey.interkey", Field: "interkey", Value: 8}: struct{}{},
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

}
