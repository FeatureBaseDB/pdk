package pdk_test

import (
	"testing"

	"github.com/pilosa/pdk"
)

func TestCollapsingMapper(t *testing.T) {
	cm := pdk.NewCollapsingMapper()
	e := &pdk.Entity{
		Subject: "blah",
		Objects: map[pdk.Property]pdk.Object{
			"aa": pdk.S("hello"),
			"bb": pdk.I(49),
		},
	}
	pr, err := cm.Map(e)
	if err != nil {
		t.Fatalf("mapping entity: %v", err)
	}
	val, err := cm.Translator.Get("aa", 0)
	if err != nil {
		t.Fatalf("translator get: %v", err)
	}
	if val != pdk.S("hello") {
		t.Fatalf("bad val from translator")
	}

	if len(pr.Rows) != 1 {
		t.Fatalf("wrong rows: %v", pr.Rows)
	}
	if len(pr.Vals) != 1 {
		t.Fatalf("wrong vals: %v", pr.Vals)
	}

}
