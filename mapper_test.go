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

	if len(pr.Rows) != 1 {
		t.Fatalf("wrong rows: %v", pr.Rows)
	}
	if len(pr.Vals) != 1 {
		t.Fatalf("wrong vals: %v", pr.Vals)
	}

}
