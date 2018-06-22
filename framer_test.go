package pdk_test

import (
	"fmt"
	"testing"

	"github.com/pilosa/pdk"
)

func TestDashField(t *testing.T) {
	tests := []struct {
		path     []string
		ignore   []string
		collapse []string
		expField string
		err      error
	}{
		{
			path:     []string{"hello", "g", "a", "b"},
			expField: "hello-g-a-b",
			err:      nil,
		},
		{
			path:     []string{"hello"},
			expField: "hello",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"g"},
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a"},
			expField: "hello-g-b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a", "g"},
			expField: "hello-b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b"},
			expField: "hello-g-a",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b", "hello", "g", "a"},
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"z", "helloz"},
			expField: "hello-g-a-b",
			err:      nil,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			df := &pdk.DashField{Ignore: test.ignore, Collapse: test.collapse}
			f, err := df.Field(test.path)
			if err != test.err {
				t.Fatal(err)
			}
			if f != test.expField {
				t.Fatalf("unexpected field: %v", f)
			}
		})
	}
}
