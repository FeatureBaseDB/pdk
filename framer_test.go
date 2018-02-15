package pdk_test

import (
	"fmt"
	"testing"

	"github.com/pilosa/pdk"
)

func TestDashFrame(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		ignore   []string
		collapse []string
		expFrame string
		expField string
		err      error
	}{
		{
			path:     []string{"hello", "g", "a", "b"},
			expFrame: "hello-g-a-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello"},
			expFrame: "hello",
			expField: "hello",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"g"},
			expFrame: "",
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a"},
			expFrame: "hello-g-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a", "g"},
			expFrame: "hello-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b"},
			expFrame: "hello-g-a",
			expField: "a",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b", "hello", "g", "a"},
			expFrame: "",
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"z", "helloz"},
			expFrame: "hello-g-a-b",
			expField: "b",
			err:      nil,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			df := &pdk.DashFrame{Ignore: test.ignore, Collapse: test.collapse}
			f, err := df.Frame(test.path)
			if err != test.err {
				t.Fatal(err)
			}
			if f != test.expFrame {
				t.Fatalf("unexpected frame: %v", f)
			}

			_, field, err := df.Field(test.path)
			if err != test.err {
				t.Fatalf("getting field: %v", err)
			}
			if field != test.expField {
				t.Fatalf("unexpected field: %v", field)
			}
		})
	}
}
