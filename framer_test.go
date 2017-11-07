package pdk_test

import (
	"testing"

	"github.com/pilosa/pdk"
)

func TestDotFrame(t *testing.T) {
	tests := []struct {
		name     string
		path     []string
		expected string
		err      error
	}{
		{
			path:     []string{"hello", "g", "a", "b"},
			expected: "hello-g-a-b",
			err:      nil,
		},
		{
			path:     []string{"hello"},
			expected: "hello",
			err:      nil,
		},
	}

	for _, test := range tests {
		f, err := pdk.DashFrame(test.path)
		if err != test.err {
			t.Fatal(err)
			if f != test.expected {
				t.Fatalf("DotFrame failed, expected %s, but got %s", test.expected, f)
			}
		}
	}
}
