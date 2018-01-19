package pdk_test

import (
	"testing"

	"github.com/pilosa/pdk"
)

func TestNexter(t *testing.T) {
	n := pdk.NewNexter(pdk.NexterStartFrom(19))
	if num := n.Next(); num != 19 {
		t.Fatalf("expected 19 for Next, but %d", num)
	}
	if num := n.Last(); num != 19 {
		t.Fatalf("expected 19 for Last, but %d", num)
	}
}
