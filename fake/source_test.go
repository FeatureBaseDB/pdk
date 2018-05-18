package fake

import (
	"io"
	"testing"
)

func TestSource(t *testing.T) {
	src := NewSource(1, 2, 10000)

	for i := 0; i < 10000; i++ {
		ev, err := src.Record()
		if err != nil {
			t.Fatalf("unexpected error on rec %d: %v", i, err)
		}
		if ev == nil {
			t.Fatalf("unexpected nil event")
		}
	}
	ev, err := src.Record()
	if err != io.EOF {
		t.Fatalf("should get EOF after 10k records, but %v", err)
	}
	if ev != nil {
		t.Fatalf("should have nil event after 10k records, but got %v", ev)
	}

}
