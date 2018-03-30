package gen_test

import (
	"testing"
	"time"

	"github.com/pilosa/pdk/fake/gen"
)

func TestTime(t *testing.T) {
	start := time.Date(2018, 01, 01, 0, 0, 0, 0, time.Local)
	last := start
	for i := 0; i < 1000; i++ {
		tim := gen.Time(start, time.Second)
		if tim.Before(last) {
			t.Fatalf("generated a time before the last time")
		}
		if tim.Sub(last) > time.Second {
			t.Fatalf("generated a time more than a second after the last one")
		}
		last = tim
	}
}
