package fake

import (
	"io"
	"testing"
)

func TestUserGenerator(t *testing.T) {
	us := NewUserSource(111, 1000)

	fnames := make(map[string]int)
	lnames := make(map[string]int)
	titles := make(map[string]int)
	allergyLens := make(map[int]int)

	for i := 0; i < 1000; i++ {
		r, err := us.Record()
		if err != nil {
			t.Fatalf("unexpected error getting record: %v", err)
		}
		rec := r.(*User)
		if rec.ID != uint64(i) {
			t.Errorf("ID exp: %d, got: %d", i, rec.ID)
		}
		if len(rec.FirstName) > 8 || len(rec.FirstName) < 1 {
			t.Errorf("FirstName got: %s", rec.FirstName)
		}
		fnames[rec.FirstName]++
		if len(rec.LastName) > 10 || len(rec.LastName) < 1 {
			t.Errorf("LastName got: %s", rec.LastName)
		}
		lnames[rec.LastName]++
		if rec.Age < 0 || rec.Age > 110 {
			t.Errorf("Age: %d", rec.Age)
		}
		if len(rec.Title) < 1 {
			t.Errorf("Title got: %s", rec.Title)
		}
		titles[rec.Title]++
		allergyLens[len(rec.Allergies)]++
		allergies := make(map[string]struct{})
		for _, aller := range rec.Allergies {
			if _, ok := allergies[aller]; ok {
				t.Errorf("got the same allergy twice: %s, record: %d", aller, rec.ID)
			}
			allergies[aller] = struct{}{}
		}
	}

	if len(titles) >= len(fnames) || len(fnames) >= len(lnames) {
		t.Errorf("unexpected distribution of strings: %d, %d, %d", len(titles), len(fnames), len(lnames))
	}

	if len(allergyLens) < 25 {
		t.Errorf("expected a wider variety of allergy lengths: %v", allergyLens)
	}

	if _, err := us.Record(); err != io.EOF {
		t.Fatalf("expected io.EOF, but got %v", err)
	}
}
