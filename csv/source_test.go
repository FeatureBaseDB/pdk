package csv_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pilosa/pdk/csv"
)

func MustGetTempFile(t *testing.T, content string) *os.File {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("getting temp file: %v", err)
	}
	n, err := f.WriteString(content)
	if err != nil || n != len(content) {
		t.Fatalf("writing temp file: %v, n: %v", err, n)
	}
	return f
}

func TestCSVSource(t *testing.T) {
	f := MustGetTempFile(t, `blah,bleh,blue
1,asdf,3
2,qwer,4
`)
	src := csv.NewSource(csv.WithURLs([]string{f.Name()}))
	rec, err := src.Record()
	if err != nil {
		t.Fatalf("getting first record: %v", err)
	}

	recmap := rec.(map[string]string)
	if len(recmap) != 4 {
		t.Fatalf("wrong length record: %v", rec)
	}
	if recmap["blah"] != "1" {
		t.Fatalf("blah")
	}
	if recmap["bleh"] != "asdf" {
		t.Fatalf("bleh")
	}
	if recmap["blue"] != "3" {
		t.Fatalf("blue")
	}

	rec, err = src.Record()
	if err != nil {
		t.Fatalf("getting first record: %v", err)
	}

	recmap = rec.(map[string]string)
	if len(recmap) != 4 {
		t.Fatalf("wrong length record: %v", rec)
	}
	if recmap["blah"] != "2" {
		t.Fatalf("blah2")
	}
	if recmap["bleh"] != "qwer" {
		t.Fatalf("blehqwer")
	}
	if recmap["blue"] != "4" {
		t.Fatalf("blue4")
	}
}
