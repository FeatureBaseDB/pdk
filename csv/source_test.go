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
	src := csv.NewCSVSource([]string{f.Name()})
	rec, err := src.Record()
	if err != nil {
		t.Fatalf("getting first record: %v", err)
	}

	recmap := rec.(map[string]interface{})
	if len(recmap) != 4 {
		t.Fatalf("wrong length record: %v", rec)
	}
	if recmap["blah"] != "1" {
		t.Fail()
	}
	if recmap["bleh"] != "asdf" {
		t.Fail()
	}
	if recmap["blue"] != "3" {
		t.Fail()
	}

	rec, err = src.Record()
	if err != nil {
		t.Fatalf("getting first record: %v", err)
	}

	recmap = rec.(map[string]interface{})
	if len(recmap) != 4 {
		t.Fatalf("wrong length record: %v", rec)
	}
	if recmap["blah"] != "2" {
		t.Fail()
	}
	if recmap["bleh"] != "qwer" {
		t.Fail()
	}
	if recmap["blue"] != "4" {
		t.Fail()
	}
}
