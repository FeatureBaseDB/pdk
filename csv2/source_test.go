package csv2

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/pilosa/pdk/file"
)

func mustTempDir(t *testing.T, prefix string) string {
	t.Helper()
	d, err := ioutil.TempDir("", prefix)
	if err != nil {
		t.Fatal("getting temp dir")
	}
	return d
}

func mustFile(t *testing.T, dir, contents string) (name string) {
	t.Helper()
	f, err := ioutil.TempFile(dir, "")
	if err != nil {
		t.Fatalf("getting temp file: %v", err)
	}

	_, err = io.WriteString(f, contents)
	if err != nil {
		t.Fatalf("writing contents: %v", err)
	}

	return f.Name()
}

func TestSource(t *testing.T) {
	d := mustTempDir(t, "testcsvsource")

	mustFile(t, d, `lah,hah,zlah
1,2,sbldak
4,8,kfue`)
	mustFile(t, d, `lah,hah,zlah
11,12,hi
9,10,by`)

	rs, err := file.NewRawSource(d)
	if err != nil {
		t.Fatalf("getting raw source: %v", err)
	}

	s := NewSourceFromRawSource(rs)

	rec, err := s.Record()
	for ; err != io.EOF; rec, err = s.Record() {
		reci := rec.(map[string]string)
		for _, key := range []string{"hah", "lah", "zlah"} {
			if _, ok := reci[key]; !ok {
				t.Fatalf("key %s not found", key)
			}
		}
	}
}
