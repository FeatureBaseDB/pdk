package file

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pilosa/pdk"
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

func TestRawSource(t *testing.T) {
	d := mustTempDir(t, "testrawsource")
	defer func() {
		os.RemoveAll(d)
	}()

	names := make([]string, 0, 2)
	names = append(names, filepath.Base(mustFile(t, d, `blah blah blah`)))
	names = append(names, filepath.Base(mustFile(t, d, `hahahahahahahaha`)))

	rs, err := NewRawSource(d)
	if err != nil {
		t.Fatalf("getting raw source: %v", err)
	}

	gotNames := make([]string, 0, 2)
	var reader pdk.NamedReadCloser
	for reader, err = rs.NextReader(); err == nil; reader, err = rs.NextReader() {
		gotNames = append(gotNames, reader.Name())
		buf, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Fatalf("reading file: %v", err)
		}
		t.Logf("%s\n", buf)
	}
	if !reflect.DeepEqual(gotNames, names) {
		names[0], names[1] = names[1], names[0]
		if !reflect.DeepEqual(gotNames, names) {
			t.Fatalf("different file names: %v", gotNames)
		}
	}
	if err != io.EOF {
		t.Fatalf("unexpected NextReader error: %v", err)
	}

}

func TestSource(t *testing.T) {
	d := mustTempDir(t, "testsource")
	defer func() {
		os.RemoveAll(d)
	}()

	mustFile(t, d, `
{"hey": 44}
{"hey": 39}
`)

	mustFile(t, d, `
{"hey": 81}
{"hey": 22}
`)

	s, err := NewSource(OptSrcSubjectAt("here"), OptSrcPath(d))
	if err != nil {
		t.Fatalf("getting source: %v", err)
	}

	vals := make(map[int]struct{})

	var rec interface{}
	for rec, err = s.Record(); err == nil; rec, err = s.Record() {
		recm, ok := rec.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{} but got %T", rec)
		}

		v, ok := recm["hey"]
		if !ok {
			t.Fatalf("key 'hey' not present in %v", recm)
		}

		if vi, ok := v.(float64); ok {
			vals[int(vi)] = struct{}{}
		} else {
			t.Fatalf("expected float")
		}
	}

	if len(vals) != 4 {
		t.Fatalf("wrong num of vals: %v", vals)
	}

	for _, v := range []int{44, 39, 81, 22} {
		if _, ok := vals[v]; !ok {
			t.Fatalf("didn't find %d in %v", v, vals)
		}
	}

}
