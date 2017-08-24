package pdk

import (
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestFileFragment(t *testing.T) {
	tests := []struct {
		data  string
		start int64
		end   int64
		exp   string
	}{
		{
			data:  "abcdefghijklmnopqrstuvwxyz",
			start: 0,
			end:   3,
			exp:   "abc",
		},
		{
			data:  "abcdefghijklmnopqrstuvwxyz",
			start: 1,
			end:   3,
			exp:   "bc",
		},
		{
			data:  "abcdefghijklmnopqrstuvwxyz",
			start: 22,
			end:   26,
			exp:   "wxyz",
		},
		{
			data:  "abcdefghijklmnopqrstuvwxyz",
			start: 0,
			end:   26,
			exp:   "abcdefghijklmnopqrstuvwxyz",
		},
	}

	for i, test := range tests {
		f := mustWriteAndOpenFile(t, []byte(test.data))
		ff, err := NewFileFragment(f, test.start, test.end)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := ioutil.ReadAll(ff)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != test.exp {
			t.Fatalf("test %d: expected '%s', but got '%s'", i, test.exp, actual)
		}
	}
}

func TestSearchReader(t *testing.T) {
	tests := []struct {
		data   string
		start  int64
		expIdx int64
		expErr error
	}{
		{
			data:   "abcd\nefgh",
			start:  0,
			expIdx: 5,
			expErr: nil,
		},
		{
			data:   "abcd\nefgh",
			start:  5,
			expIdx: 4,
			expErr: io.EOF,
		},
		{
			data:   string3000(),
			start:  1,
			expIdx: 2500,
			expErr: nil,
		},
	}

	for i, test := range tests {
		f := mustWriteAndOpenFile(t, []byte(test.data))
		_, err := f.Seek(test.start, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		actIdx, actErr := searchReader(f, '\n')
		if actIdx != test.expIdx || actErr != test.expErr {
			t.Fatalf("test %d: expected idx: %d, and err: %v, but got idx: %d and err: %v", i, test.expIdx, test.expErr, actIdx, actErr)
		}
	}
}

func TestSeekAndSearch(t *testing.T) {
	tests := []struct {
		data      string
		splitSize int64
		expOff    int64
		expErr    error
	}{
		{
			data:      "abcd\nefgh",
			splitSize: 5,
			expOff:    9,
			expErr:    io.EOF,
		},
	}

	for i, test := range tests {
		f := mustWriteAndOpenFile(t, []byte(test.data))
		actOff, actErr := seekAndSearch(f, test.splitSize, '\n')
		if actOff != test.expOff || actErr != test.expErr {
			t.Fatalf("test %d: expected idx: %d, and err: %v, but got idx: %d and err: %v", i, test.expOff, test.expErr, actOff, actErr)
		}
	}
}

func TestSplitFileLines(t *testing.T) {
	tests := []struct {
		data     string
		numParts int64
		exp      []string
	}{
		{
			data:     "abcdef\ngh",
			numParts: 2,
			exp:      []string{"abcdef\n", "gh"},
		},
		{
			data:     "aaaa\nbbbb\ncccc\ndd\n",
			numParts: 4,
			exp:      []string{"aaaa\n", "bbbb\n", "cccc\n", "dd\n"},
		},
	}

	for i, test := range tests {
		f := mustWriteAndOpenFile(t, []byte(test.data))
		frags, err := SplitFileLines(f, test.numParts)
		if err != nil {
			t.Fatal(err)
		}
		var actual []string
		for _, frag := range frags {
			bytes, err := ioutil.ReadAll(frag)
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, string(bytes))
		}
		if !reflect.DeepEqual(actual, test.exp) {
			t.Fatalf("test %d: expected: %#v, but got %#v", i, test.exp, actual)
		}
	}
}

func string3000() string {
	var ret string
	for i := 0; i < 300; i++ {
		ret = ret + "1234567890"
		if i == 249 {
			ret += "\n"
		}
	}
	return ret
}

func mustWriteAndOpenFile(t *testing.T, data []byte) *os.File {
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}

	n, err := tf.Write(data)
	if n < len(data) {
		t.Fatal("Didn't write the whole data")
	}
	if err != nil {
		t.Fatal(err)
	}

	_, err = tf.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	return tf
}
