// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package csv_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
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

func TestWithTransientErrors(t *testing.T) {
	tests := []struct {
		content  *erroringOpenStringer
		expected map[string]map[string]int // counts for each header/value combination
		num      int                       // number of identical erroringOpenStringers to run (default 1)
		conc     int                       // concurrency for csv.Source default 1
	}{
		{
			content: &erroringOpenStringer{
				Name:    "file0",
				ErrSpec: []int{-1},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file1",
				ErrSpec: []int{2},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file2",
				ErrSpec: []int{-1, 2},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file3",
				ErrSpec: []int{2, -1},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file4",
				ErrSpec: []int{8, -1},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file5",
				ErrSpec: []int{7, -1},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file6",
				ErrSpec: []int{-1, 8},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file7",
				ErrSpec: []int{-1, 7},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file8",
				ErrSpec: []int{6},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file9",
				ErrSpec: []int{5},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file10",
				ErrSpec: []int{0, 9},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			expected: map[string]map[string]int{"a": {"1": 5}, "b": {"1": 5}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file11",
				ErrSpec: []int{-1},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			num:      2,
			conc:     1,
			expected: map[string]map[string]int{"a": {"1": 10}, "b": {"1": 10}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file12",
				ErrSpec: []int{2},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			num:      2,
			conc:     3,
			expected: map[string]map[string]int{"a": {"1": 10}, "b": {"1": 10}},
		},
		{
			content: &erroringOpenStringer{
				Name:    "file13",
				ErrSpec: []int{-1, 2},
				Content: SimpleContent(t, []string{"a", "b"}, "1", 5),
			},
			num:      2,
			conc:     4,
			expected: map[string]map[string]int{"a": {"1": 10}, "b": {"1": 10}},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d: %s", i, test.content.Name), func(t *testing.T) {
			if test.num == 0 {
				test.num = 1
			}
			openers := make([]csv.OpenStringer, test.num)
			for i := 0; i < test.num; i++ {
				openers[i] = &erroringOpenStringer{
					Name:    fmt.Sprintf("%s #%d", test.content.Name, i),
					ErrSpec: test.content.ErrSpec,
					Content: test.content.Content,
				}
			}
			src := csv.NewSource(csv.WithOpenStringers(openers), csv.WithConcurrency(test.conc))
			actual := make(map[string]map[string]int)
			var reci interface{}
			var err error
			for reci, err = src.Record(); err != io.EOF; reci, err = src.Record() {
				if err != nil {
					t.Fatalf("record err: %v", err)
				}
				rec := reci.(map[string]string)
				for head, val := range rec {
					if head == "," {
						continue
					}
					vals, ok := actual[head]
					if !ok {
						vals = make(map[string]int)
						actual[head] = vals
					}
					vals[val] += 1
				}
			}
			if err != nil && err != io.EOF {
				t.Fatalf("reading from src: %v", err)
			}
			if !reflect.DeepEqual(actual, test.expected) {
				t.Fatalf("unexpected results act: %#v, exp: %#v", actual, test.expected)
			}
		})
	}
}

func SimpleContent(t *testing.T, header []string, data string, lines int) string {
	buf := bytes.NewBuffer([]byte{})
	_, err := buf.WriteString(strings.Join(header, ",") + "\n")
	if err != nil {
		t.Fatalf("writing header: %v", err)
	}

	for i := 0; i < lines; i++ {
		for j := 0; j < len(header); j++ {
			if j == len(header)-1 {
				_, err = buf.WriteString(data + "\n")
			} else {
				_, err = buf.WriteString(data + ",")
			}
			if err != nil {
				t.Fatalf("writing data: %v", err)
			}
		}
	}
	return buf.String()
}

// erroringOpenStringer takes CSV content and an ErrSpec which specifies when
// errors should occur when Opening or Reading it.
type erroringOpenStringer struct {
	Name    string
	ErrSpec []int // each int specifies at which byte an error should occur. -1 means during Open() call
	Content string

	open    bool
	cpos    int
	specPos int
}

func (e *erroringOpenStringer) String() string {
	return e.Name
}

func (e *erroringOpenStringer) Open() (io.ReadCloser, error) {
	e.cpos = 0
	e.open = true
	if e.specPos < len(e.ErrSpec) && e.ErrSpec[e.specPos] == -1 {
		e.specPos++
		return nil, errors.New("couldn't open!")
	}

	return e, nil
}

func (e *erroringOpenStringer) Read(p []byte) (n int, err error) {
	if !e.open {
		return 0, errors.New("not open!")
	}
	var errPos int
	if e.specPos < len(e.ErrSpec) && e.ErrSpec[e.specPos] != -1 {
		errPos = e.ErrSpec[e.specPos]
		e.specPos++
		err = errors.New("couldn't read!")
	} else {
		errPos = len(e.Content)
		err = io.EOF
	}
	copy(p, e.Content[e.cpos:errPos])
	if e.cpos+len(p) > errPos {
		e.open = false
		return errPos - e.cpos, err
	}
	e.cpos += len(p)
	return len(p), nil
}

func (e *erroringOpenStringer) Close() error { return nil }
