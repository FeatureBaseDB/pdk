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

package pdk_test

import (
	"fmt"
	"testing"

	"github.com/pilosa/pdk"
)

func TestDashFrame(t *testing.T) {
	tests := []struct {
		path     []string
		ignore   []string
		collapse []string
		expFrame string
		expField string
		err      error
	}{
		{
			path:     []string{"hello", "g", "a", "b"},
			expFrame: "hello-g-a-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello"},
			expFrame: "hello",
			expField: "hello",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"g"},
			expFrame: "",
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a"},
			expFrame: "hello-g-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"a", "g"},
			expFrame: "hello-b",
			expField: "b",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b"},
			expFrame: "hello-g-a",
			expField: "a",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			collapse: []string{"b", "hello", "g", "a"},
			expFrame: "",
			expField: "",
			err:      nil,
		},
		{
			path:     []string{"hello", "g", "a", "b"},
			ignore:   []string{"z", "helloz"},
			expFrame: "hello-g-a-b",
			expField: "b",
			err:      nil,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			df := &pdk.DashFrame{Ignore: test.ignore, Collapse: test.collapse}
			f, err := df.Frame(test.path)
			if err != test.err {
				t.Fatal(err)
			}
			if f != test.expFrame {
				t.Fatalf("unexpected frame: %v", f)
			}

			_, field, err := df.Field(test.path)
			if err != test.err {
				t.Fatalf("getting field: %v", err)
			}
			if field != test.expField {
				t.Fatalf("unexpected field: %v", field)
			}
		})
	}
}
