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

func TestCollapsingMapper(t *testing.T) {
	tests := []struct {
		nilColTranslator bool
		nilTranslator    bool
		nilNexter        bool
		expCol           interface{}
		expRows          map[string]interface{}
	}{
		{
			false,
			false,
			false,
			uint64(0),
			map[string]interface{}{"aa": uint64(0)},
		},
		{
			true,
			false,
			false,
			uint64(0),
			map[string]interface{}{"aa": uint64(0)},
		},
		{
			true,
			false,
			true,
			"blah",
			map[string]interface{}{"aa": uint64(0)},
		},
		{
			true,
			true,
			true,
			"blah",
			map[string]interface{}{"aa": "hello"},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			cm := pdk.NewCollapsingMapper()
			if test.nilColTranslator {
				cm.ColTranslator = nil
			}
			if test.nilTranslator {
				cm.Translator = nil
			}
			if test.nilNexter {
				cm.Nexter = nil
			}

			e := &pdk.Entity{
				Subject: "blah",
				Objects: map[pdk.Property]pdk.Object{
					"aa":     pdk.S("hello"),
					"bb":     pdk.I(49),
					"active": pdk.B(true),
					"alive":  pdk.B(true),
				},
			}
			pr, err := cm.Map(e)
			if err != nil {
				t.Fatalf("mapping entity: %v", err)
			}

			if cm.Translator != nil {
				val, err := cm.Translator.Get("aa", 0)
				if err != nil {
					t.Fatalf("translator get: %v", err)
				}
				if val != pdk.S("hello") {
					t.Fatalf("bad val from translator")
				}

				idactive, err := cm.Translator.GetID("default", "active")
				if err != nil {
					t.Fatal(err)
				}
				idalive, err := cm.Translator.GetID("default", "alive")
				if err != nil {
					t.Fatal(err)
				}
				if !(idalive == 0 && idactive == 1 || idactive == 0 && idalive == 1) {
					t.Fatalf("mapping error, active: %v, alive: %v", idactive, idalive)
				}
			}

			if len(pr.Rows) != 3 {
				t.Fatalf("wrong rows: %v", pr.Rows)
			}
			if len(pr.Vals) != 1 {
				t.Fatalf("wrong vals: %v", pr.Vals)
			}

			// Ensure the field values are correct.
			for _, f := range pr.Rows {
				// For now we only look at the aa field. The map in test.expRows makes it hard to test rows
				// that get mapped to the same field, so that part of the test would need to be changed.
				if f.Field == "aa" {
					if f.ID != test.expRows[f.Field] {
						t.Fatalf("wrong row id, expected: %v, but got: %v", test.expRows[f.Field], f.ID)
					}
				}
			}

			// Ensure the column value is correct.
			if pr.Col != test.expCol {
				t.Fatalf("wrong col, expected: %v, but got: %v", test.expCol, pr.Col)
			}
		})
	}
}
