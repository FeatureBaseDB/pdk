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

package geohash_test

import (
	"testing"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/geohash"
	"github.com/pkg/errors"
)

func TestTransform(t *testing.T) {
	tests := []struct {
		name        string
		transformer *geohash.Transformer
		entity      *pdk.Entity
		expErr      error
	}{
		{
			name: "simple",
			transformer: &geohash.Transformer{
				Precision:  6,
				LatPath:    []string{"latitude"},
				LonPath:    []string{"longitude"},
				ResultPath: []string{"geohash"},
			},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{
					"latitude":  pdk.F64(31.1),
					"longitude": pdk.F64(42.2),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.transformer.Transform(test.entity)
			if errors.Cause(err) != test.expErr {
				t.Fatalf("got %v, expected %v", err, test.expErr)
			}
			if err != nil {
				return
			}
			hash, err := test.entity.Literal(test.transformer.ResultPath...)
			if err != nil {
				t.Fatalf("should be a value at ResultPath, but: %v", err)
			}

			hashS, ok := hash.(pdk.S)
			if !ok {
				t.Fatalf("hash is not a string")
			}
			if len(hashS) != test.transformer.Precision {
				t.Fatalf("unexpected length of hashVal %v", hashS)
			}
		})
	}
}
