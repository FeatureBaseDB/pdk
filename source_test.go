// Copyright 2017-2019 Pilosa Corp.
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
	"errors"
	"testing"

	"github.com/pilosa/pdk"
)

type Source interface {
	Record() (interface{}, error)
}

type mockSource struct {
	items []interface{}
	index int
}

func (s *mockSource) Record() (interface{}, error) {
	var r interface{}
	if len(s.items) > 0 {
		r = s.items[0]
		s.items = append(s.items[:0], s.items[1:]...)
		return r, nil
	} else {
		return nil, errors.New("No more items")
	}
}

func TestMockSource(t *testing.T) {
	source := &mockSource{items: []interface{}{0, 1}}

	i, err := source.Record()
	if err != nil {
		t.Fatal("Record expected")
	}
	if i.(int) != 0 {
		t.Fatal("Record should be 0")
	}

	i, err = source.Record()
	if err != nil {
		t.Fatal("Record expected")
	}
	if i.(int) != 1 {
		t.Fatal("Record should be 1")
	}

	i, err = source.Record()
	if err == nil {
		t.Fatal("Error expected")
	}
}

func TestSourcePeek(t *testing.T) {
	ms := &mockSource{items: []interface{}{0, 1}}
	source := pdk.NewPeekingSource(ms)

	i, err := source.Peek()
	if err != nil {
		t.Fatal(err.Error())
	}
	if i.(int) != 0 {
		t.Fatal("Record should be 0")
	}

	i, err = source.Record()
	if err != nil {
		t.Fatal("Record expected")
	}
	if i.(int) != 0 {
		t.Fatal("Record should be 0")
	}

	i, err = source.Record()
	if err != nil {
		t.Fatal("Record expected")
	}
	if i.(int) != 1 {
		t.Fatal("Record should be 1")
	}

	i, err = source.Record()
	if err == nil {
		t.Fatal("Error expected")
	}
}
