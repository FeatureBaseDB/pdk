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
	"testing"

	"github.com/pilosa/pdk"
)

func TestCollapsingMapper(t *testing.T) {
	cm := pdk.NewCollapsingMapper()
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

	if len(pr.Rows) != 3 {
		t.Fatalf("wrong rows: %v", pr.Rows)
	}
	if len(pr.Vals) != 1 {
		t.Fatalf("wrong vals: %v", pr.Vals)
	}

}
