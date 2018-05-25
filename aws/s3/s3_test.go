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

package s3

import (
	"io"
	"testing"
)

func TestNewSource(t *testing.T) {
	src, err := NewSource(
		OptSrcBucket("pdk-test-bucket"),
		OptSrcRegion("us-east-1"),
		OptSrcBufSize(9191),
		OptSrcSubjectAt("#@!pdksubj"),
	)
	if err != nil {
		t.Fatalf("getting new source: %v", err)
	}

	if src.bucket != "pdk-test-bucket" {
		t.Fatalf("wrong bucket name: %s", src.bucket)
	}
	if src.region != "us-east-1" {
		t.Fatalf("wrong region name: %s", src.region)
	}
	if cap(src.records) != 9191 {
		t.Fatalf("wrong chan bufsize: %d", cap(src.records))
	}

	expSubjects := map[string]struct{}{
		"pdk-test-bucket.myfile1#0": struct{}{},
		"pdk-test-bucket.myfile1#1": struct{}{},
		"pdk-test-bucket.myfile1#2": struct{}{},
		"pdk-test-bucket.myfile2#0": struct{}{},
		"pdk-test-bucket.myfile2#1": struct{}{},
		"pdk-test-bucket.myfile2#2": struct{}{},
	}

	recs := make([]map[string]interface{}, 0)
	for rec, err := src.Record(); err != io.EOF; rec, err = src.Record() {
		recmap := rec.(map[string]interface{})
		subj, ok := recmap["#@!pdksubj"].(string)
		if !ok {
			t.Fatalf("subject wrong in %#v", recmap)
		}
		delete(expSubjects, subj)
		if err != nil {
			t.Fatalf("calling src.Record: %v", err)
		}
		recs = append(recs, recmap)
	}

	if len(expSubjects) > 0 {
		t.Fatalf("not all expected subjects were found, leftovers: %#v", expSubjects)
	}

	if len(recs) != 6 {
		for i, rec := range recs {
			t.Logf("%d: %#v\n", i, rec)
		}
		t.Fatal("wrong number of records")
	}
	widg := recs[0]["widget"].(map[string]interface{})
	if _, ok := widg["window"]; !ok {
		t.Fatalf("unexpected value does not have widget.window: %#v", recs[0])
	}
}
