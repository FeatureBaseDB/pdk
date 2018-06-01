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

package http_test

import (
	"fmt"
	"net"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/pilosa/pdk/http"
)

func TestJSONSource(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	j, err := http.NewJSONSource(http.WithListener(ln))
	if err != nil {
		t.Fatalf("getting json source: %v", err)
	}

	tests := []struct {
		method string
		path   string
		data   string
		exp    []map[string]interface{}
		expErr string
	}{
		{
			method: "POST",
			path:   "/",
			data:   `{"hello": 2}`,
			exp:    []map[string]interface{}{{"hello": 2.0}},
			expErr: "",
		},
		{
			method: "POST",
			path:   "/blah",
			data:   `{"hello": 2}`,
			exp:    []map[string]interface{}{{"hello": 2.0}},
			expErr: "",
		},
		{
			method: "POST",
			path:   "/blah",
			data:   `{"hello": 2}{"goodbye": 3}`,
			exp:    []map[string]interface{}{{"hello": 2.0}, {"goodbye": 3.0}},
			expErr: "",
		},
		{
			method: "POST",
			path:   "/blah",
			data: `{"hello": 2}
{"goodbye": 3}`,
			exp:    []map[string]interface{}{{"hello": 2.0}, {"goodbye": 3.0}},
			expErr: "",
		},
		{
			method: "POST",
			path:   "/blah",
			data: `{"hello": 2}  
  {"goodbye": 3}`,
			exp:    []map[string]interface{}{{"hello": 2.0}, {"goodbye": 3.0}},
			expErr: "",
		},
		// // TODO we now ignore errors in the JSONSource, so these test are
		// // commented. need to decide on an actual strategy for error handling
		// // and reporting.
		// {
		// 	method: "POST",
		// 	path:   "/",
		// 	data:   `{"hello: 2}`,
		// 	exp:    nil,
		// 	expErr: "decoding json",
		// },
		// {
		// 	method: "GET",
		// 	path:   "/",
		// 	data:   ``,
		// 	exp:    nil,
		// 	expErr: "unsupported method",
		// },
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			j.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(test.method, test.path, strings.NewReader(test.data)))
			for _, exp := range test.exp {
				data, err := j.Record()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				} else if !reflect.DeepEqual(data, exp) {
					t.Fatalf("unexpected data: %#v, exp: %#v", data, exp)
				}
			}
		})
	}

}
