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

package http

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// JSONSource implements the pdk.Source interface by listening for HTTP post
// requests and decoding json from their bodies.
type JSONSource struct {
	addr     string
	listener net.Listener
	server   *http.Server
	records  chan record
}

// WithAddr is an option for the JSONSource which causes it to bind to the given
// address.
func WithAddr(addr string) JSONSourceOption {
	return func(j *JSONSource) {
		j.addr = addr
	}
}

// WithListener is an option for JSONSource which causes it to use the given
// listener. It will infer the address from the listener.
func WithListener(l net.Listener) JSONSourceOption {
	return func(j *JSONSource) {
		j.listener = l
		j.addr = l.Addr().String()
	}
}

// WithBuffer is an option for JSONSource which modifies the length of the
// channel used to buffer received records (while they are waiting to be
// retrieved by a call to Record).
func WithBuffer(n int) JSONSourceOption {
	return func(j *JSONSource) {
		if n > -1 {
			j.records = make(chan record, n)
		}
	}
}

// JSONSourceOption is a functional option type for JSONSource.
type JSONSourceOption func(j *JSONSource)

// NewJSONSource creates a JSONSource - it takes JSONSourceOptions which modify
// its behavior.
func NewJSONSource(opts ...JSONSourceOption) (*JSONSource, error) {
	j := &JSONSource{
		records: make(chan record, 3),
	}
	for _, opt := range opts {
		opt(j)
	}

	if j.listener == nil {
		var err error
		j.listener, err = net.Listen("tcp", j.addr)
		if err != nil {
			return nil, err
		}
	}
	j.listener = tcpKeepAliveListener{j.listener.(*net.TCPListener)}

	j.server = &http.Server{
		Addr:    j.addr,
		Handler: j,
	}
	go func() {
		err := j.server.Serve(j.listener)
		if err != nil {
			j.records <- record{err: errors.Wrap(err, "starting server")}
			close(j.records)
		}
	}()
	return j, nil
}

// Addr gets the address that the JSONSource is listening on.
func (j *JSONSource) Addr() string {
	if j.listener != nil {
		return j.listener.Addr().String()
	}
	return j.addr
}

type record struct {
	data interface{}
	err  error
}

// Record returns an unmarshaled json document as a map[string]interface. That
// is, the resulting interface{} can be cast to a map[string]interface{}.
func (j *JSONSource) Record() (interface{}, error) {
	rec, ok := <-j.records
	if !ok {
		return nil, io.EOF
	}
	return rec.data, rec.err
}

// ServeHTTP implements http.Handler for JSONSource
func (j *JSONSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		err := errors.Errorf("unsupported method: %v, request: %#v", r.Method, r)
		log.Println(err)
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	dec := json.NewDecoder(r.Body)
	for {
		stuff := make(map[string]interface{})
		err := dec.Decode(&stuff)
		if err == io.EOF {
			return
		}
		if err != nil {
			err := errors.Wrap(err, "decoding json")
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		j.records <- record{data: stuff}
	}
}

// tcpKeepAliveListener is copied from net/http

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
