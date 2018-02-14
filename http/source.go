package http

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type JSONSource struct {
	addr     string
	listener net.Listener
	server   *http.Server
	records  chan record
}

func WithAddr(addr string) JSONSourceOption {
	return func(j *JSONSource) {
		j.addr = addr
	}
}

func WithListener(l net.Listener) JSONSourceOption {
	return func(j *JSONSource) {
		j.listener = l
		j.addr = l.Addr().String()
	}
}

func WithBuffer(n int) JSONSourceOption {
	return func(j *JSONSource) {
		if n > -1 {
			j.records = make(chan record, n)
		}
	}
}

type JSONSourceOption func(j *JSONSource)

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

type record struct {
	data interface{}
	err  error
}

func (j *JSONSource) Record() (interface{}, error) {
	rec, ok := <-j.records
	if !ok {
		return nil, io.EOF
	}
	return rec.data, rec.err
}

func (j *JSONSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		j.records <- record{err: errors.Errorf("unsupported method: %v, request: %#v", r.Method, r)}
		return
	}
	dec := json.NewDecoder(r.Body)
	stuff := make(map[string]interface{})
	err := dec.Decode(&stuff)
	if err != nil {
		j.records <- record{err: errors.Wrap(err, "decoding json")}
		return
	}
	j.records <- record{data: stuff}
}

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
