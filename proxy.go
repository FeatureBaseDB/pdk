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

package pdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

// KeyMapper describes the functionality for mapping the keys contained
// in requests and responses.
type KeyMapper interface {
	MapRequest(body []byte) ([]byte, error)
	MapResult(frame string, res interface{}) (interface{}, error)
}

// Proxy describes the functionality for proxying requests.
type Proxy interface {
	ProxyRequest(orig *http.Request, origbody []byte) (*http.Response, error)
}

// StartMappingProxy listens for incoming http connections on `bind` and
// and uses h to handle all requests.
// This function does not return unless there is a problem (like
// http.ListenAndServe).
func StartMappingProxy(bind string, h http.Handler) error {
	s := http.Server{
		Addr:    bind,
		Handler: h,
	}
	return s.ListenAndServe()
}

type pilosaForwarder struct {
	phost     string
	client    http.Client
	km        KeyMapper
	colMapper FrameTranslator
	proxy     Proxy
}

// NewPilosaForwarder returns a new pilosaForwarder which forwards all requests
// to `phost`. It inspects pilosa responses and runs the row ids through the
// Translator `t` to translate them to whatever they were mapped from.
func NewPilosaForwarder(phost string, t Translator, colTranslator ...FrameTranslator) *pilosaForwarder {
	if !strings.HasPrefix(phost, "http://") {
		phost = "http://" + phost
	}
	f := &pilosaForwarder{
		phost: phost,
		km:    NewPilosaKeyMapper(t, colTranslator...),
	}
	f.proxy = NewPilosaProxy(phost, &f.client)
	return f
}

func (p *pilosaForwarder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// inspect the request to determine which queries have a frame - the Translator
	// needs the frame for it's lookups.
	frames, err := GetFrames(body)
	if err != nil {
		http.Error(w, "getting frames: "+err.Error(), http.StatusBadRequest)
		return
	}

	body, err = p.km.MapRequest(body)
	if err != nil {
		http.Error(w, "mapping request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// forward the request and get the pilosa response
	resp, err := p.proxy.ProxyRequest(req, body)
	if err != nil {
		log.Println("here", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// decode pilosa response for inspection
	dec := json.NewDecoder(resp.Body)
	pilosaResp := &pilosa.QueryResponse{}
	err = dec.Decode(pilosaResp)
	if err != nil {
		log.Printf("decoding json: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// for each query result, try to map it
	mappedResp := &pilosa.QueryResponse{
		Results: make([]interface{}, len(pilosaResp.Results)),
	}
	for i, result := range pilosaResp.Results {
		if frames[i] == "" {
			mappedResp.Results[i] = result
			continue
		}
		mappedResult, err := p.km.MapResult(frames[i], result)
		if err != nil {
			http.Error(w, "mapping result: "+err.Error(), http.StatusInternalServerError)
			return
		}
		mappedResp.Results[i] = mappedResult
	}

	// Allow cross-domain requests
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// write the mapped response back to the client
	enc := json.NewEncoder(w)
	err = enc.Encode(mappedResp)
	if err != nil {
		log.Println(err)
		http.Error(w, "encoding newresp: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// pilosaProxy implements the Proxy interface.
type pilosaProxy struct {
	host   string
	client *http.Client
}

// NewPilosaProxy returns a pilosaProxy based on `host` and `client`.
func NewPilosaProxy(host string, client *http.Client) *pilosaProxy {
	return &pilosaProxy{
		host:   host,
		client: client,
	}
}

// proxyRequest modifies the http.Request object in place to change it from a
// server side request object to the proxy server to a client side request and
// sends it to pilosa, returning the response.
func (p *pilosaProxy) ProxyRequest(orig *http.Request, origbody []byte) (*http.Response, error) {
	reqURL, err := url.Parse(p.host + orig.URL.String())
	if err != nil {
		log.Printf("error parsing url: %v, err: %v", p.host+orig.URL.String(), err)
		return nil, errors.Wrapf(err, "parsing url: %v", p.host+orig.URL.String())
	}
	orig.URL = reqURL
	orig.Host = ""
	orig.RequestURI = ""
	orig.Body = ioutil.NopCloser(bytes.NewBuffer(origbody))
	orig.ContentLength = int64(len(origbody))
	resp, err := p.client.Do(orig)
	return resp, err
}

// PilosaKeyMapper implements the KeyMapper interface.
type PilosaKeyMapper struct {
	t Translator
	c FrameTranslator
}

// NewPilosaKeyMapper returns a PilosaKeyMapper.
func NewPilosaKeyMapper(t Translator, colTranslator ...FrameTranslator) *PilosaKeyMapper {
	pkm := &PilosaKeyMapper{
		t: t,
	}
	if len(colTranslator) > 0 {
		pkm.c = colTranslator[0]
	}
	return pkm
}

// MapResult converts the result of a single top level query (one element of
// QueryResponse.Results) to its mapped counterpart.
func (p *PilosaKeyMapper) MapResult(frame string, res interface{}) (mappedRes interface{}, err error) {
	log.Printf("mapping result: '%#v'", res)
	defer func() {
		log.Printf("mapped result: '%#v'", mappedRes)
	}()
	switch result := res.(type) {
	case uint64:
		// Count
		mappedRes = result
	case []interface{}:
		return p.mapSliceInterfaceResult(frame, result)
	case map[string]interface{}:
		// Bitmap/Intersect/Difference/Union
		return p.mapBitmapResult(frame, result)
	case bool:
		// SetBit/ClearBit
		mappedRes = result
	default:
		// Range? SetRowAttrs?
		mappedRes = result
	}
	return mappedRes, nil
}

func (p *PilosaKeyMapper) mapBitmapResult(frame string, result map[string]interface{}) (mappedRes interface{}, err error) {
	cols, ok := result["columns"]
	if !ok {
		return result, errors.Errorf("columns key not in result: %#v", result)
	}
	colsSlice, ok := cols.([]interface{})
	if !ok {
		return result, errors.Errorf("columns should be a slice but is %T, %#v", cols, cols)
	}
	mappedCols, err := p.mapColumnSlice(frame, colsSlice)
	if err != nil {
		return result, errors.Wrap(err, "mapping column slice")
	}
	result["columns"] = mappedCols
	return result, nil
}

func (p *PilosaKeyMapper) mapSliceInterfaceResult(frame string, res []interface{}) (mappedRes interface{}, err error) {
	if len(res) == 0 {
		return res, nil
	}
	switch res[0].(type) {
	case map[string]interface{}:
		return p.mapTopNResult(frame, res)
	default:
		return mappedRes, errors.Errorf("unexpected result type in slice: %T, %#v", res[0], res[0])
	}
}

func (p *PilosaKeyMapper) mapColumnSlice(frame string, result []interface{}) (mappedRes interface{}, err error) {
	cols := make([]interface{}, len(result))
	for i, icol := range result {
		col, ok := icol.(float64)
		if !ok {
			return nil, errors.Errorf("expected float64, but got %T %#v", icol, icol)
		}
		colV, err := p.c.Get(uint64(col))
		if err != nil {
			return nil, errors.Wrap(err, "translating column id to value")
		}
		cols[i] = colV
	}
	return cols, nil
}

func (p *PilosaKeyMapper) mapTopNResult(frame string, result []interface{}) (mappedRes interface{}, err error) {
	mr := make([]struct {
		Key   interface{}
		Count uint64
	}, len(result))
	for i, intpair := range result {
		if pair, ok := intpair.(map[string]interface{}); ok {
			pairkey, gotKey := pair["id"]
			paircount, gotCount := pair["count"]
			if !(gotKey && gotCount) {
				return nil, fmt.Errorf("expected pilosa.Pair, but have wrong keys: got %v", pair)
			}
			keyFloat, isKeyFloat := pairkey.(float64)
			countFloat, isCountFloat := paircount.(float64)
			if !(isKeyFloat && isCountFloat) {
				return nil, fmt.Errorf("expected pilosa.Pair, but have wrong value types: got %v", pair)
			}
			keyVal, err := p.t.Get(frame, uint64(keyFloat))
			if err != nil {
				return nil, errors.Wrap(err, "translator.Get")
			}

			switch kv := keyVal.(type) {
			case []byte:
				mr[i].Key = string(kv)
			default:
				mr[i].Key = keyVal
			}
			mr[i].Count = uint64(countFloat)
		} else {
			return nil, fmt.Errorf("unknown type in inner slice: %v", intpair)
		}
	}
	mappedRes = mr
	return mappedRes, nil
}

// MapRequest takes a request body and returns a mapped version of that body.
func (p *PilosaKeyMapper) MapRequest(body []byte) ([]byte, error) {
	log.Printf("mapping request: '%s'", body)
	query, err := pql.ParseString(string(body))
	if err != nil {
		return nil, errors.Wrap(err, "parsing string")
	}
	for _, call := range query.Calls {
		err := p.mapCall(call)
		if err != nil {
			return nil, errors.Wrap(err, "mapping call")
		}
	}
	log.Printf("mapped request: '%s'", query.String())
	return []byte(query.String()), nil
}

func (p *PilosaKeyMapper) mapCall(call *pql.Call) error {
	if call.Name == "Bitmap" {
		id, err := p.t.GetID(call.Args["frame"].(string), call.Args["row"])
		if err != nil {
			return errors.Wrap(err, "getting ID")
		}
		call.Args["row"] = id
		return nil
	}
	for _, child := range call.Children {
		if err := p.mapCall(child); err != nil {
			return errors.Wrap(err, "mapping call")
		}
	}
	return nil
}

// GetFrames interprets body as pql queries and then tries to determine the
// frame of each. Some queries do not have frames, and the empty string will be
// returned for these.
func GetFrames(body []byte) ([]string, error) {
	query, err := pql.ParseString(string(body))
	if err != nil {
		return nil, fmt.Errorf("parsing query: %v", err.Error())
	}

	frames := make([]string, len(query.Calls))

	for i, call := range query.Calls {
		if frame, ok := call.Args["frame"].(string); ok {
			frames[i] = frame
		} else {
			frames[i] = ""
		}
	}
	return frames, nil
}
