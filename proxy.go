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
)

// Translator describes the functionality which the proxy server requires to
// translate row ids to what they were mapped from.
type Translator interface {
	// Get must be safe for concurrent access
	Get(frame string, id uint64) interface{}
	GetID(frame string, val interface{}) (uint64, error)
}

// StartMappingProxy listens for incoming http connections on `bind` and
// forwards all requests to `pilosa`. It inspects pilosa responses and runs the
// row ids through the Translator `m` to translate them to whatever they were
// mapped from. This function does not return unless there is a problem (like
// http.ListenAndServe).
func StartMappingProxy(bind, pilosa string, m Translator) error {
	if !strings.HasPrefix(pilosa, "http://") {
		pilosa = "http://" + pilosa
	}
	handler := &pilosaForwarder{phost: pilosa, m: m}
	s := http.Server{
		Addr:    bind,
		Handler: handler,
	}
	return s.ListenAndServe()
}

type pilosaForwarder struct {
	phost  string
	client http.Client
	m      Translator
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
	frames, err := getFrames(body)
	if err != nil {
		http.Error(w, "getting frames: "+err.Error(), http.StatusBadRequest)
		return
	}

	body, err = p.mapRequest(body)
	if err != nil {
		http.Error(w, "mapping request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// forward the request and get the pilosa response
	resp, err := p.proxyRequest(req, body)
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
		mappedResult, err := p.mapResult(frames[i], result)
		if err != nil {
			http.Error(w, "mapping result: "+err.Error(), http.StatusInternalServerError)
			return
		}
		mappedResp.Results[i] = mappedResult
	}

	// write the mapped response back to the client
	enc := json.NewEncoder(w)
	err = enc.Encode(mappedResp)
	if err != nil {
		log.Println(err)
		http.Error(w, "encoding newresp: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// proxyRequest modifies the http.Request object in place to change it from a
// server side request object to the proxy server to a client side request and
// sends it to pilosa, returning the response.
func (p *pilosaForwarder) proxyRequest(orig *http.Request, origbody []byte) (*http.Response, error) {
	reqURL, err := url.Parse(p.phost + orig.URL.String())
	if err != nil {
		log.Printf("error parsing url: %v, err: %v", p.phost+orig.URL.String(), err)
		return nil, err
	}
	orig.URL = reqURL
	orig.Host = ""
	orig.RequestURI = ""
	orig.Body = ioutil.NopCloser(bytes.NewBuffer(origbody))
	orig.ContentLength = int64(len(origbody))
	resp, err := p.client.Do(orig)
	return resp, err
}

// mapResult converts the result of a single top level query (one element of
// QueryResponse.Results) to it's mapped counterpart.
func (p *pilosaForwarder) mapResult(frame string, res interface{}) (mappedRes interface{}, err error) {
	switch result := res.(type) {
	case uint64:
		// Count
		mappedRes = result
	case []interface{}:
		// TopN
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
				mr[i].Key = p.m.Get(frame, uint64(keyFloat))
				mr[i].Count = uint64(countFloat)
			} else {
				return nil, fmt.Errorf("unknown type in inner slice: %v", intpair)
			}
		}
		mappedRes = mr
	case map[string]interface{}:
		// Bitmap/Intersect/Difference/Union
		mappedRes = result
	case bool:
		// SetBit/ClearBit
		mappedRes = result
	default:
		// Range? SetRowAttrs?
		mappedRes = result
	}
	return mappedRes, nil
}

func (p *pilosaForwarder) mapRequest(body []byte) ([]byte, error) {
	query, err := pql.ParseString(string(body))
	if err != nil {
		return nil, err
	}
	for _, call := range query.Calls {
		err := p.mapCall(call)
		if err != nil {
			return nil, err
		}
	}
	return []byte(query.String()), nil
}

func (p *pilosaForwarder) mapCall(call *pql.Call) error {
	if call.Name == "Bitmap" {
		id, err := p.m.GetID(call.Args["frame"].(string), call.Args["rowID"])
		if err != nil {
			return err
		}
		call.Args["rowID"] = id
		return nil
	}
	for _, child := range call.Children {
		if err := p.mapCall(child); err != nil {
			return err
		}
	}
	return nil
}

// getFrames interprets body as pql queries and then tries to determine the
// frame of each. Some queries do not have frames, and the empty string will be
// returned for these.
func getFrames(body []byte) ([]string, error) {
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
