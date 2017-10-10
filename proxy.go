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
	phost  string
	client http.Client
	km     KeyMapper
	proxy  Proxy
}

// NewPilosaForwarder returns a new pilosaForwarder which forwards all requests
// to `phost`. It inspects pilosa responses and runs the row ids through the
// Translator `t` to translate them to whatever they were mapped from.
func NewPilosaForwarder(phost string, t Translator) *pilosaForwarder {
	if !strings.HasPrefix(phost, "http://") {
		phost = "http://" + phost
	}
	f := &pilosaForwarder{
		phost: phost,
		km:    NewPilosaKeyMapper(t),
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

// PilosaKeyMapper implements the KeyMapper interface.
type PilosaKeyMapper struct {
	t Translator
}

func NewPilosaKeyMapper(t Translator) *PilosaKeyMapper {
	return &PilosaKeyMapper{
		t: t,
	}
}

// MapResult converts the result of a single top level query (one element of
// QueryResponse.Results) to its mapped counterpart.
func (p *PilosaKeyMapper) MapResult(frame string, res interface{}) (mappedRes interface{}, err error) {
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
				keyVal := p.t.Get(frame, uint64(keyFloat))
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

func (p *PilosaKeyMapper) MapRequest(body []byte) ([]byte, error) {
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

func (p *PilosaKeyMapper) mapCall(call *pql.Call) error {
	if call.Name == "Bitmap" {
		id, err := p.t.GetID(call.Args["frame"].(string), call.Args["rowID"])
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
