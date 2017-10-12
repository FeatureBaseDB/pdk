package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

type AvroParser struct {
	RegistryURL string

	lock  sync.RWMutex
	cache map[int32]*goavro.Codec
}

func NewAvroParserRegistry(registryURL string) *AvroParser {
	return &AvroParser{
		RegistryURL: registryURL,
		cache:       make(map[int32]*goavro.Codec),
	}
}

func (p *AvroParser) Parse(record []byte) (interface{}, error) {
	kr, err := Decode(record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't decode kafka record")
	}

	if kr.Value[0] != 0 {
		return nil, errors.Errorf("unexpected magic byte in avro kafka value, should be 0, but got %d", kr.Value[0])
	}
	id := int32(binary.BigEndian.Uint32(kr.Value[1:]))
	codec, err := p.GetCodec(id)
	if err != nil {
		return nil, errors.Wrap(err, "getting avro codec")
	}
	val, _, err := codec.NativeFromBinary(append([]byte{0}, kr.Value[5:]...))
	return val, errors.Wrap(err, "decoding avro record")
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	Id      int    `json:"id"`      // Registry's unique id
}

func (p *AvroParser) GetCodec(id int32) (*goavro.Codec, error) {
	p.lock.RLock()
	if codec, ok := p.cache[id]; ok {
		p.lock.RUnlock()
		return codec, nil
	}
	p.lock.RUnlock()
	p.lock.Lock()
	defer p.lock.Unlock()
	r, err := http.Get(fmt.Sprintf("http://%s/schemas/ids/%d", p.RegistryURL, id))
	if err != nil {
		return nil, errors.Wrap(err, "getting schema from registry")
	}
	if r.StatusCode >= 300 {
		bod, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get schema, code: %d, no body", r.StatusCode)
		}
		return nil, errors.Errorf("Failed to get schema, code: %d, resp: %s", r.StatusCode, bod)
	}
	dec := json.NewDecoder(r.Body)
	schema := &Schema{}
	err = dec.Decode(schema)
	if err != nil {
		return nil, errors.Wrap(err, "decoding schema from registry")
	}
	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "creating codec from schema")
	}
	p.cache[id] = codec
	return codec, nil
}
