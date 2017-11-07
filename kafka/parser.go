package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/elodina/go-avro"
	"github.com/pkg/errors"
)

type AvroParser struct {
	RegistryURL string

	lock  sync.RWMutex
	cache map[int32]avro.Schema
}

func NewAvroParserRegistry(registryURL string) *AvroParser {
	return &AvroParser{
		RegistryURL: registryURL,
		cache:       make(map[int32]avro.Schema),
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
	val, err := AvroDecode(codec, kr.Value[5:])
	return val, errors.Wrap(err, "decoding avro record")
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	Id      int    `json:"id"`      // Registry's unique id
}

func (p *AvroParser) GetCodec(id int32) (s avro.Schema, rerr error) {
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
	defer func() {
		// hahahahahaha
		rerr = r.Body.Close()
	}()
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
	codec, err := avro.ParseSchema(schema.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "parsing schema")
	}
	p.cache[id] = codec
	return codec, rerr
}

func AvroDecode(codec avro.Schema, data []byte) (map[string]interface{}, error) {
	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(codec)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(data)

	decodedRecord := avro.NewGenericRecord(codec)
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err := reader.Read(decodedRecord, decoder)
	if err != nil {
		return nil, errors.Wrap(err, "reading generic datum")
	}

	return decodedRecord.Map(), nil
}

type JSONParser struct{}

func (p *JSONParser) Parse(record []byte) (interface{}, error) {
	kr, err := Decode(record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't decode kafka record")
	}

	parsed := make(map[string]interface{})

	err = json.Unmarshal(kr.Value, &parsed)
	return parsed, errors.Wrap(err, "unmarshaling json")

}
