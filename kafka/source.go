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

package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/elodina/go-avro"
	"github.com/pkg/errors"
)

// Source implements the pdk.Source interface using kafka as a data source.
type Source struct {
	Hosts   []string
	Topics  []string
	Group   string
	Type    string
	MaxMsgs int
	numMsgs int

	consumer *cluster.Consumer
	messages <-chan *sarama.ConsumerMessage
}

// NewSource gets a new Source
func NewSource() *Source {
	return &Source{
		Hosts:  []string{"localhost:9092"},
		Topics: []string{"test"},
		Group:  "group0",
		Type:   "json",
	}
}

// Record returns the value of the next kafka message.
func (s *Source) Record() (interface{}, error) {
	if s.MaxMsgs > 0 {
		s.numMsgs++
		if s.numMsgs > s.MaxMsgs {
			return nil, io.EOF
		}
	}
	msg, ok := <-s.consumer.Messages()
	if ok {
		var ret interface{}
		switch s.Type {
		case "json":
			parsed := make(map[string]interface{})
			err := json.Unmarshal(msg.Value, &parsed)
			if err != nil {
				return nil, errors.Wrap(err, "unmarshaling json")
			}
			ret = parsed
		case "raw":
			ret = msg
		default:
			return nil, errors.Errorf("unsupported kafka message type: '%v'", s.Type)
		}
		s.consumer.MarkOffset(msg, "") // mark message as processed
		return ret, nil
	}
	return nil, errors.New("messages channel closed")
}

// Open initializes the kafka source.
func (s *Source) Open() error {
	// init (custom) config, enable errors and notifications
	sarama.Logger = log.New(ioutil.Discard, "", 0)
	config := cluster.NewConfig()
	config.Config.Version = sarama.V0_10_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true

	var err error
	s.consumer, err = cluster.NewConsumer(s.Hosts, s.Group, s.Topics, config)
	if err != nil {
		return errors.Wrap(err, "getting new consumer")
	}
	s.messages = s.consumer.Messages()

	// consume errors
	go func() {
		for err := range s.consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range s.consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()
	return nil
}

// Close closes the underlying kafka consumer.
func (s *Source) Close() error {
	err := s.consumer.Close()
	return errors.Wrap(err, "closing kafka consumer")
}

// ConfluentSource implements pdk.Source using Kafka and the Confluent schema
// registry.
type ConfluentSource struct {
	Source
	RegistryURL string
	lock        sync.RWMutex
	cache       map[int32]avro.Schema
}

// NewConfluentSource returns a new ConfluentSource.
func NewConfluentSource() *ConfluentSource {
	src := &ConfluentSource{
		cache: make(map[int32]avro.Schema),
	}
	src.Type = "raw"
	return src
}

// Record returns the next value from kafka.
func (s *ConfluentSource) Record() (interface{}, error) {
	rec, err := s.Source.Record()
	if err != nil {
		return rec, err
	}
	msg, ok := rec.(*sarama.ConsumerMessage)
	if !ok {
		return rec, errors.Errorf("record is not a raw kafka record, but a %T", rec)
	}
	val := msg.Value
	return s.decodeAvroValueWithSchemaRegistry(val)
}

func (s *ConfluentSource) decodeAvroValueWithSchemaRegistry(val []byte) (interface{}, error) {
	if len(val) <= 6 || val[0] != 0 {
		return nil, errors.Errorf("unexpected magic byte or length in avro kafka value, should be 0x00, but got 0x%.8s", val)
	}
	id := int32(binary.BigEndian.Uint32(val[1:]))
	codec, err := s.getCodec(id)
	if err != nil {
		return nil, errors.Wrap(err, "getting avro codec")
	}
	ret, err := avroDecode(codec, val[5:])
	return ret, errors.Wrap(err, "decoding avro record")
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

func (s *ConfluentSource) getCodec(id int32) (rschema avro.Schema, rerr error) {
	s.lock.RLock()
	if codec, ok := s.cache[id]; ok {
		s.lock.RUnlock()
		return codec, nil
	}
	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()
	r, err := http.Get(fmt.Sprintf("http://%s/schemas/ids/%d", s.RegistryURL, id))
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
	s.cache[id] = codec
	return codec, rerr
}

func avroDecode(codec avro.Schema, data []byte) (map[string]interface{}, error) {
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
