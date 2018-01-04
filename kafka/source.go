package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/elodina/go-avro"
	"github.com/pkg/errors"
)

type KafkaSource struct {
	KafkaHosts []string
	Topics     []string
	Group      string
	Type       string

	consumer *cluster.Consumer
	messages <-chan *sarama.ConsumerMessage
}

func (s *KafkaSource) Record() (interface{}, error) {
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
	} else {
		return nil, errors.New("messages channel closed")
	}
}

func (s *KafkaSource) Open() error {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Config.Version = sarama.V0_10_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true

	var err error
	s.consumer, err = cluster.NewConsumer(s.KafkaHosts, s.Group, s.Topics, config)
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

func (s *KafkaSource) Close() error {
	err := s.consumer.Close()
	return errors.Wrap(err, "closing kafka consumer")
}

type ConfluentSource struct {
	KafkaSource
	RegistryURL string
	lock        sync.RWMutex
	cache       map[int32]avro.Schema
}

func NewConfluentSource() *ConfluentSource {
	src := &ConfluentSource{
		cache: make(map[int32]avro.Schema),
	}
	src.Type = "raw"
	return src
}

func (s *ConfluentSource) Record() (interface{}, error) {
	rec, err := s.KafkaSource.Record()
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
	codec, err := s.GetCodec(id)
	if err != nil {
		return nil, errors.Wrap(err, "getting avro codec")
	}
	ret, err := AvroDecode(codec, val[5:])
	return ret, errors.Wrap(err, "decoding avro record")
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	Id      int    `json:"id"`      // Registry's unique id
}

func (p *ConfluentSource) GetCodec(id int32) (s avro.Schema, rerr error) {
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
