package kafka

import (
	"bytes"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"

	"encoding/gob"
)

type Source struct {
	KafkaHosts []string
	Topics     []string
	Group      string

	consumer *cluster.Consumer
	messages <-chan *sarama.ConsumerMessage
}

func NewSource() *Source {
	return &Source{}
}

type Record struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

func Decode(raw []byte) (Record, error) {
	b := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(b)
	rec := Record{}
	err := dec.Decode(&rec)
	return rec, errors.Wrap(err, "decoding bytes")
}

func (s *Source) Record() ([]byte, error) {
	msg, ok := <-s.consumer.Messages()
	if ok {
		rec := Record{Key: msg.Key, Value: msg.Value, Timestamp: msg.Timestamp}
		var b bytes.Buffer
		enc := gob.NewEncoder(&b)
		err := enc.Encode(rec)
		if err != nil {
			return nil, errors.Wrap(err, "serializing record to bytes")
		}
		s.consumer.MarkOffset(msg, "") // mark message as processed
		return b.Bytes(), nil
	} else {
		return nil, errors.New("messages channel closed")
	}

}

func (s *Source) Open() error {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
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

func (s *Source) Close() error {
	err := s.consumer.Close()
	return errors.Wrap(err, "closing kafka consumer")
}
