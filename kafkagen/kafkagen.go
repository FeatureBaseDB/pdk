package kafkagen

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pilosa/pdk/fake"
	"github.com/pkg/errors"
)

// Main holds the execution state for the kafka generator.
type Main struct {
	Hosts       []string
	Topic       string
	Group       string
	RegistryURL string

	Rate time.Duration
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Hosts:       []string{"localhost:9092"},
		Topic:       "test",
		Group:       "group0",
		RegistryURL: "localhost:8081",

		Rate: time.Second * 1,
	}
}

// JSONEvent implements the sarama.Encoder interface for Event using json.
type JSONEvent fake.Event

// Encode marshals the event to json.
func (e JSONEvent) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// Length returns the length of the marshalled json.
func (e JSONEvent) Length() int {
	bytes, _ := e.Encode()
	return len(bytes)
}

// Run runs the kafka generator.
func (m *Main) Run() error {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_0
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(m.Hosts, conf)
	if err != nil {
		return errors.Wrap(err, "getting new producer")
	}
	defer producer.Close()

	for ticker := time.NewTicker(m.Rate); true; <-ticker.C {
		ev := fake.GenEvent()
		msg := &sarama.ProducerMessage{Topic: m.Topic, Value: JSONEvent(*ev)}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Error sending message: '%v', backing off", err)
			time.Sleep(time.Second * 10)
		}
	}
	return nil
}
