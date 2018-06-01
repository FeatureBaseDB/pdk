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
