package kafka

import (
	"github.com/pilosa/pdk/v2"
	"github.com/pkg/errors"
)

type Main struct {
	pdk.Main    `flag:"!embed"`
	KafkaHosts  []string `help:"Comma separated list of host:port pairs for Kafka."`
	RegistryURL string   `help:"Location of Confluent Schema Registry. Must start with 'https://' if you want to use TLS."`
	MaxMsgs     int      `help:"Number of messages to consume from Kafka before stopping. Useful for testing when you don't want to run indefinitely."`
	Group       string   `help:"Kafka group."`
	Topics      []string `help:"Kafka topics to read from."`
}

func NewMain() *Main {
	m := &Main{
		Main:        *pdk.NewMain(),
		KafkaHosts:  []string{"localhost:9092"},
		RegistryURL: "localhost:8081",
		Group:       "defaultgroup",
		Topics:      []string{"defaulttopic"},
	}
	m.NewSource = func() (pdk.Source, error) {
		source := NewSource()
		source.Hosts = m.KafkaHosts
		source.RegistryURL = m.RegistryURL
		source.Group = m.Group
		source.Topics = m.Topics
		source.MaxMsgs = m.MaxMsgs
		source.Log = m.Main.Log()

		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}
