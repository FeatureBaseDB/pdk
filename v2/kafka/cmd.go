package kafka

import (
	"github.com/pilosa/pdk/v2"
	"github.com/pkg/errors"
)

type Main struct {
	pdk.Main    `flag:"!embed"`
	KafkaHosts  []string `help:"Comma separated list of host:port pairs for Kafka."`
	RegistryURL string   `help:"Location of Confluent Schema Registry. Must start with 'https://' if you want to use TLS."`
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
		source.Topics = m.Topics
		source.Group = m.Group
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
