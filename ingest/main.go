package ingest

import (
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/kafka"
	"github.com/pkg/errors"
)

type Main struct {
	KafkaHosts  []string
	KafkaTopics []string
	KafkaGroup  string
	RegistryURL string
	PilosaHosts []string
	Index       string
	BatchSize   uint
}

func NewMain() *Main {
	return &Main{
		KafkaHosts:  []string{"localhost:9092"},
		KafkaTopics: []string{"test"},
		KafkaGroup:  "group0",
		RegistryURL: "localhost:8081",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "pdk",
		BatchSize:   1000,
	}
}

func (m *Main) Run() error {
	src := &kafka.KafkaSource{}
	src.KafkaHosts = m.KafkaHosts
	src.Topics = m.KafkaTopics
	src.Group = m.KafkaGroup
	if m.RegistryURL != "" {
		src.Type = "json"
	} else {
		src.Type = "raw"
	}
	err := src.Open()
	if err != nil {
		return errors.Wrap(err, "opening kafka source")
	}

	var parser pdk.Parrrser
	parser = pdk.NewDefaultGenericParser()

	var mapper pdk.Mapppper
	mapper = pdk.NewDefaultGenericMapper()

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
