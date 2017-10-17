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

func (m *Main) Run() error {
	src := kafka.NewSource()
	src.KafkaHosts = m.KafkaHosts
	src.Topics = m.KafkaTopics
	src.Group = m.KafkaGroup

	var parser pdk.Parrrser
	if m.RegistryURL != "" {
		parser = kafka.NewAvroParserRegistry(m.RegistryURL)
	} else {
		parser = &kafka.JSONParser{}
	}

	var mapper pdk.Mapppper
	mapper = pdk.NewDefaultGenericMapper()

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
