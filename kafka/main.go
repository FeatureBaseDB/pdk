package kafka

import (
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main holds the options for running Pilosa ingestion from Kafka.
type Main struct {
	Hosts       []string
	Topics      []string
	Group       string
	RegistryURL string
	PilosaHosts []string
	Index       string
	BatchSize   uint
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Hosts:       []string{"localhost:9092"},
		Topics:      []string{"test"},
		Group:       "group0",
		RegistryURL: "localhost:8081",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "pdk",
		BatchSize:   1000,
	}
}

// Run begins indexing data from Kafka into Pilosa.
func (m *Main) Run() error {
	src := &Source{}
	src.Hosts = m.Hosts
	src.Topics = m.Topics
	src.Group = m.Group
	if m.RegistryURL != "" {
		src.Type = "json"
	} else {
		src.Type = "raw"
	}

	if err := src.Open(); err != nil {
		return errors.Wrap(err, "opening kafka source")
	}

	parser := pdk.NewDefaultGenericParser()

	mapper := pdk.NewCollapsingMapper()

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
