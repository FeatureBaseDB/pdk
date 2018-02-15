package http

import (
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

type Main struct {
	Bind        string   `help:"Listen for post requests on this address."`
	PilosaHosts []string `help:"List of host:port pairs for Pilosa cluster."`
	Index       string   `help:"Pilosa index to write to."`
	BatchSize   uint     `help:"Batch size for Pilosa imports."`
}

func NewMain() *Main {
	return &Main{
		Bind:        ":0",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "jsonhttp",
		BatchSize:   10,
	}
}

func (m *Main) Run() error {
	src, err := NewJSONSource(WithAddr(m.Bind))
	if err != nil {
		return errors.Wrap(err, "getting json source")
	}

	log.Println("listening on", src.Addr())

	var parser pdk.Parrrser
	parser = pdk.NewDefaultGenericParser()

	var mapper pdk.Mapppper
	mapper = pdk.NewCollapsingMapper()

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
