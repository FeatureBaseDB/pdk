package kafka

import (
	"log"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main holds the options for running Pilosa ingestion from Kafka.
type Main struct {
	Hosts       []string `help:"Comma separated list of Kafka hosts and ports"`
	Topics      []string `help:"Comma separated list of Kafka topics"`
	Group       string   `help:"Kafka group"`
	RegistryURL string   `help:"URL of the confluent schema registry. Not required."`
	Framer      pdk.DashFrame
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	SubjectPath []string `help:"Path to value in each record that should be mapped to column ID. Blank gets a sequential ID."`
	Proxy       string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
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
		Proxy:       ":13131",
	}
}

// Run begins indexing data from Kafka into Pilosa.
func (m *Main) Run() error {
	src := &Source{}
	src.Hosts = m.Hosts
	src.Topics = m.Topics
	src.Group = m.Group
	if m.RegistryURL == "" {
		src.Type = "json"
	} else {
		src.Type = "raw"
	}

	if err := src.Open(); err != nil {
		return errors.Wrap(err, "opening kafka source")
	}

	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 {
		parser.Subjecter = pdk.BlankSubjecter{}
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &m.Framer

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{},
		gopilosa.OptImportStrategy(gopilosa.BatchImport),
		gopilosa.OptImportBatchSize(int(m.BatchSize)))
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	return errors.Wrap(ingester.Run(), "running ingester")
}
