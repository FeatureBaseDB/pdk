package gen

import (
	"log"
	"time"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/fake"
	"github.com/pkg/errors"
)

// Main holds the options for generating fake data and ingesting it to Pilosa.
type Main struct {
	Seed           int64  `help:"Random seed for generating data. -1 will use current nanosecond."`
	GenConcurrency int    `help:"Number of goroutines generating data."`
	Num            uint64 `help:"Number of records to generate. 0 means infinity."`
	Framer         pdk.DashFrame
	PilosaHosts    []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index          string   `help:"Pilosa index."`
	BatchSize      uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	SubjectPath    []string `help:"Path to value in each record that should be mapped to column ID. Blank gets a sequential ID."`
	Proxy          string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		GenConcurrency: 1,
		Num:            0,
		PilosaHosts:    []string{"localhost:10101"},
		Index:          "pdk",
		BatchSize:      1000,
		Proxy:          ":13131",
	}
}

// Run begins generating data and ingesting it to Pilosa.
func (m *Main) Run() error {
	if m.Seed == -1 {
		m.Seed = time.Now().UnixNano()
	}

	src := fake.NewSource(m.Seed, m.GenConcurrency, m.Num)

	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 {
		parser.Subjecter = pdk.BlankSubjecter{}
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &m.Framer

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, nil, m.BatchSize)
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
