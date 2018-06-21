package file

import (
	"fmt"
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main contains the configuration for an ingester with an S3 Source.
type Main struct {
	Path        string   `help:"File or directory path to read from."`
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	Framer      pdk.DashFrame
	SubjectAt   string   `help:"Tells the source to add a unique 'subject' key to each record which is the filename + record number."`
	SubjectPath []string `help:"Path to value in each record that should be mapped to column ID. Blank gets a sequential ID."`
	Proxy       string   `help:"Bind to this address to proxy and translate requests to Pilosa"`

	done chan struct{}
}

// NewMain gets a new Main with the default configuration.
func NewMain() *Main {
	return &Main{
		PilosaHosts: []string{"localhost:10101"},
		Index:       "pdk",
		BatchSize:   1000,
		SubjectAt:   "#@!pdksubj",
		SubjectPath: []string{},
		Proxy:       ":13131",
	}
}

// Run runs the ingester.
func (m *Main) Run() error {
	src, err := NewSource(
		OptSrcPath(m.Path),
		OptSrcSubjectAt(m.SubjectAt),
	)
	if err != nil {
		return errors.Wrap(err, "getting file source")
	}

	translateColumns := true
	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 && m.SubjectAt == "" {
		parser.Subjecter = pdk.BlankSubjecter{}
		translateColumns = false
	} else if len(m.SubjectPath) == 0 && m.SubjectAt != "" {
		m.SubjectPath = []string{m.SubjectAt}
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &m.Framer
	if translateColumns {
		fmt.Println("setting col translator")
		mapper.ColTranslator = pdk.NewMapFrameTranslator()
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}
	ingester := pdk.NewIngester(src, parser, mapper, indexer)

	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator, mapper.ColTranslator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	if err := ingester.Run(); err != nil {
		return errors.Wrap(err, "running ingester")
	}
	return nil
}
