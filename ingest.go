package pdk

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/pilosa/pdk/termstat"
)

// Ingester combines a Source, Parser, Mapper, and Indexer, and uses them to
// ingest data into Pilosa. This could be a streaming situation where the Source
// never ends, and calling it just waits for more data to be available, or a
// batch situation where the Source eventually returns io.EOF (or some other
// error), and the Ingester completes (after the other components are done).
type Ingester struct {
	ParseConcurrency int

	src     Source
	parser  RecordParser
	mapper  RecordMapper
	indexer Indexer

	Transformers []Transformer

	Stats Statter
	Log   Logger
}

// NewIngester gets a new Ingester.
func NewIngester(source Source, parser RecordParser, mapper RecordMapper, indexer Indexer) *Ingester {
	return &Ingester{
		ParseConcurrency: 1,

		src:     source,
		parser:  parser,
		mapper:  mapper,
		indexer: indexer,

		// Reasonable defaults for crosscutting dependencies.
		Stats: termstat.NewCollector(os.Stdout),
		Log:   StdLogger{log.New(os.Stderr, "Ingest", log.LstdFlags)},
	}
}

// Run runs the ingest.
func (n *Ingester) Run() error {
	pwg := sync.WaitGroup{}
	for i := 0; i < n.ParseConcurrency; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var recordErr error
			for {
				// Source
				var rec interface{}
				rec, recordErr = n.src.Record()
				if recordErr != nil {
					break
				}
				n.Stats.Count("ingest.Record", 1, 1)

				// Parse
				val, err := n.parser.Parse(rec)
				if err != nil {
					n.Log.Printf("couldn't parse record %s, err: %v", rec, err)
					n.Stats.Count("ingest.ParseError", 1, 1)
					continue
				}
				n.Stats.Count("ingest.Parse", 1, 1)

				// Transform
				for _, tr := range n.Transformers {
					err := tr.Transform(val)
					if err != nil {
						n.Log.Printf("Problem with transformer %#v: %v", tr, err)
						n.Stats.Count("ingest.TransformError", 1, 1)
					}
				}
				n.Stats.Count("ingest.Transform", 1, 1)

				// Map
				pr, err := n.mapper.Map(val)
				if err != nil {
					n.Log.Printf("couldn't map val: %s, err: %v", val, err)
					n.Stats.Count("ingest.MapError", 1, 1)
					continue
				}

				// Index
				n.Stats.Count("ingest.Map", 1, 1)
				for _, row := range pr.Rows {
					n.indexer.AddColumn(row.Field, pr.Col, row.ID)
					n.Stats.Count("ingest.AddBit", 1, 1)
				}
				for _, val := range pr.Vals {
					n.indexer.AddValue(val.Field, pr.Col, val.Value)
					n.Stats.Count("ingest.AddValue", 1, 1)
				}
			}
			if recordErr != io.EOF && recordErr != nil {
				n.Log.Printf("error in ingest run loop: %v", recordErr)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}
