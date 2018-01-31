package enterprise

import (
	"io"
	"log"
	"sync"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Ingester combines a Source, Parser, Mapper, and Indexer, and uses them to
// ingest data into Pilosa. This could be a streaming situation where the Source
// never ends, and calling it just waits for more data to be available, or a
// batch situation where the Source eventually returns io.EOF (or some other
// error), and the Ingester completes (after the other components are done).
type Ingester struct {
	ParseConcurrency int

	src     pdk.Source
	parser  pdk.Parrrser
	framer  pdk.Framer
	indexer Indexer
}

func NewIngester(source pdk.Source, parser pdk.Parrrser, indexer Indexer) *Ingester {
	return &Ingester{
		ParseConcurrency: 1,
		src:              source,
		parser:           parser,
		framer:           pdk.DashFrame,
		indexer:          indexer,
	}
}

func (n *Ingester) Run() error {
	pwg := sync.WaitGroup{}
	for i := 0; i < n.ParseConcurrency; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			importer := NewImporter(WithFramer(n.framer), WithIndexer(n.indexer))
			var err error
			for {
				var rec interface{}
				rec, err = n.src.Record()
				if err != nil {
					break
				}
				ent, err := n.parser.Parse(rec)
				if err != nil {
					log.Printf("couldn't parse record %s, err: %v", rec, err)
					continue
				}
				err = importer.Import(ent)
				if err != nil {
					log.Println(errors.Wrap(err, "importing entity"))
				}
			}
			if err != io.EOF && err != nil {
				log.Printf("error in ingest run loop: %v", err)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}
