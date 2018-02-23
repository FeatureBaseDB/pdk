package pdk

import (
	"io"
	"log"
	"sync"
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
}

// NewIngester gets a new Ingester.
func NewIngester(source Source, parser RecordParser, mapper RecordMapper, indexer Indexer) *Ingester {
	return &Ingester{
		ParseConcurrency: 1,
		src:              source,
		parser:           parser,
		mapper:           mapper,
		indexer:          indexer,
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
				var rec interface{}
				rec, recordErr = n.src.Record()
				if recordErr != nil {
					break
				}
				val, err := n.parser.Parse(rec)
				if err != nil {
					log.Printf("couldn't parse record %s, err: %v", rec, err)
					continue
				}
				pr, err := n.mapper.Map(val)
				if err != nil {
					log.Printf("couldn't map val: %s, err: %v", val, err)
					continue
				}
				for _, row := range pr.Rows {
					n.indexer.AddBit(row.Frame, pr.Col, row.ID)
				}
				for _, val := range pr.Vals {
					n.indexer.AddValue(val.Frame, val.Field, pr.Col, val.Value)
				}
			}
			if recordErr != io.EOF && recordErr != nil {
				log.Printf("error in ingest run loop: %v", recordErr)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}
