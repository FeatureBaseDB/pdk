package pdk

import (
	"io"
	"log"
	"sync"
)

type Ingester struct {
	ParseConcurrency int

	src     Source
	parser  Parrrser
	mapper  Mapppper
	indexer Indexer
}

func NewIngester(source Source, parser Parrrser, mapper Mapppper, indexer Indexer) *Ingester {
	return &Ingester{
		ParseConcurrency: 1,
		src:              source,
		parser:           parser,
		mapper:           mapper,
		indexer:          indexer,
	}
}

func (n *Ingester) Run() error {
	pwg := sync.WaitGroup{}
	for i := 0; i < n.ParseConcurrency; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var err error
			for {
				rec, err := n.src.Record()
				if err != nil {
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
			if err != io.EOF && err != nil {
				log.Printf("error in ingest run loop: %v", err)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}
