package enterprise

import (
	"io"
	"log"
	"sync"

	"github.com/pilosa/pdk"
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
			frames := make(map[string]ChanBitIterator)
			var err error
			for {
				rec, err := n.src.Record()
				if err != nil {
					break
				}
				ent, err := n.parser.Parse(rec)
				if err != nil {
					log.Printf("couldn't parse record %s, err: %v", rec, err)
					continue
				}
				// this logic is non-general - treats everything as a string and doesn't use fields at all.
				pdk.Walk(ent, func(path []string, l pdk.Literal) {
					name, err := n.framer.Frame(path)
					if err != nil {
						log.Printf("couldn't frame %v: %v", path, err)
						return
					}
					iter, err := n.getFrame(frames, name)
					if err != nil {
						log.Fatalf("couldn't get frame: %v, err: %v", name, err)
						return
					}

					iter <- Bit{Column: string(ent.Subject), Row: string(l.(pdk.S))}
				})
			}
			if err != io.EOF && err != nil {
				log.Printf("error in ingest run loop: %v", err)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}

func (n *Ingester) getFrame(frames map[string]ChanBitIterator, frame string) (ChanBitIterator, error) {
	if iter, ok := frames[frame]; ok {
		return iter, nil
	}
	iter, err := n.indexer.Frame(frame)
	if err != nil {
		return nil, err
	}
	frames[frame] = iter
	return iter, nil
}
