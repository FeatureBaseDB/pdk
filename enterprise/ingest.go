package enterprise

import (
	"io"
	"log"
	"strings"
	"sync"

	gopilosa "github.com/pilosa/go-pilosa"
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
			frames := make(map[string]ChanBitIterator)
			fields := make(map[string]map[string]ChanValIterator)
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
				// this logic is non-general
				pdk.Walk(ent, func(path []string, l pdk.Literal) {
					if ls, ok := l.(pdk.S); ok {
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
						iter <- gopilosa.Bit{ColumnKey: string(ent.Subject), RowKey: string(ls)}
					} else if li, ok := l.(pdk.I); ok {
						name, field, err := n.framer.Field(path)
						if Contains(name, "date", "dist", "tude", "amount", "tax", "extra", "sur") {
							return
						}
						if err != nil {
							log.Printf("couldn't Field %v: %v", path, err)
							return
						}
						iter, err := n.getField(fields, name, field)
						if err != nil {
							log.Fatalf("couldn't get field: %v, err: %v", name, err)
							return
						}
						iter <- gopilosa.FieldValue{ColumnKey: string(ent.Subject), Value: int64(li)}
					} else {
						log.Printf("unhandled type %T, val %v frame %v", l, l, path)
					}
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

func (n *Ingester) getField(fields map[string]map[string]ChanValIterator, frame, field string) (ChanValIterator, error) {
	fmap, ok := fields[frame]
	if !ok {
		fmap = make(map[string]ChanValIterator)
		fields[frame] = fmap
	}
	iter, ok := fmap[field]
	var err error
	if !ok {
		iter, err = n.indexer.Field(frame, field)
		if err != nil {
			return nil, errors.Wrap(err, "getting field iter")
		}
		fmap[field] = iter
	}
	return iter, nil

}

func Contains(name string, any ...string) bool {
	for _, a := range any {
		if strings.Contains(name, a) {
			return true
		}
	}
	return false
}
