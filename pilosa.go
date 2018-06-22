package pdk

import (
	"io"
	"log"
	"sync"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Index struct {
	client    *gopilosa.Client
	batchSize uint

	lock        sync.RWMutex
	index       *gopilosa.Index
	importWG    sync.WaitGroup
	recordChans map[string]chanRecordIterator
}

func newIndex() *Index {
	return &Index{
		recordChans: make(map[string]chanRecordIterator),
	}
}

// Client returns a Pilosa client.
func (i *Index) Client() *gopilosa.Client {
	return i.client
}

// AddColumnTimestamp adds a column to be imported to Pilosa with a timestamp.
func (i *Index) AddColumnTimestamp(field string, row, col uint64, ts time.Time) {
	i.addColumn(field, row, col, ts.UnixNano())
}

// AddColumn adds a column to be imported to Pilosa.
func (i *Index) AddColumn(field string, col uint64, row uint64) {
	i.addColumn(field, col, row, 0)
}

func (i *Index) addColumn(fieldName string, col uint64, row uint64, ts int64) {
	var c chanRecordIterator
	var ok bool
	i.lock.RLock()
	if c, ok = i.recordChans[fieldName]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		fieldType := gopilosa.OptFieldSet(gopilosa.CacheTypeRanked, 100000)
		if ts != 0 {
			fieldType = gopilosa.OptFieldTime(gopilosa.TimeQuantumYearMonthDayHour)
		}
		field, err := i.index.Field(fieldName, []gopilosa.FieldOption{fieldType})
		if err != nil {
			log.Println(errors.Wrapf(err, "describing field: %v", fieldName))
			return
		}
		err = i.setupField(field)
		if err != nil {
			log.Println(errors.Wrapf(err, "setting up field '%s'", fieldName)) // TODO make AddBit/AddValue return err?
			return
		}
		c = i.recordChans[fieldName]
	} else {
		i.lock.RUnlock()
	}
	c <- gopilosa.Bit{RowID: row, ColumnID: col, Timestamp: ts}
}

// AddValue adds a value to be imported to Pilosa.
func (i *Index) AddValue(fieldName string, col uint64, val int64) {
	var c chanRecordIterator
	var ok bool

	i.lock.RLock()
	if c, ok = i.recordChans[fieldName]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		field, err := i.index.Field(fieldName, gopilosa.OptFieldInt(0, 1<<31-1))
		if err != nil {
			log.Println(errors.Wrap(err, "describing field"))
			return
		}
		err = i.setupField(field)
		if err != nil {
			log.Println(errors.Wrap(err, "setting up field"))
			return
		}
		c = i.recordChans[fieldName]
	} else {
		i.lock.RUnlock()
	}
	c <- gopilosa.FieldValue{ColumnID: col, Value: val}
}

// Close ensures that all ongoing imports have finished and cleans up internal
// state.
func (i *Index) Close() error {
	for _, cbi := range i.recordChans {
		close(cbi)
	}
	i.importWG.Wait()
	return nil
}

func NewRankedField(index *gopilosa.Index, name string, size int) *gopilosa.Field {
	field, err := index.Field(name, gopilosa.OptFieldSet(gopilosa.CacheTypeRanked, size))
	if err != nil {
		panic(err)
	}
	return field
}

func NewIntField(index *gopilosa.Index, name string, min, max int64) *gopilosa.Field {
	field, err := index.Field(name, gopilosa.OptFieldInt(min, max))
	if err != nil {
		panic(err)
	}
	return field

}

// setupField ensures the existence of a field in Pilosa,
// and starts importers for the field.
// It is not threadsafe - callers must hold i.lock.Lock() or guarantee that they have
// exclusive access to Index before calling.
func (i *Index) setupField(field *gopilosa.Field) error {
	fieldName := field.Name()
	if _, ok := i.recordChans[fieldName]; !ok {
		err := i.client.EnsureField(field)
		if err != nil {
			return errors.Wrapf(err, "creating field '%v'", field)
		}
		i.recordChans[fieldName] = newChanRecordIterator()
		i.importWG.Add(1)
		go func(fram *gopilosa.Field, cbi chanRecordIterator) {
			defer i.importWG.Done()
			err := i.client.ImportField(fram, cbi, gopilosa.OptImportStrategy(gopilosa.BatchImport), gopilosa.OptImportBatchSize(int(i.batchSize)))
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", fieldName))
			}
		}(field, i.recordChans[fieldName])
	}
	return nil
}

// SetupPilosa returns a new Indexer after creating the given fields and starting importers.
func SetupPilosa(hosts []string, indexName string, schema *gopilosa.Schema, batchsize uint) (Indexer, error) {
	indexer := newIndex()
	indexer.batchSize = batchsize
	client, err := gopilosa.NewClient(hosts,
		gopilosa.OptClientSocketTimeout(time.Minute*60),
		gopilosa.OptClientConnectTimeout(time.Second*60))
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa cluster client")
	}
	indexer.client = client
	err = client.SyncSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "synchronizing schema")
	}
	indexer.index, err = schema.Index(indexName)
	if err != nil {
		return nil, errors.Wrap(err, "getting index")
	}
	for _, field := range indexer.index.Fields() {
		err := indexer.setupField(field)
		if err != nil {
			return nil, errors.Wrapf(err, "setting up field '%s'", field.Name())
		}
	}
	return indexer, nil
}

type chanRecordIterator chan gopilosa.Record

func newChanRecordIterator() chanRecordIterator {
	return make(chan gopilosa.Record, 200000)
}

func (c chanRecordIterator) NextRecord() (gopilosa.Record, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}
