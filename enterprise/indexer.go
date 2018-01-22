package enterprise

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

	lock       sync.Mutex
	index      *gopilosa.Index
	bitChans   map[string]ChanBitIterator
	fieldChans map[string]map[string]ChanValIterator
}

type Indexer interface {
	Frame(name string) (ChanBitIterator, error)
	Field(frame, field string) (ChanValIterator, error)
	Close() error
}

func (idx *Index) Frame(name string) (ChanBitIterator, error) {
	idx.lock.Lock()
	defer idx.lock.Unlock()

	if iterator, ok := idx.bitChans[name]; ok {
		return iterator, nil
	}
	log.Printf("creating frame %s", name)
	err := idx.setupFrame(FrameSpec{Name: name, CacheType: gopilosa.CacheTypeRanked, CacheSize: 100000})
	if err != nil {
		return nil, err
	}
	return idx.bitChans[name], nil
}

func (idx *Index) Field(frame, field string) (ChanValIterator, error) {
	idx.lock.Lock()
	defer idx.lock.Unlock()

	if valMap, ok := idx.fieldChans[frame]; ok {
		if iterator, ok := valMap[field]; ok {
			return iterator, nil
		}
	}
	frameSpec := NewFieldFrameSpec(field, -2000000000, 2000000000)
	frameSpec.Name = frame
	log.Printf("creating in field %s in frame %s", field, frame)
	err := idx.setupFrame(frameSpec)
	if err != nil {
		return nil, err
	}
	return idx.fieldChans[frame][field], nil

}

func NewIndex() *Index {
	return &Index{
		bitChans:   make(map[string]ChanBitIterator),
		fieldChans: make(map[string]map[string]ChanValIterator),
	}
}

func (i *Index) Close() error {
	i.lock.Lock()
	defer i.lock.Unlock()
	for _, cbi := range i.bitChans {
		close(cbi)
	}
	for _, m := range i.fieldChans {
		for _, cvi := range m {
			close(cvi)
		}
	}
	return nil
}

type FrameSpec struct {
	Name           string
	CacheType      gopilosa.CacheType
	CacheSize      uint
	InverseEnabled bool
	Fields         []FieldSpec
}

type FieldSpec struct {
	Name string
	Min  int
	Max  int
}

func NewRankedFrameSpec(name string, size int) FrameSpec {
	fs := FrameSpec{
		Name:      name,
		CacheType: gopilosa.CacheTypeRanked,
		CacheSize: uint(size),
	}
	return fs
}

// NewFieldFrameSpec creates a frame which is dedicated to a single BSI field
// which will have the same name as the frame
func NewFieldFrameSpec(name string, min int, max int) FrameSpec {
	fs := FrameSpec{
		Name:      name,
		CacheType: gopilosa.CacheType(""),
		CacheSize: 0,
		Fields:    []FieldSpec{{Name: name, Min: min, Max: max}},
	}
	return fs
}

// setupFrame ensures the existence of a frame with the given configuration in
// Pilosa, and starts importers for the frame and any fields. It is not
// threadsafe - callers must hold i.lock.Lock() or guarantee that they have
// exclusive access to Index before calling.
func (i *Index) setupFrame(frame FrameSpec) error {
	var fram *gopilosa.Frame
	var err error
	if _, ok := i.bitChans[frame.Name]; !ok {
		frameOptions := &gopilosa.FrameOptions{CacheType: frame.CacheType, CacheSize: frame.CacheSize}
		if len(frame.Fields) > 0 {
			frameOptions.RangeEnabled = true
		}
		fram, err = i.index.Frame(frame.Name, frameOptions)
		if err != nil {
			return errors.Wrapf(err, "making frame: %v", frame.Name)
		}
		err = i.client.EnsureFrame(fram)
		if err != nil {
			return errors.Wrapf(err, "creating frame '%v'", frame)
		}
		i.bitChans[frame.Name] = NewChanBitIterator()
		go func(fram *gopilosa.Frame, frame FrameSpec) {
			// TODO change to i.client.ImportFrameK when gopilosa supports enterprise imports
			err := i.client.ImportFrame(fram, i.bitChans[frame.Name], i.batchSize)
			if err != nil {
				log.Println(errors.Wrapf(err, "starting frame import for %v", frame.Name))
			}
		}(fram, frame)
	} else {
		fram, err = i.index.Frame(frame.Name, nil)
		if err != nil {
			return errors.Wrap(err, "getting frame: %v")
		}
	}
	if _, ok := i.fieldChans[frame.Name]; !ok {
		i.fieldChans[frame.Name] = make(map[string]ChanValIterator)
	}

	for _, field := range frame.Fields {
		if _, ok := i.fieldChans[frame.Name][field.Name]; ok {
			continue // valChan for this field exists, so importer should already be running.
		}
		i.fieldChans[frame.Name][field.Name] = NewChanValIterator()
		err := i.ensureField(fram, field)
		if err != nil {
			return err
		}
		go func(fram *gopilosa.Frame, frame FrameSpec, field FieldSpec) {
			// TODO change to i.client.ImportValueFrameK when gopilosa supports enterprise imports
			i.client.ImportValueFrame(fram, field.Name, i.fieldChans[frame.Name][field.Name], i.batchSize)
			// i.ImportValueFrameK(fram, field.Name, i.fieldChans[frame.Name][field.Name], i.batchSize)
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", field))
			}
		}(fram, frame, field)
	}
	return nil
}

func (i *Index) ensureField(frame *gopilosa.Frame, fieldSpec FieldSpec) error {
	if _, exists := frame.Fields()[fieldSpec.Name]; exists {
		return nil
	}
	err := i.client.CreateIntField(frame, fieldSpec.Name, fieldSpec.Min, fieldSpec.Max)
	return errors.Wrapf(err, "creating field %#v", fieldSpec)
}

func SetupIndex(hosts []string, index string, frames []FrameSpec, batchsize uint) (Indexer, error) {
	indexer := NewIndex()
	indexer.batchSize = batchsize
	client, err := gopilosa.NewClientFromAddresses(hosts,
		&gopilosa.ClientOptions{SocketTimeout: time.Minute * 60,
			ConnectTimeout: time.Second * 60,
		})
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa cluster client")
	}
	indexer.client = client
	schema, err := client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	indexer.index, err = schema.Index(index)
	if err != nil {
		return nil, errors.Wrap(err, "getting index")
	}
	err = client.SyncSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "ensuring index exists")
	}
	for _, frame := range frames {
		err := indexer.setupFrame(frame)
		if err != nil {
			return nil, errors.Wrapf(err, "setting up frame '%s'", frame.Name)
		}
	}
	return indexer, nil
}

func NewChanBitIterator() ChanBitIterator {
	return make(chan gopilosa.Bit, 200000)
}

type ChanBitIterator chan gopilosa.Bit

func (c ChanBitIterator) NextBit() (gopilosa.Bit, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}

func NewChanValIterator() ChanValIterator {
	return make(chan gopilosa.FieldValue, 200000)
}

type ChanValIterator chan gopilosa.FieldValue

func (c ChanValIterator) NextValue() (gopilosa.FieldValue, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}
