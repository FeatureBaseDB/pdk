package pdk

import (
	"io"
	"log"
	"sync"
	"time"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Index struct {
	client    *pcli.Client
	batchSize uint

	lock       sync.RWMutex
	index      *pcli.Index
	importWG   sync.WaitGroup
	bitChans   map[string]ChanBitIterator
	fieldChans map[string]map[string]ChanValIterator
}

func NewIndex() *Index {
	return &Index{
		bitChans:   make(map[string]ChanBitIterator),
		fieldChans: make(map[string]map[string]ChanValIterator),
	}
}

func (i *Index) Client() *pcli.Client {
	return i.client
}

func (i *Index) AddBit(frame string, col uint64, row uint64) {
	var c ChanBitIterator
	var ok bool
	i.lock.RLock()
	if c, ok = i.bitChans[frame]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		err := i.setupFrame(FrameSpec{Name: frame, CacheType: pcli.CacheTypeRanked, CacheSize: 100000})
		if err != nil {
			log.Println(errors.Wrapf(err, "setting up frame '%s'", frame)) // TODO make AddBit/AddValue return err?
			return
		}
		c = i.bitChans[frame]
	} else {
		i.lock.RUnlock()
	}
	c <- pcli.Bit{RowID: row, ColumnID: col}
}

func (i *Index) AddValue(frame, field string, col uint64, val int64) {
	var c ChanValIterator
	i.lock.RLock()
	fieldmap, ok := i.fieldChans[frame]
	if ok {
		c, ok = fieldmap[field]
	}
	if !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		err := i.setupFrame(FrameSpec{
			Name:      frame,
			CacheType: pcli.CacheTypeRanked,
			CacheSize: 1000,
			Fields: []FieldSpec{
				{
					Name: field,
					Min:  0,
					Max:  1 << 32,
				},
			}})
		if err != nil {
			log.Println(errors.Wrap(err, "setting up field/frame"))
			return
		}
		c = i.fieldChans[frame][field]
	} else {
		i.lock.RUnlock()
	}
	c <- pcli.FieldValue{ColumnID: col, Value: val}
}

func (i *Index) Close() error {
	for _, cbi := range i.bitChans {
		close(cbi)
	}
	for _, m := range i.fieldChans {
		for _, cvi := range m {
			close(cvi)
		}
	}
	i.importWG.Wait()
	return nil
}

type FrameSpec struct {
	Name           string
	CacheType      pcli.CacheType
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
		CacheType: pcli.CacheTypeRanked,
		CacheSize: uint(size),
	}
	return fs
}

// NewFieldFrameSpec creates a frame which is dedicated to a single BSI field
// which will have the same name as the frame
func NewFieldFrameSpec(name string, min int, max int) FrameSpec {
	fs := FrameSpec{
		Name:      name,
		CacheType: pcli.CacheType(""),
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
	var fram *pcli.Frame
	var err error
	if _, ok := i.bitChans[frame.Name]; !ok {
		frameOptions := &pcli.FrameOptions{CacheType: frame.CacheType, CacheSize: frame.CacheSize}
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
		i.importWG.Add(1)
		go func(fram *pcli.Frame, frame FrameSpec) {
			defer i.importWG.Done()
			err := i.client.ImportFrame(fram, i.bitChans[frame.Name], i.batchSize)
			if err != nil {
				log.Println(errors.Wrapf(err, "starting frame import for %v", frame.Name))
			}
		}(fram, frame)
	} else {
		fram, err = i.index.Frame(frame.Name, nil)
		if err != nil {
			return errors.Wrap(err, "making frame: %v")
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
			return errors.Wrapf(err, "creating field %#v", field)
		}
		i.importWG.Add(1)
		go func(fram *pcli.Frame, frame FrameSpec, field FieldSpec) {
			defer i.importWG.Done()
			err := i.client.ImportValueFrame(fram, field.Name, i.fieldChans[frame.Name][field.Name], i.batchSize)
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", field))
			}
		}(fram, frame, field)
	}
	return nil
}

func (i *Index) ensureField(frame *pcli.Frame, fieldSpec FieldSpec) error {
	if _, exists := frame.Fields()[fieldSpec.Name]; exists {
		return nil
	}
	err := i.client.CreateIntField(frame, fieldSpec.Name, fieldSpec.Min, fieldSpec.Max)
	return errors.Wrapf(err, "creating field %#v", fieldSpec)
}

func SetupPilosa(hosts []string, index string, frames []FrameSpec, batchsize uint) (Indexer, error) {
	indexer := NewIndex()
	indexer.batchSize = batchsize
	client, err := pcli.NewClientFromAddresses(hosts,
		&pcli.ClientOptions{SocketTimeout: time.Minute * 60,
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
	err = client.EnsureIndex(indexer.index)
	if err != nil {
		return nil, errors.Wrap(err, "ensuring index existence")
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
	return make(chan pcli.Bit, 200000)
}

type ChanBitIterator chan pcli.Bit

func (c ChanBitIterator) NextBit() (pcli.Bit, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}

func NewChanValIterator() ChanValIterator {
	return make(chan pcli.FieldValue, 200000)
}

type ChanValIterator chan pcli.FieldValue

func (c ChanValIterator) NextValue() (pcli.FieldValue, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}
