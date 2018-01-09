package pdk

import (
	"io"
	"log"
	"time"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Indexer interface {
	AddBit(frame string, col uint64, row uint64)
	AddValue(frame, field string, col uint64, val int64)
	Close() error
}

type Index struct {
	client *pcli.Client

	bitChans   map[string]ChanBitIterator
	fieldChans map[string]map[string]ChanValIterator
}

func NewIndex() *Index {
	return &Index{
		bitChans:   make(map[string]ChanBitIterator),
		fieldChans: make(map[string]map[string]ChanValIterator),
	}
}

func (i *Index) AddBit(frame string, col uint64, row uint64) {
	var c ChanBitIterator
	var ok bool
	if c, ok = i.bitChans[frame]; !ok {
		log.Printf("Unknown frame in AddBit: %v", frame)
	}
	c <- pcli.Bit{RowID: row, ColumnID: col}
}

func (i *Index) AddValue(frame, field string, col uint64, val int64) {
	fieldmap, ok := i.fieldChans[frame]
	if !ok {
		log.Printf("Unknown frame in AddValue: %v", frame)
		return
	}
	var c ChanValIterator
	if c, ok = fieldmap[field]; !ok {
		log.Printf("Unknown field in AddValue: %v", field)
		return
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

func SetupPilosa(hosts []string, index string, frames []FrameSpec, batchsize uint) (Indexer, error) {
	var BATCHSIZE uint = batchsize
	indexer := NewIndex()
	client, err := pcli.NewClientFromAddresses(hosts,
		&pcli.ClientOptions{SocketTimeout: time.Minute * 60,
			ConnectTimeout: time.Second * 60,
		})
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa cluster client")
	}
	indexer.client = client

	idx, err := pcli.NewIndex(index)
	if err != nil {
		return nil, errors.Wrap(err, "making index")
	}
	err = client.EnsureIndex(idx)
	if err != nil {
		return nil, errors.Wrap(err, "ensuring index existence")
	}
	for _, frame := range frames {
		frameOptions := &pcli.FrameOptions{CacheType: frame.CacheType, CacheSize: frame.CacheSize}
		for _, field := range frame.Fields {
			err := frameOptions.AddIntField(field.Name, field.Min, field.Max)
			if err != nil {
				return nil, errors.Wrapf(err, "adding int field %v", field)
			}
		}
		fram, err := idx.Frame(frame.Name, frameOptions)
		if err != nil {
			return nil, errors.Wrap(err, "making frame: %v")
		}
		err = client.EnsureFrame(fram)
		if err != nil {
			return nil, errors.Wrapf(err, "creating frame '%v'", frame)
		}

		indexer.fieldChans[frame.Name] = make(map[string]ChanValIterator)
		indexer.bitChans[frame.Name] = NewChanBitIterator()
		go func(fram *pcli.Frame, frame FrameSpec) {
			err := client.ImportFrame(fram, indexer.bitChans[frame.Name], BATCHSIZE)
			if err != nil {
				log.Println(errors.Wrapf(err, "starting frame import for %v", frame.Name))
			}
		}(fram, frame)
		for _, field := range frame.Fields {
			indexer.fieldChans[frame.Name][field.Name] = NewChanValIterator()
			go func(fram *pcli.Frame, frame FrameSpec, field FieldSpec) {
				err := client.ImportValueFrame(fram, field.Name, indexer.fieldChans[frame.Name][field.Name], BATCHSIZE)
				if err != nil {
					log.Println(errors.Wrapf(err, "starting field import for %v", field))
				}
			}(fram, frame, field)
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
