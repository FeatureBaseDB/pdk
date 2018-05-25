// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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

	lock       sync.RWMutex
	index      *gopilosa.Index
	importWG   sync.WaitGroup
	bitChans   map[string]chanBitIterator
	fieldChans map[string]map[string]chanValIterator
}

func newIndex() *Index {
	return &Index{
		bitChans:   make(map[string]chanBitIterator),
		fieldChans: make(map[string]map[string]chanValIterator),
	}
}

// Client returns a Pilosa client.
func (i *Index) Client() *gopilosa.Client {
	return i.client
}

// AddBitTimestamp adds a bit to be imported to Pilosa with a timestamp.
func (i *Index) AddBitTimestamp(frame string, row, col uint64, ts time.Time) {
	i.addBit(frame, row, col, ts.UnixNano())
}

// AddBit adds a bit to be imported to Pilosa.
func (i *Index) AddBit(frame string, col uint64, row uint64) {
	i.addBit(frame, col, row, 0)
}

func (i *Index) addBit(frame string, col uint64, row uint64, ts int64) {
	var c chanBitIterator
	var ok bool
	i.lock.RLock()
	if c, ok = i.bitChans[frame]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		frameSpec := FrameSpec{Name: frame, CacheType: gopilosa.CacheTypeRanked, CacheSize: 100000}
		if ts != 0 {
			frameSpec.TimeQuantum = gopilosa.TimeQuantumYearMonthDayHour
		}
		err := i.setupFrame(frameSpec)
		if err != nil {
			log.Println(errors.Wrapf(err, "setting up frame '%s'", frame)) // TODO make AddBit/AddValue return err?
			return
		}
		c = i.bitChans[frame]
	} else {
		i.lock.RUnlock()
	}
	c <- gopilosa.Bit{RowID: row, ColumnID: col, Timestamp: ts}
}

// AddValue adds a value to be imported to Pilosa.
func (i *Index) AddValue(frame, field string, col uint64, val int64) {
	var c chanValIterator
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
			CacheType: gopilosa.CacheTypeRanked,
			CacheSize: 1000,
			Fields: []FieldSpec{
				{
					Name: field,
					Min:  0,
					Max:  1<<31 - 1,
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
	c <- gopilosa.FieldValue{ColumnID: col, Value: val}
}

// Close ensures that all ongoing imports have finished and cleans up internal
// state.
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

// FrameSpec holds a frame name and options.
type FrameSpec struct {
	Name           string
	CacheType      gopilosa.CacheType
	CacheSize      uint
	InverseEnabled bool
	TimeQuantum    gopilosa.TimeQuantum
	Fields         []FieldSpec
}

func (f FrameSpec) toOptions() *gopilosa.FrameOptions {
	return &gopilosa.FrameOptions{
		TimeQuantum:    f.TimeQuantum,
		CacheType:      f.CacheType,
		CacheSize:      f.CacheSize,
		InverseEnabled: f.InverseEnabled,
		RangeEnabled:   len(f.Fields) > 0,
	}
}

// FieldSpec holds a field name and options.
type FieldSpec struct {
	Name string
	Min  int
	Max  int
}

// NewRankedFrameSpec returns a new FrameSpec with the cache type ranked and the
// given name and size.
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
		fram, err = i.index.Frame(frame.Name, frame.toOptions())
		if err != nil {
			return errors.Wrapf(err, "making frame: %v", frame.Name)
		}
		err = i.client.EnsureFrame(fram)
		if err != nil {
			return errors.Wrapf(err, "creating frame '%v'", frame)
		}
		i.bitChans[frame.Name] = newChanBitIterator()
		i.importWG.Add(1)
		go func(fram *gopilosa.Frame, cbi chanBitIterator) {
			defer i.importWG.Done()
			err := i.client.ImportFrame(fram, cbi, gopilosa.OptImportStrategy(gopilosa.BatchImport), gopilosa.OptImportBatchSize(int(i.batchSize)))
			if err != nil {
				log.Println(errors.Wrapf(err, "starting frame import for %v", frame.Name))
			}
		}(fram, i.bitChans[frame.Name])
	} else {
		fram, err = i.index.Frame(frame.Name, nil)
		if err != nil {
			return errors.Wrap(err, "making frame: %v")
		}
	}
	if _, ok := i.fieldChans[frame.Name]; !ok {
		i.fieldChans[frame.Name] = make(map[string]chanValIterator)
	}

	for _, field := range frame.Fields {
		if _, ok := i.fieldChans[frame.Name][field.Name]; ok {
			continue // valChan for this field exists, so importer should already be running.
		}
		i.fieldChans[frame.Name][field.Name] = newChanValIterator()
		err := i.ensureField(fram, field)
		if err != nil {
			return errors.Wrapf(err, "ensuring field %#v", field)
		}
		i.importWG.Add(1)
		go func(fram *gopilosa.Frame, field FieldSpec, cvi chanValIterator) {
			defer i.importWG.Done()
			err := i.client.ImportValueFrame(fram, field.Name, cvi, gopilosa.OptImportStrategy(gopilosa.BatchImport), gopilosa.OptImportBatchSize(int(i.batchSize)))
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", field))
			}
		}(fram, field, i.fieldChans[frame.Name][field.Name])
	}
	return nil
}

func (i *Index) ensureField(frame *gopilosa.Frame, fieldSpec FieldSpec) error {
	if _, exists := frame.Fields()[fieldSpec.Name]; exists {
		return nil
	}
	err := i.client.CreateIntField(frame, fieldSpec.Name, fieldSpec.Min, fieldSpec.Max)
	return errors.Wrap(err, "creating field")
}

// SetupPilosa returns a new Indexer after creating the given frames and starting importers.
func SetupPilosa(hosts []string, index string, frames []FrameSpec, batchsize uint) (Indexer, error) {
	indexer := newIndex()
	indexer.batchSize = batchsize
	client, err := gopilosa.NewClient(hosts,
		gopilosa.SocketTimeout(time.Minute*60),
		gopilosa.ConnectTimeout(time.Second*60))
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

func newChanBitIterator() chanBitIterator {
	return make(chan gopilosa.Bit, 200000)
}

type chanBitIterator chan gopilosa.Bit

func (c chanBitIterator) NextRecord() (gopilosa.Record, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}

func newChanValIterator() chanValIterator {
	return make(chan gopilosa.FieldValue, 200000)
}

type chanValIterator chan gopilosa.FieldValue

func (c chanValIterator) NextRecord() (gopilosa.Record, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}
