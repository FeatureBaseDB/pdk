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
	"fmt"
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
func (i *Index) AddColumnTimestamp(field string, row, col uint64OrString, ts time.Time) {
	i.addColumn(field, row, col, ts.UnixNano())
}

// AddColumn adds a column to be imported to Pilosa.
func (i *Index) AddColumn(field string, col, row uint64OrString) {
	i.addColumn(field, col, row, 0)
}

type uint64OrString interface{}

func uint64Cast(u uint64OrString) uint64 {
	ret, _ := u.(uint64)
	return ret
}

func stringCast(u uint64OrString) string {
	ret, _ := u.(string)
	return ret
}

func validUint64OrString(u uint64OrString) bool {
	if _, ok := u.(uint64); !ok {
		if _, ok := u.(string); !ok {
			return false
		}
	}
	return true
}

func (i *Index) addColumn(fieldName string, col uint64OrString, row uint64OrString, ts int64) {
	if !validUint64OrString(col) || !validUint64OrString(row) {
		panic(fmt.Sprintf("a %T and a %T were passed, both must be either uint64 or string", col, row))
	}
	var c chanRecordIterator
	var ok bool
	i.lock.RLock()
	if c, ok = i.recordChans[fieldName]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		fieldType := gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 100000)
		if ts != 0 {
			fieldType = gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumYearMonthDayHour)
		}
		fieldOpts := []gopilosa.FieldOption{fieldType}
		// If row value is a string then configure the field to use row keys.
		if _, ok := row.(string); ok {
			fieldOpts = append(fieldOpts, gopilosa.OptFieldKeys(true))
		}
		field := i.index.Field(fieldName, fieldOpts...)
		err := i.setupField(field)
		if err != nil {
			log.Println(errors.Wrapf(err, "setting up field '%s'", fieldName)) // TODO make AddBit/AddValue return err?
			return
		}
		c = i.recordChans[fieldName]
	} else {
		i.lock.RUnlock()
	}
	c <- gopilosa.Column{
		RowID: uint64Cast(row), ColumnID: uint64Cast(col),
		RowKey: stringCast(row), ColumnKey: stringCast(col),
		Timestamp: ts}
}

// AddValue adds a value to be imported to Pilosa.
func (i *Index) AddValue(fieldName string, col uint64OrString, val int64) {
	var c chanRecordIterator
	var ok bool

	if !validUint64OrString(col) {
		panic(fmt.Sprintf("a %T was passed, must be eithe uint64 or string", col))
	}

	i.lock.RLock()
	if c, ok = i.recordChans[fieldName]; !ok {
		i.lock.RUnlock()
		i.lock.Lock()
		defer i.lock.Unlock()
		field := i.index.Field(fieldName, gopilosa.OptFieldTypeInt(0, 1<<31-1))
		err := i.setupField(field)
		if err != nil {
			log.Println(errors.Wrap(err, "setting up field"))
			return
		}
		c = i.recordChans[fieldName]
	} else {
		i.lock.RUnlock()
	}
	c <- gopilosa.FieldValue{ColumnID: uint64Cast(col), ColumnKey: stringCast(col), Value: val}
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
	return index.Field(name, gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, size))
}

func NewIntField(index *gopilosa.Index, name string, min, max int64) *gopilosa.Field {
	return index.Field(name, gopilosa.OptFieldTypeInt(min, max))
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
			err := i.client.ImportField(fram, cbi, gopilosa.OptImportBatchSize(int(i.batchSize)))
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", fieldName))
			}
		}(field, i.recordChans[fieldName])
	}
	return nil
}

// SetupPilosa returns a new Indexer after creating the given fields and starting importers.
func SetupPilosa(hosts []string, indexName string, schema *gopilosa.Schema, batchsize uint) (Indexer, error) {
	if schema == nil {
		schema = gopilosa.NewSchema()
	}
	indexer := newIndex()
	indexer.batchSize = batchsize
	client, err := gopilosa.NewClient(hosts,
		gopilosa.OptClientSocketTimeout(time.Minute*60),
		gopilosa.OptClientConnectTimeout(time.Second*60))
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa cluster client")
	}
	indexer.client = client
	indexer.index = schema.Index(indexName)
	err = client.SyncSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "synchronizing schema")
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
