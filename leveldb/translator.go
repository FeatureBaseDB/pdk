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

package leveldb

import (
	"encoding/binary"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"

	"os"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var _ pdk.Translator = &Translator{}

// Translator is a pdk.Translator which stores the two way val/id mapping in
// leveldb.
type Translator struct {
	lock    sync.RWMutex
	dirname string
	fields  map[string]*FieldTranslator
}

// FieldTranslator is a pdk.FieldTranslator which uses leveldb.
type FieldTranslator struct {
	lock   valueLocker
	idMap  *leveldb.DB
	valMap *leveldb.DB
	curID  *uint64
}

type errorList []error

func (errs errorList) Error() string {
	errstrings := make([]string, len(errs))
	for i, err := range errs {
		errstrings[i] = err.Error()
	}
	return strings.Join(errstrings, "; ")
}

// Close closes all of the underlying leveldb instances.
func (lt *Translator) Close() error {
	errs := make(errorList, 0)
	for f, lft := range lt.fields {
		err := lft.Close()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "field : %v", f))
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// Close closes the two leveldbs used by the FieldTranslator.
func (lft *FieldTranslator) Close() error {
	errs := make(errorList, 0)
	err := lft.idMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing idMap"))
	}
	err = lft.valMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing valMap"))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// getFieldTranslator retrieves or creates a FieldTranslator for the given field.
func (lt *Translator) getFieldTranslator(field string) (*FieldTranslator, error) {
	lt.lock.RLock()
	if tr, ok := lt.fields[field]; ok {
		lt.lock.RUnlock()
		return tr, nil
	}
	lt.lock.RUnlock()
	lt.lock.Lock()
	defer lt.lock.Unlock()
	if tr, ok := lt.fields[field]; ok {
		return tr, nil
	}
	lft, err := NewFieldTranslator(lt.dirname, field)
	if err != nil {
		return nil, errors.Wrap(err, "creating new FieldTranslator")
	}
	lt.fields[field] = lft
	return lft, nil
}

// NewFieldTranslator creates a new FieldTranslator which uses LevelDB as
// backing storage.
func NewFieldTranslator(dirname string, field string) (*FieldTranslator, error) {
	err := os.MkdirAll(dirname, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "making directory")
	}
	var initialID uint64
	mdbs := &FieldTranslator{
		curID: &initialID,
		lock:  newBucketVLock(),
	}
	mdbs.idMap, err = leveldb.OpenFile(dirname+"/"+field+"-id", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+field+"-id")
	}
	mdbs.valMap, err = leveldb.OpenFile(dirname+"/"+field+"-val", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+field+"-val")
	}
	return mdbs, nil
}

// NewTranslator gets a new Translator.
func NewTranslator(dirname string, fields ...string) (lt *Translator, err error) {
	lt = &Translator{
		dirname: dirname,
		fields:  make(map[string]*FieldTranslator),
	}
	for _, field := range fields {
		lft, err := NewFieldTranslator(dirname, field)
		if err != nil {
			return nil, errors.Wrap(err, "making FieldTranslator")
		}
		lt.fields[field] = lft
	}
	return lt, err
}

// Get returns the value mapped to the given id in the given field.
func (lt *Translator) Get(field string, id uint64) (val interface{}, err error) {
	lft, err := lt.getFieldTranslator(field)
	if err != nil {
		return nil, errors.Wrap(err, "getting field translator")
	}
	val, err = lft.Get(id)
	return val, err
}

// Get returns the value mapped to the given id.
func (lft *FieldTranslator) Get(id uint64) (val interface{}, err error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	data, err := lft.idMap.Get(idBytes, nil)
	if err != nil {
		return nil, errors.Wrap(err, "fetching from idMap")
	}
	return pdk.FromBytes(data), nil
}

// GetID returns the integer id associated with the given value in the given field.
// It allocates a new ID if the value is not found.
func (lt *Translator) GetID(field string, val interface{}) (id uint64, err error) {
	lft, err := lt.getFieldTranslator(field)
	if err != nil {
		return 0, errors.Wrap(err, "getting field translator")
	}
	return lft.GetID(val)
}

// GetID returns the integer id associated with the given value. It allocates a
// new ID if the value is not found.
func (lft *FieldTranslator) GetID(val interface{}) (id uint64, err error) {
	var vall pdk.Literal
	switch valt := val.(type) {
	case []byte:
		vall = pdk.S(valt)
	case string:
		vall = pdk.S(valt)
	default:
		var ok bool
		if vall, ok = val.(pdk.Literal); !ok {
			return 0, errors.Errorf("val needs to be string, byte slice, or Literal, but is type: %T, val: '%v'", val, val)
		}
	}
	valBytes := pdk.ToBytes(vall)
	var data []byte

	// if you're expecting most of the mapping to already be done, this would be faster
	data, err = lft.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}

	// else, val not found
	lft.lock.Lock(valBytes)
	defer lft.lock.Unlock(valBytes)
	// re-read after locking
	data, err = lft.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}

	idBytes := make([]byte, 8)
	new := atomic.AddUint64(lft.curID, 1)
	binary.BigEndian.PutUint64(idBytes, new-1)
	err = lft.idMap.Put(idBytes, valBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into idmap")
	}
	err = lft.valMap.Put(valBytes, idBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into valmap")
	}
	return new - 1, nil
}

type valueLocker interface {
	Lock(val []byte)
	Unlock(val []byte)
}

type bucketVLock struct {
	ms []sync.Mutex
}

func newBucketVLock() bucketVLock {
	return bucketVLock{
		ms: make([]sync.Mutex, 1000),
	}
}

func (b bucketVLock) Lock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Lock()
}

func (b bucketVLock) Unlock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Unlock()
}
