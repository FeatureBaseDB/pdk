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

// Package boltdb provides a pdk.Translator implementation using boltdb. BoltDB
// is great, but this package is not particularly well-used or tested, and it is
// recommended that one use the leveldb translator instead which has better
// write performance.
package boltdb

import (
	"sync"
	"time"

	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

var (
	idBucket  = []byte("idKey")
	valBucket = []byte("valKey")
)

// Translator is a pdk.Translator which stores the two way val/id mapping in
// boltdb. It only accepts string values to map.
type Translator struct {
	Db     *bolt.DB
	fmu    sync.RWMutex
	fields map[string]struct{}
}

// Close syncs and closes the underlying boltdb.
func (bt *Translator) Close() error {
	err := bt.Db.Sync()
	if err != nil {
		return errors.Wrap(err, "syncing db")
	}
	return bt.Db.Close()
}

// NewTranslator gets a new Translator
func NewTranslator(filename string, fields ...string) (bt *Translator, err error) {
	bt = &Translator{
		fields: make(map[string]struct{}),
	}
	bt.Db, err = bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second, InitialMmapSize: 50000000, NoGrowSync: true})
	if err != nil {
		return nil, errors.Wrapf(err, "opening db file '%v'", filename)
	}
	bt.Db.MaxBatchDelay = 400 * time.Microsecond
	err = bt.Db.Update(func(tx *bolt.Tx) error {
		ib, err := tx.CreateBucketIfNotExists(idBucket)
		if err != nil {
			return errors.Wrap(err, "creating idKey bucket")
		}
		vb, err := tx.CreateBucketIfNotExists(valBucket)
		if err != nil {
			return errors.Wrap(err, "creating valKey bucket")
		}
		for _, field := range fields {
			_, _, err = bt.addField(ib, vb, field)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "ensuring bucket existence")
	}
	return bt, nil
}

func (bt *Translator) addField(ib, vb *bolt.Bucket, field string) (fib, fvb *bolt.Bucket, err error) {
	fib, err = ib.CreateBucketIfNotExists([]byte(field))
	if err != nil {
		return nil, nil, errors.Wrap(err, "adding "+field+" to id bucket")
	}
	fvb, err = vb.CreateBucketIfNotExists([]byte(field))
	if err != nil {
		return nil, nil, errors.Wrap(err, "adding "+field+" to id bucket")
	}
	bt.fmu.Lock()
	bt.fields[field] = struct{}{}
	bt.fmu.Unlock()

	return fib, fvb, nil
}

// Get returns the previously mapped value to the monotonic id generated from
// GetID. For BoltTranslator, val will always be a []byte.
func (bt *Translator) Get(field string, id uint64) (val interface{}) {
	bt.fmu.RLock()
	if _, ok := bt.fields[field]; !ok {
		panic(errors.Errorf("can't Get() with unknown field '%v'", field))
	}
	bt.fmu.RUnlock()
	err := bt.Db.View(func(tx *bolt.Tx) error {
		ib := tx.Bucket(idBucket)
		fib := ib.Bucket([]byte(field))
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, id)
		val = fib.Get(idBytes)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return val
}

// GetID maps val (which must be a byte slice) to a monotonic id.
func (bt *Translator) GetID(field string, val interface{}) (id uint64, err error) {
	// ensure field existence
	bt.fmu.RLock()
	_, ok := bt.fields[field]
	bt.fmu.RUnlock()
	if !ok {
		err = bt.Db.Update(func(tx *bolt.Tx) error {
			ib := tx.Bucket(idBucket)
			vb := tx.Bucket(valBucket)
			_, _, err := bt.addField(ib, vb, field)
			return err
		})
		if err != nil {
			return 0, errors.Wrap(err, "adding fields in GetID")
		}
	}

	// check that val is of a supported type
	bsval, ok := val.([]byte)
	if !ok {
		return 0, errors.Errorf("val %v of type %T for field %v not supported by BoltTranslator - must be a []byte. ", val, val, field)
	}

	// look up to see if this val is already mapped to an id
	var ret []byte
	err = bt.Db.View(func(tx *bolt.Tx) error {
		vb := tx.Bucket(valBucket)
		fvb := vb.Bucket([]byte(field))
		ret = fvb.Get(bsval)
		return nil
	})
	if len(ret) == 8 {
		return binary.BigEndian.Uint64(ret), nil
	}

	// get new id, and map it in both directions
	err = bt.Db.Batch(func(tx *bolt.Tx) error {
		fib := tx.Bucket(idBucket).Bucket([]byte(field))
		fvb := tx.Bucket(valBucket).Bucket([]byte(field))

		id, err = fib.NextSequence()
		if err != nil {
			return err
		}
		keybytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keybytes, id)
		err = fib.Put(keybytes, bsval)
		if err != nil {
			return errors.Wrap(err, "inserting into idKey bucket")
		}
		err = fvb.Put(bsval, keybytes)
		if err != nil {
			return errors.Wrap(err, "inserting into valKey bucket")
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// BulkAdd adds many values to a field at once, allocating ids.
func (bt *Translator) BulkAdd(field string, values [][]byte) error {
	var batchSize uint64 = 10000
	var batch uint64
	for batch*batchSize < uint64(len(values)) {
		err := bt.Db.Batch(func(tx *bolt.Tx) error {
			fib := tx.Bucket(idBucket).Bucket([]byte(field))
			fvb := tx.Bucket(valBucket).Bucket([]byte(field))

			for i := batch * batchSize; i < (batch+1)*batchSize && i < uint64(len(values)); i++ {
				idBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(idBytes, i)
				valBytes := values[i]
				err := fib.Put(idBytes, valBytes)
				if err != nil {
					return errors.Wrap(err, "putting into idKey bucket")
				}
				err = fvb.Put(valBytes, idBytes)
				if err != nil {
					return errors.Wrap(err, "putting into valKey bucket")
				}
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "inserting batch")
		}
		batch++
	}
	return nil
}
