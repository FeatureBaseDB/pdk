package pdk

import (
	"fmt"
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

// BoltTranslator is a Translator which stores the two way val/id mapping in
// boltdb. It only accepts string values to map.
type BoltTranslator struct {
	Db     *bolt.DB
	fmu    sync.RWMutex
	frames map[string]struct{}
}

func (bt *BoltTranslator) Close() error {
	err := bt.Db.Sync()
	if err != nil {
		return errors.Wrap(err, "syncing db")
	}
	return bt.Db.Close()
}

func NewBoltTranslator(filename string, frames ...string) (bt *BoltTranslator, err error) { //TODO frames handling
	bt = &BoltTranslator{
		frames: make(map[string]struct{}),
	}
	bt.Db, err = bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second, InitialMmapSize: 500000000, NoGrowSync: true})
	if err != nil {
		return nil, errors.Wrapf(err, "opening db file '%v'", filename)
	}
	bt.Db.NoSync = true
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
		for _, frame := range frames {
			_, _, err = bt.addFrame(ib, vb, frame)
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

func (bt *BoltTranslator) addFrame(ib, vb *bolt.Bucket, frame string) (fib, fvb *bolt.Bucket, err error) {
	fib, err = ib.CreateBucketIfNotExists([]byte(frame))
	if err != nil {
		return nil, nil, errors.Wrap(err, "adding "+frame+" to id bucket")
	}
	fvb, err = vb.CreateBucketIfNotExists([]byte(frame))
	if err != nil {
		return nil, nil, errors.Wrap(err, "adding "+frame+" to id bucket")
	}
	bt.fmu.Lock()
	bt.frames[frame] = struct{}{}
	bt.fmu.Unlock()

	return fib, fvb, nil
}

// Get returns the previously mapped to the monotonic id generated from GetID.
// For BoltTranslator, val will always be a string.
func (bt *BoltTranslator) Get(frame string, id uint64) (val interface{}) {
	bt.fmu.RLock()
	if _, ok := bt.frames[frame]; !ok {
		panic(errors.Errorf("can't Get() with unknown frame '%v'", frame))
	}
	bt.fmu.RUnlock()
	err := bt.Db.View(func(tx *bolt.Tx) error {
		ib := tx.Bucket(idBucket)
		fib := ib.Bucket([]byte(frame))
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, id)
		val = fib.Get(idBytes)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return string(val.([]byte))
}

// GetID maps val (which must be a string) to a monotonic id.
func (bt *BoltTranslator) GetID(frame string, val interface{}) (id uint64, err error) {
	// ensure frame existence
	bt.fmu.RLock()
	if _, ok := bt.frames[frame]; !ok {
		bt.fmu.RUnlock()
		err = bt.Db.Update(func(tx *bolt.Tx) error {
			ib := tx.Bucket(idBucket)
			vb := tx.Bucket(valBucket)
			_, _, err := bt.addFrame(ib, vb, frame)
			return err
		})
		if err != nil {
			return 0, errors.Wrap(err, "adding frames in GetID")
		}
	} else {
		bt.fmu.RUnlock()
	}

	// check that val is of a supported type
	sval, ok := val.(string)
	if !ok {
		return 0, errors.Errorf("val %v of type %T for frame %v not supported by BoltTranslator - must be a string. ", val, val, frame)
	}
	bsval := []byte(sval)

	// look up to see if this val is already mapped to an id
	var ret []byte
	err = bt.Db.View(func(tx *bolt.Tx) error {
		vb := tx.Bucket(valBucket)
		fvb := vb.Bucket([]byte(frame))
		if fvb == nil {
			fmt.Println("valBucket - ", frame)
			err2 := vb.ForEach(func(name []byte, val []byte) error {
				fmt.Println("BUCKET!!", string(name), string(val))
				return nil
			})
			if err2 != nil {
				fmt.Println("fetching vb buckets: ", err2)
			}
		}
		ret = fvb.Get(bsval)
		return nil
	})
	if ret != nil && len(ret) == 8 {
		return binary.BigEndian.Uint64(ret), nil
	}

	// get new id, and map it in both directions
	err = bt.Db.Batch(func(tx *bolt.Tx) error {
		fib := tx.Bucket(idBucket).Bucket([]byte(frame))
		fvb := tx.Bucket(valBucket).Bucket([]byte(frame))

		id, _ = fib.NextSequence()
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

func (bt *BoltTranslator) BulkAdd(frame string, values []string) error {
	var batchSize uint64 = 10000
	var batch uint64 = 0
	for batch*batchSize < uint64(len(values)) {
		err := bt.Db.Batch(func(tx *bolt.Tx) error {
			fib := tx.Bucket(idBucket).Bucket([]byte(frame))
			fvb := tx.Bucket(valBucket).Bucket([]byte(frame))

			for i := batch * batchSize; i < (batch+1)*batchSize && i < uint64(len(values)); i++ {
				idBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(idBytes, i)
				valBytes := []byte(values[i])
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
