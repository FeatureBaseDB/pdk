package pdk

import (
	"time"

	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

var (
	idBucket  = []byte("idKey")
	valBucket = []byte("valKey")
)

// BoltTranslator is a Translator which stores the two way val/id mapping in boltdb
type BoltTranslator struct {
	db     *bolt.DB
	frames map[string]struct{}
}

func (bt *BoltTranslator) Close() error {
	return bt.db.Close()
}

func NewBoltTranslator(filename string, frames ...string) (bt *BoltTranslator, err error) { //TODO frames handling
	bt = &BoltTranslator{
		frames: make(map[string]struct{}),
	}
	bt.db, err = bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, errors.Wrapf(err, "opening db file '%v'", filename)
	}
	err = bt.db.Update(func(tx *bolt.Tx) error {
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
	bt.frames[frame] = struct{}{}
	return fib, fvb, nil
}

// Get returns the previously mapped to the monotonic id generated from GetID.
// For BoltTranslator, val will always be a byte slice.
func (bt *BoltTranslator) Get(frame string, id uint64) (val interface{}) {
	if _, ok := bt.frames[frame]; !ok {
		panic(errors.Errorf("can't Get() with unknown frame '%v'", frame))
	}
	err := bt.db.View(func(tx *bolt.Tx) error {
		ib := tx.Bucket(idBucket)
		fib := ib.Bucket([]byte(frame))
		idBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(idBytes, id)
		val = fib.Get(idBytes)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return val
}

// GetID maps val (which must be a string or byte slice) to a monotonic id
func (bt *BoltTranslator) GetID(frame string, val interface{}) (id uint64, err error) {
	// ensure frame existence
	if _, ok := bt.frames[frame]; !ok {
		err = bt.db.Update(func(tx *bolt.Tx) error {
			ib := tx.Bucket(idBucket)
			vb := tx.Bucket(valBucket)
			_, _, err = bt.addFrame(ib, vb, frame)
			return err
		})
		if err != nil {
			return 0, errors.Wrap(err, "adding frames in GetID")
		}
	}

	// check that val is of a supported type
	var bsval []byte
	switch tval := val.(type) {
	case string:
		bsval = []byte(tval)
	case []byte:
		bsval = tval
	default:
		return 0, errors.Errorf("val %v of type %T for frame %v not supported by BoltTranslator. Type must be string or []byte", tval, tval, frame)
	}

	// look up to see if this val is already mapped to an id
	var ret []byte
	err = bt.db.View(func(tx *bolt.Tx) error {
		vb := tx.Bucket(valBucket)
		fvb := vb.Bucket([]byte(frame))
		ret = fvb.Get(bsval)
		return nil
	})
	if ret != nil && len(ret) == 8 {
		return binary.LittleEndian.Uint64(ret), nil
	}

	// get new id, and map it in both directions
	err = bt.db.Update(func(tx *bolt.Tx) error {
		fib := tx.Bucket(idBucket).Bucket([]byte(frame))
		fvb := tx.Bucket(valBucket).Bucket([]byte(frame))

		id, _ = fib.NextSequence()
		keybytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keybytes, id)
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
