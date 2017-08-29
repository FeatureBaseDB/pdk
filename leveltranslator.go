package pdk

import (
	"encoding/binary"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"

	"os"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelTranslator is a Translator which stores the two way val/id mapping in
// leveldb.
type LevelTranslator struct {
	fmu    sync.RWMutex
	frames map[string]mapDBs
}

type mapDBs struct {
	lock   ValueLocker
	idMap  *leveldb.DB
	valMap *leveldb.DB
	curID  *uint64
}

type Errors []error

func (errs Errors) Error() string {
	errstrings := make([]string, len(errs))
	for i, err := range errs {
		errstrings[i] = err.Error()
	}
	return strings.Join(errstrings, "; ")
}

func (lt *LevelTranslator) Close() error {
	errs := make(Errors, 0)
	for f, dbs := range lt.frames {
		err := dbs.idMap.Close()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "closing idMap for frame: %v", f))
		}
		err = dbs.valMap.Close()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "closing valMap for frame: %v", f))
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func NewLevelTranslator(dirname string, frames ...string) (lt *LevelTranslator, err error) {
	lt = &LevelTranslator{
		frames: make(map[string]mapDBs),
	}
	err = os.MkdirAll(dirname, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "making directory")
	}
	for _, frame := range frames {
		var initialID uint64 = 0
		mdbs := mapDBs{
			curID: &initialID,
			lock:  NewBucketVLock(),
		}
		mdbs.idMap, err = leveldb.OpenFile(dirname+"/"+frame+"-id", &opt.Options{})
		if err != nil {
			return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+frame+"-id")
		}
		mdbs.valMap, err = leveldb.OpenFile(dirname+"/"+frame+"-val", &opt.Options{})
		if err != nil {
			return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+frame+"-val")
		}
		lt.frames[frame] = mdbs
	}
	return lt, err
}

func (lt *LevelTranslator) Get(frame string, id uint64) (val interface{}) {
	var dbs mapDBs
	var ok bool
	if dbs, ok = lt.frames[frame]; !ok {
		panic(errors.Errorf("frame %v not found in level translator", frame))
	}
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	data, err := dbs.idMap.Get(idBytes, nil)
	if err != nil {
		panic(err)
	}
	return data
}

func (lt *LevelTranslator) GetID(frame string, val interface{}) (id uint64, err error) {
	var dbs mapDBs
	var ok bool
	if dbs, ok = lt.frames[frame]; !ok {
		return 0, errors.Errorf("frame %v not found in level translator", frame)
	}
	var valBytes []byte
	switch valt := val.(type) {
	case []byte:
		valBytes = valt
	case string:
		valBytes = []byte(valt)
	default:
		return 0, errors.Errorf("val needs to be of type []byte, but is type: %T, val: '%v'", val, val)
	}
	var data []byte

	// data, err = dbs.valMap.Get(valBytes, &opt.ReadOptions{})
	// if err != nil && err != leveldb.ErrNotFound {
	// 	return 0, errors.Wrap(err, "trying to read value map")
	// } else if err == nil {
	// 	return binary.BigEndian.Uint64(data), nil
	// }

	// else, val not found
	dbs.lock.Lock(valBytes)
	defer dbs.lock.Unlock(valBytes)
	// re-read after locking
	data, err = dbs.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}

	idBytes := make([]byte, 8)
	new := atomic.AddUint64(dbs.curID, 1)
	binary.BigEndian.PutUint64(idBytes, new-1)
	err = dbs.idMap.Put(idBytes, valBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into idmap")
	}
	err = dbs.valMap.Put(valBytes, idBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into valmap")
	}
	return new - 1, nil
}

type ValueLocker interface {
	Lock(val []byte)
	Unlock(val []byte)
}

type SingleVLock struct {
	m *sync.Mutex
}

func NewSingleVLock() SingleVLock {
	return SingleVLock{
		m: &sync.Mutex{},
	}
}

func (s SingleVLock) Lock(val []byte) {
	s.m.Lock()
}

func (s SingleVLock) Unlock(val []byte) {
	s.m.Unlock()
}

type BucketVLock struct {
	ms []sync.Mutex
}

func NewBucketVLock() BucketVLock {
	return BucketVLock{
		ms: make([]sync.Mutex, 1000),
	}
}

func (b BucketVLock) Lock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Lock()
}

func (b BucketVLock) Unlock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Unlock()
}
