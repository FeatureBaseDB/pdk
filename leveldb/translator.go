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

// Translator is a Translator which stores the two way val/id mapping in
// leveldb.
type Translator struct {
	lock    sync.RWMutex
	dirname string
	frames  map[string]*FrameTranslator
}

type FrameTranslator struct {
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

// Close closes all of the underlying leveldb instances.
func (lt *Translator) Close() error {
	errs := make(Errors, 0)
	for f, lft := range lt.frames {
		err := lft.Close()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "frame : %v", f))
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// Close closes the two leveldbs used by the FrameTranslator.
func (lft *FrameTranslator) Close() error {
	errs := make(Errors, 0)
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

// getFrameTranslator retrieves or creates a FrameTranslator for the given frame.
func (lt *Translator) getFrameTranslator(frame string) (*FrameTranslator, error) {
	lt.lock.RLock()
	if tr, ok := lt.frames[frame]; ok {
		lt.lock.RUnlock()
		return tr, nil
	}
	lt.lock.RUnlock()
	lt.lock.Lock()
	defer lt.lock.Unlock()
	if tr, ok := lt.frames[frame]; ok {
		return tr, nil
	}
	lft, err := NewFrameTranslator(lt.dirname, frame)
	if err != nil {
		return nil, errors.Wrap(err, "creating new FrameTranslator")
	}
	lt.frames[frame] = lft
	return lft, nil
}

// NewFrameTranslator creates a new FrameTranslator which uses LevelDB as
// backing storage.
func NewFrameTranslator(dirname string, frame string) (*FrameTranslator, error) {
	err := os.MkdirAll(dirname, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "making directory")
	}
	var initialID uint64 = 0
	mdbs := &FrameTranslator{
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
	return mdbs, nil
}

func NewTranslator(dirname string, frames ...string) (lt *Translator, err error) {
	lt = &Translator{
		dirname: dirname,
		frames:  make(map[string]*FrameTranslator),
	}
	for _, frame := range frames {
		lft, err := NewFrameTranslator(dirname, frame)
		if err != nil {
			return nil, errors.Wrap(err, "making FrameTranslator")
		}
		lt.frames[frame] = lft
	}
	return lt, err
}

func (lt *Translator) Get(frame string, id uint64) (val interface{}, err error) {
	lft, err := lt.getFrameTranslator(frame)
	if err != nil {
		return nil, errors.Wrap(err, "getting frame translator")
	}
	val, err = lft.Get(id)
	return val, err
}

func (lft *FrameTranslator) Get(id uint64) (val interface{}, err error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	data, err := lft.idMap.Get(idBytes, nil)
	if err != nil {
		return nil, errors.Wrap(err, "fetching from idMap")
	}
	return pdk.FromBytes(data), nil
}

func (lt *Translator) GetID(frame string, val interface{}) (id uint64, err error) {
	lft, err := lt.getFrameTranslator(frame)
	if err != nil {
		return 0, errors.Wrap(err, "getting frame translator")
	}
	return lft.GetID(val)
}

func (lft *FrameTranslator) GetID(val interface{}) (id uint64, err error) {
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
