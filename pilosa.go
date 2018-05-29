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
	client *gopilosa.Client

	lock       sync.RWMutex
	index      *gopilosa.Index
	importWG   sync.WaitGroup
	bitChans   map[string]chanBitIterator
	fieldChans map[string]map[string]chanValIterator
	frames     map[string]struct{} // frames tracks which frames have been setup
}

func newIndex() *Index {
	return &Index{
		bitChans:   make(map[string]chanBitIterator),
		fieldChans: make(map[string]map[string]chanValIterator),
		frames:     make(map[string]struct{}),
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

	importOptions []gopilosa.ImportOption
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
func NewRankedFrameSpec(name string, size int, importOptions ...gopilosa.ImportOption) FrameSpec {
	fs := FrameSpec{
		Name:      name,
		CacheType: gopilosa.CacheTypeRanked,
		CacheSize: uint(size),

		importOptions: importOptions,
	}
	return fs
}

// NewFieldFrameSpec creates a frame which is dedicated to a single BSI field
// which will have the same name as the frame
func NewFieldFrameSpec(name string, min int, max int, importOptions ...gopilosa.ImportOption) FrameSpec {
	fs := FrameSpec{
		Name:      name,
		CacheType: gopilosa.CacheType(""),
		CacheSize: 0,
		Fields:    []FieldSpec{{Name: name, Min: min, Max: max}},

		importOptions: importOptions,
	}
	return fs
}

// setupFrame ensures the existence of a frame with the given configuration in
// Pilosa, and starts importers for the frame and any fields. It is not
// threadsafe - callers must hold i.lock.Lock() or guarantee that they have
// exclusive access to Index before calling.
func (i *Index) setupFrame(frame FrameSpec) error {
	// If this frame has already been set up, don't set it up again.
	if _, ok := i.frames[frame.Name]; ok {
		return nil
	}
	i.frames[frame.Name] = struct{}{}

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

		// Don't handle bits for a frame with fields.
		if len(frame.Fields) == 0 {
			i.bitChans[frame.Name] = newChanBitIterator()
			i.importWG.Add(1)
			go func(fram *gopilosa.Frame, cbi chanBitIterator) {
				defer i.importWG.Done()
				err := i.client.ImportFrame(fram, cbi, frame.importOptions...)
				if err != nil {
					log.Println(errors.Wrapf(err, "starting frame import for %v", frame.Name))
				}
			}(fram, i.bitChans[frame.Name])
		}
	} else {
		// TODO: Currently this is assuming that the frame already exists.
		// Should options be passed just in case? It seems odd that there
		// is effectively only a getOrCreateFrame in the client.
		fram, err = i.index.Frame(frame.Name, nil)
		if err != nil {
			return errors.Wrap(err, "getting frame: %v")
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
			err := i.client.ImportValueFrame(fram, field.Name, cvi, frame.importOptions...)
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
func SetupPilosa(hosts []string, index string, frames []FrameSpec) (Indexer, error) {
	indexer := newIndex()
	client, err := gopilosa.NewClient(hosts,
		gopilosa.OptClientSocketTimeout(time.Minute*60),
		gopilosa.OptClientConnectTimeout(time.Second*60))
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
