package enterprise

import (
	"fmt"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Importer provides a convenient interface for a single goroutine importing
// entities to Pilosa Enterprise. It is NOT threadsafe, and each goroutine
// should maintain its own Importer (though they may all reference the same
// Indexer and Framer - provided their implementations are threadsafe).
type Importer struct {
	framer  pdk.Framer
	indexer Indexer
	frames  map[string]ChanBitIterator
	fields  map[string]map[string]ChanValIterator
}

type ImporterOption func(*Importer)

func WithFramer(framer pdk.Framer) ImporterOption {
	return func(imp *Importer) {
		imp.framer = framer
	}
}

func WithIndexer(indexer Indexer) ImporterOption {
	return func(imp *Importer) {
		imp.indexer = indexer
	}
}

func NewImporter(options ...ImporterOption) *Importer {
	imp := &Importer{
		frames: make(map[string]ChanBitIterator),
		fields: make(map[string]map[string]ChanValIterator),
	}
	for _, opt := range options {
		opt(imp)
	}
	return imp
}

func (imp *Importer) Import(ent *pdk.Entity) error {
	return pdk.Walk(ent, func(path []string, l pdk.Literal) error {
		if ltime, ok := l.(pdk.Time); ok {
			l = pdk.U32(time.Time(ltime).Unix())
		}
		if lfloat, ok := l.(pdk.F64); ok {
			l = pdk.I64(lfloat * 1000)
		}
		if lfloat, ok := l.(pdk.F32); ok {
			l = pdk.I64(lfloat * 1000)
		}
		switch lt := l.(type) {
		case pdk.S:
			name, err := imp.framer.Frame(path)
			if err != nil {
				return errors.Wrapf(err, "couldn't frame %v", path)
			}
			iter, err := imp.getFrame(name)
			if err != nil {
				return errors.Wrapf(err, "couldn't get frame: %v", name)
			}
			iter <- gopilosa.Bit{ColumnKey: string(ent.Subject), RowKey: string(lt)}
		case pdk.U8, pdk.U16, pdk.U32, pdk.U64, pdk.U, pdk.I8, pdk.I16, pdk.I32, pdk.I64, pdk.I:
			name, field, err := imp.framer.Field(path)
			if err != nil {
				return errors.Wrapf(err, "couldn't Field %v", path)
			}
			iter, err := imp.getField(name, field, getRangeOption(lt))
			if err != nil {
				return errors.Wrapf(err, "couldn't get field: %v", name)
			}
			iter <- gopilosa.FieldValue{ColumnKey: string(ent.Subject), Value: pdk.Int64ize(lt)}
		case pdk.Time:
			return errors.New("pdk.Time unimplemented")
		case pdk.F64, pdk.F32:
			return errors.New("pdk.F64 and pdk.F32 unimplemented")
		default:
			panic(fmt.Sprintf("unhandled literal: %v type %T", lt, lt))
		}
		return nil
	})
}

func getRangeOption(val pdk.Literal) FieldOpt {
	switch val.(type) {
	case pdk.U8:
		return OptRange(0, 0xFF)
	case pdk.U16:
		return OptRange(0, 0xFFFF)
	case pdk.U32:
		return OptRange(0, 0xFFFFFFFF)
	case pdk.U64, pdk.U:
		return OptRange(0, 0x7FFFFFFFFFFFFFFF) // can't use the highest unsigned uint64s because arg is an int
	case pdk.I8:
		return OptRange(-128, 0x7F)
	case pdk.I16:
		return OptRange(-32768, 0x7FFF)
	case pdk.I32:
		return OptRange(-2147483648, 0x7FFFFFFF)
	case pdk.I64, pdk.I:
		return OptRange(-9223372036854775808, 0x7FFFFFFFFFFFFFFF)
	default:
		panic(fmt.Sprintf("RangeOption called with unknown val %#v type %T", val, val))
	}
}

func (imp *Importer) getFrame(frame string, options ...FrameOpt) (ChanBitIterator, error) {
	if iter, ok := imp.frames[frame]; ok {
		return iter, nil
	}
	iter, err := imp.indexer.Frame(frame, options...)
	if err != nil {
		return nil, err
	}
	imp.frames[frame] = iter
	return iter, nil
}

func (imp *Importer) getField(frame, field string, options ...FieldOpt) (ChanValIterator, error) {
	fmap, ok := imp.fields[frame]
	if !ok {
		fmap = make(map[string]ChanValIterator)
		imp.fields[frame] = fmap
	}
	iter, ok := fmap[field]
	var err error
	if !ok {
		iter, err = imp.indexer.Field(frame, field, options...)
		if err != nil {
			return nil, errors.Wrap(err, "getting field iter")
		}
		fmap[field] = iter
	}
	return iter, nil
}
