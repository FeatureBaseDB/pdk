package pdk

import (
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
)

// Source is the interface for getting raw data one record at a time.
// Implementations of Source should be thread safe.
type Source interface {
	Record() (interface{}, error)
}

// RecordParser is the interface for turning raw records from Source into Go
// objects. Implementations of Parser should be thread safe.
type RecordParser interface {
	Parse(data interface{}) (*Entity, error)
}

// RecordMapper is the interface for taking parsed records from the Parser and
// figuring out what bits and values to set in Pilosa. RecordMappers usually
// have a Translator and a Nexter for converting arbitrary values to monotonic
// integer ids and generating column ids respectively. Implementations should be
// thread safe.
type RecordMapper interface {
	Map(record *Entity) (PilosaRecord, error)
}

// Indexer puts stuff into Pilosa.
type Indexer interface {
	AddBit(frame string, col, row uint64)
	AddBitTimestamp(frame string, col, row uint64, ts time.Time)
	AddValue(field string, col uint64, val int64)
	// AddRowAttr(frame string, row uint64, key string, value AttrVal)
	// AddColAttr(col uint64, key string, value AttrVal)
	Close() error
	Client() *gopilosa.Client
}

// Transformer is an interface for something which performs an in-place
// transformation on an Entity. It might enrich the entity by adding new fields,
// delete existing fields that don't need to be indexed, or change fields.
type Transformer interface {
	Transform(e *Entity) error
}
