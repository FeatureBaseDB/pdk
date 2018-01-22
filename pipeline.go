package pdk

import (
	gopilosa "github.com/pilosa/go-pilosa"
)

// Source is the interface for getting raw data one record at a time.
// Implementations of Source should be thread safe.
type Source interface {
	Record() (interface{}, error)
}

// Parser is the interface for turning raw records from Source into Go objects.
// Implementations of Parser should be thread safe. The current naming is a
// temporary workaround until the previous Parser interface can be deprecated.
type Parrrser interface {
	Parse(data interface{}) (*Entity, error)
}

// Mapper is the interface for taking parsed records from the Parser and
// figuring out what bits and values to set in Pilosa. Mappers usually have a
// Translator and a Nexter for converting arbitrary values to monotonic integer
// ids and generating column ids respectively.
type Mapppper interface {
	Map(record *Entity) (PilosaRecord, error)
}

// Indexer puts stuff into Pilosa.
type Indexer interface {
	AddBit(frame string, col uint64, row uint64)
	AddValue(frame, field string, col uint64, val int64)
	// AddRowAttr(frame string, row uint64, key string, value AttrVal)
	// AddColAttr(col uint64, key string, value AttrVal)
	Close() error
	Client() *gopilosa.Client
}
