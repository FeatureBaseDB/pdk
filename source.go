package pdk

import "time"

// Source is the interface for getting raw data one record at a time.
// Implementations of Source should be thread safe.
type Source interface {
	Record() ([]byte, error)
}

type Parrrser interface {
	Parse(record []byte) (interface{}, error)
}

type Mapppper interface {
	Map(parsedRecord interface{}) (PilosaRecord, error)
}

type PilosaRecord struct {
	Col  uint64
	Bits []SetBit
	Vals []Val
}

type SetBit struct {
	Frame string
	Row   uint64
	Time  time.Time
}

type Val struct {
	Frame string
	Field string
	Value uint64
}
