package pdk

// Source is the interface for getting raw data one record at a time.
// Implementations of Source should be thread safe.
type Source interface {
	Record() ([]byte, error)
}
<<<<<<< 98bd967c38cd14fded4418ca92e2c6b823734583

// Parser is the interface for turning raw records from Source into Go objects.
// Implementations of Parser should be thread safe. The current naming is a
// temporary workaround until the previous Parser interface can be deprecated.
type Parrrser interface {
	Parse(record []byte) (interface{}, error)
}

// Mapper is the interface for taking parsed records from the Parser and
// figuring out what bits and values to set in Pilosa. Mappers usually have a
// Translator and a Nexter for converting arbitrary values to monotonic integer
// ids and generating column ids respectively.
type Mapppper interface {
	Map(parsedRecord interface{}) (PilosaRecord, error)
}

// PilosaRecord represents a number of set bits and values in a single Column
// in Pilosa.
type PilosaRecord struct {
	Col  uint64
	Bits []SetBit
	Vals []Val
}

// SetBit represents a bit to set in Pilosa sans column id (which is held by the
// PilosaRecord containg the SetBit).
type SetBit struct {
	Frame string
	Row   uint64
	Time  time.Time
}

// Val represents a BSI value to set in a Pilosa field sans column id (which is
// held by the PilosaRecord containg the Val).
type Val struct {
	Frame string
	Field string
	Value int64
}
=======
>>>>>>> monster commit - avro registry stuff, generic translator, mapper
