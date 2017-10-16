package pdk

// Parser is the interface for turning raw records from Source into Go objects.
// Implementations of Parser should be thread safe. The current naming is a
// temporary workaround until the previous Parser interface can be deprecated.
type Parrrser interface {
	Parse(record []byte) (interface{}, error)
}
