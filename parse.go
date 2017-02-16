package pdk

import (
	"strconv"
	"time"
)

// Parser represents a single method for parsing a string field to a value
type Parser interface {
	Parse(string) (interface{}, error)
}

// IntParser is a parser for integer types
type IntParser struct {
}

// FloatParser is a parser for float types
type FloatParser struct {
}

// StringParser is a parser for string types
type StringParser struct {
}

// TimeParser is a parser for timestamps
type TimeParser struct {
	Layout string
}

// IPParser is a parser for IP addresses
type IPParser struct {
}

// Parse parses an integer string to an int64 value
func (p IntParser) Parse(field string) (result interface{}, err error) {
	return strconv.ParseInt(field, 10, 64)
}

// Parse parses a float string to a float64 value
func (p FloatParser) Parse(field string) (result interface{}, err error) {
	return strconv.ParseFloat(field, 64)
}

// Parse is an identity parser for strings
func (p StringParser) Parse(field string) (result interface{}, err error) {
	return field, nil
}

// Parse parses a timestamp string to a time.Time value
func (p TimeParser) Parse(field string) (result interface{}, err error) {
	return time.Parse(p.Layout, field)
}

// Parse parses an IP string into a TODO
func (p IPParser) Parse(field string) (result interface{}, err error) {
	return field, nil
}

// BitMapper is a struct for mapping some set of data fields to a
// (frame, id) combination for sending to Pilsoa as a SetBit query
type BitMapper struct {
	Frame   string
	Mapper  Mapper
	Parsers []Parser
	Fields  []int
}

// AttrMapper is a struct for mapping some set of data fields to a
// value for sending to Pilosa as a SetProfileAttrs query
type AttrMapper struct {
	Mapper  Mapper
	Parsers []Parser
	Fields  []int
}
