// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
// (frame, id) combination for sending to Pilosa as a SetBit query
type BitMapper struct {
	Frame   string
	Mapper  Mapper
	Parsers []Parser
	Fields  []int
}

// AttrMapper is a struct for mapping some set of data fields to a
// value for sending to Pilosa as a SetColumnAttrs query
type AttrMapper struct {
	Mapper  Mapper
	Parsers []Parser
	Fields  []int
}
