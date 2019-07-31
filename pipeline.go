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
	"io"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
)

// NamedReadCloser adds the ability to associate a name and other
// arbitrary metdata with a ReadCloser. This is used by RawSource for
// e.g. a directory reading source to associate the file name of each
// reader it returns. This might be needed to generate the identifier
// for each record within the file, or to help build a recovery
// mechanism in the case of process crash (tracking which files have
// and have not been read yet).
type NamedReadCloser interface {
	io.ReadCloser
	Name() string
	Meta() map[string]interface{} // not sure if we need this
}

// RawSource is the interface for getting actual raw data from a data
// source. The returned ReadCloser may have multiple individual
// records—how it's interpreted will depend on the adapter which
// converts the bytes read into Go objects. As an example, a RawSource
// for S3 might return each object in an S3 bucket as a reader. Each
// object might be a CSV file with many individual records which a
// separate CSV parser could then turn into a
// pdk.Source. Alternatively, an optimized implementation might read
// the CSV and convert it directly to PilosaRecord objects.
type RawSource interface {
	NextReader() (NamedReadCloser, error)
}

// Source is the interface for getting raw data one record at a time.
// Implementations of Source should be thread safe.
type Source interface {
	Record() (interface{}, error)
}

// Peeker is an interface for peeking ahead at the next record
// to be returned by Source.Record().
type Peeker interface {
	Peek() (interface{}, error)
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
	AddColumn(field string, col, row uint64OrString)
	AddColumnTimestamp(field string, col, row uint64OrString, ts time.Time)
	AddValue(field string, col uint64OrString, val int64)
	// AddRowAttr(field string, row uint64, key string, value AttrVal)
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
