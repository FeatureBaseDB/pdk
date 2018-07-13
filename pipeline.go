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
	AddColumn(field string, col, row uint64)
	AddColumnTimestamp(field string, col, row uint64, ts time.Time)
	AddValue(field string, col uint64, val int64)
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
