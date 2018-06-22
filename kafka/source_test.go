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

package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"testing"

	"github.com/elodina/go-avro"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

func TestConfluentSource(t *testing.T) {
	regURL := StartFakeRegistry(t)
	source := NewConfluentSource()
	source.RegistryURL = regURL
	data := GetAvroEncodedValue(t)
	val := append([]byte{0, 0, 0, 0, 1}, data...)

	parsedRec, err := source.decodeAvroValueWithSchemaRegistry(val)
	if err != nil {
		t.Fatal(err)
	}

	if parsedRec.(map[string]interface{})["mysubthing"].(map[string]interface{})["subdub"] != 3.14 {
		t.Fatalf("parsed and original are different")
	}

}

var value = map[string]interface{}{
	"thing_string": "blah",
	"thing_int":    34,
	"mysubthing": map[string]interface{}{
		"com.pilosa.thing.SubThing": map[string]interface{}{
			"substring": map[string]interface{}{"string": "blahsub"},
			"subdub":    map[string]interface{}{"double": 3.14},
		},
	},
}

func GetAvroEncodedValue(t *testing.T) []byte {
	codec, err := goavro.NewCodec(schema1)
	if err != nil {
		t.Fatal(err)
	}

	data, err := codec.BinaryFromNative([]byte{}, value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestElodinaDecode(t *testing.T) {
	data := GetAvroEncodedValue(t)

	schema, err := avro.ParseSchema(schema1)
	if err != nil {
		t.Fatal(err)
	}

	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(schema)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(data)

	decodedRecord := avro.NewGenericRecord(schema)
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err = reader.Read(decodedRecord, decoder)
	if err != nil {
		t.Fatal(err)
	}

	gomap := decodedRecord.Map()
	if gomap["thing_int"].(int32) != 34 {
		t.Fatalf("unexpected decoded map: %v", gomap)
	}
}

func StartFakeRegistry(t *testing.T) string {
	server := &http.Server{Addr: ":0", Handler: http.HandlerFunc(RegistryHandler)}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatalf("starting fake registry listener: %v", err)
	}
	go func() {
		log.Printf("fake registry test server failed: %v", server.Serve(ln))
	}()
	return ln.Addr().String()
}

var schema1 string = `{
    "fields": [
        {
            "name": "thing_string",
            "type": "string"
        },
        {
            "name": "thing_int",
            "type": "int"
        },
        {
            "name": "mysubthing",
            "type": [
                "null",
                {
                    "fields": [
                       {
                            "name": "substring",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "subdub",
                            "type": [
                                "null",
                                "double"
                            ]
                        }
                    ],
                    "name": "SubThing",
                    "type": "record"
                }
            ]
        }
    ],
    "name": "Thing",
    "namespace": "com.pilosa.thing",
    "type": "record"
}`

func RegistryHandler(w http.ResponseWriter, r *http.Request) {
	var id int32
	_, err := fmt.Sscanf(r.URL.Path, "/schemas/ids/%d", &id)
	if err != nil {
		http.Error(w, errors.Wrap(err, "extracting id from path").Error(), http.StatusBadRequest)
		return
	}
	enc := json.NewEncoder(w)

	if id == 1 {
		err := enc.Encode(Schema{Schema: schema1, ID: 1})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, fmt.Sprintf("unknown id: %d", id), http.StatusNotFound)
		return
	}
}
