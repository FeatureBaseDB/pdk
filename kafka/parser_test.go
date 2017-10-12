package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"testing"

	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

func TestParse(t *testing.T) {
	regURL := StartFakeRegistry(t)
	parser := NewAvroParserRegistry(regURL)
	codec, err := goavro.NewCodec(schema1)
	if err != nil {
		t.Fatal(err)
	}

	value := map[string]interface{}{
		"thing_string": "blah",
		"thing_int":    34,
		"mysubthing": map[string]interface{}{
			"com.pilosa.thing.SubThing": map[string]interface{}{
				"substring": map[string]interface{}{"string": "blahsub"},
				"subdub":    map[string]interface{}{"double": 3.14},
			},
		},
	}
	native := make(map[string]interface{})
	native["com.pilosa.thing.Thing"] = value

	data, err := codec.BinaryFromNative([]byte{}, native)
	if err != nil {
		t.Fatal(err)
	}
	rec := Record{
		Value: append([]byte{0, 0, 0, 0, 1}, data[1:]...),
	}
	recbytes, err := Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	parsedRec, err := parser.Parse(recbytes)
	if err != nil {
		t.Fatal(err)
	}

	if parsedRec.(map[string]interface{})["com.pilosa.thing.Thing"].(map[string]interface{})["mysubthing"].(map[string]interface{})["com.pilosa.thing.SubThing"].(map[string]interface{})["subdub"].(map[string]interface{})["double"] != native["com.pilosa.thing.Thing"].(map[string]interface{})["mysubthing"].(map[string]interface{})["com.pilosa.thing.SubThing"].(map[string]interface{})["subdub"].(map[string]interface{})["double"] {
		t.Fatalf("parsed and original are different")
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

var schema1 string = `[
{
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
}]`

func RegistryHandler(w http.ResponseWriter, r *http.Request) {
	var id int32
	_, err := fmt.Sscanf(r.URL.Path, "/schemas/ids/%d", &id)
	if err != nil {
		http.Error(w, errors.Wrap(err, "extracting id from path").Error(), http.StatusBadRequest)
		return
	}
	enc := json.NewEncoder(w)

	if id == 1 {
		err := enc.Encode(Schema{Schema: schema1, Id: 1})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, fmt.Sprintf("unknown id: %d", id), http.StatusNotFound)
		return
	}

}
