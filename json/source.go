package json

import (
	"encoding/json"
	"io"
)

// Source is a pdk.Source for reading json data.
type Source struct {
	dec *json.Decoder
}

// NewSource gets a new json source which will decode from the given reader.
func NewSource(r io.Reader) *Source {
	return &Source{
		dec: json.NewDecoder(r),
	}
}

// Record implements pdk.Source. It returns the next json object that can be
// decoded from the reader. It is guaranteed to return a map[string]interface{}
// if there is no error.
func (s *Source) Record() (rec interface{}, err error) {
	var res map[string]interface{}
	err = s.dec.Decode(&res)
	if err != nil {
		return nil, err
	}
	return res, nil
}
