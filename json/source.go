package json

import (
	"encoding/json"
	"io"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
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

type rawSourceSource struct {
	rs pdk.RawSource

	s *Source
}

func NewSourceFromRawSource(rs pdk.RawSource) pdk.Source {
	return &rawSourceSource{rs: rs}
}

func (r *rawSourceSource) Record() (rec interface{}, err error) {
	if r.s == nil {
		reader, err := r.rs.NextReader()
		if err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "getting next reader")
		} else if err == io.EOF {
			return nil, err
		}
		r.s = NewSource(reader)
	}
	rec, err = r.s.Record()
	if err == io.EOF {
		r.s = nil
		return r.Record()
	} else if err != nil {
		return rec, err
	}
	return rec, err
}
