package geohash_test

import (
	"testing"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/geohash"
	"github.com/pkg/errors"
)

func TestTransform(t *testing.T) {
	tests := []struct {
		name        string
		transformer *geohash.Transformer
		entity      *pdk.Entity
		expErr      error
	}{
		{
			name: "simple",
			transformer: &geohash.Transformer{
				Precision:  6,
				LatPath:    []string{"latitude"},
				LonPath:    []string{"longitude"},
				ResultPath: []string{"geohash"},
			},
			entity: &pdk.Entity{
				Objects: map[pdk.Property]pdk.Object{
					"latitude":  pdk.F64(31.1),
					"longitude": pdk.F64(42.2),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.transformer.Transform(test.entity)
			if errors.Cause(err) != test.expErr {
				t.Fatalf("got %v, expected %v", err, test.expErr)
			}
			if err != nil {
				return
			}
			hash, err := test.entity.Literal(test.transformer.ResultPath...)
			if err != nil {
				t.Fatalf("should be a value at ResultPath, but: %v", err)
			}

			hashS, ok := hash.(pdk.S)
			if !ok {
				t.Fatalf("hash is not a string")
			}
			if len(hashS) != test.transformer.Precision {
				t.Fatalf("unexpected length of hashVal %v", hashS)
			}
		})
	}
}
