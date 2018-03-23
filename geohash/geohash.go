package geohash

import (
	"github.com/gansidui/geohash"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Transformer is a pdk.Transformer for geohashing locations to strings.
type Transformer struct {
	Precision  int
	LatPath    []string
	LonPath    []string
	ResultPath []string
}

// Transform hashes the latitude and longitude at the given paths and sets the
// resulting string at the result path on the Entity.
func (t *Transformer) Transform(e *pdk.Entity) error {
	latitude, err := e.F64(t.LatPath...)
	if err != nil {
		return errors.Wrap(err, "getting latidude")
	}
	longitude, err := e.F64(t.LonPath...)
	if err != nil {
		return errors.Wrap(err, "getting longitude")
	}
	hsh := geoHash(float64(latitude), float64(longitude), t.Precision)
	err = e.SetString(hsh, t.ResultPath...)
	return errors.Wrap(err, "setting result")
}

func geoHash(lat, lon float64, precision int) string {
	hash, _ := geohash.Encode(lat, lon, precision)
	return hash[:precision]
}
