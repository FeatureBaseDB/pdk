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

package geohash

import (
	"github.com/mmcloughlin/geohash"
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
	hash := geohash.EncodeWithPrecision(lat, lon, uint(precision))
	return hash[:precision]
}
