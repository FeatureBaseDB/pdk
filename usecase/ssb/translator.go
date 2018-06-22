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

package ssb

import (
	"fmt"

	"github.com/pilosa/pdk/leveldb"
	"github.com/pkg/errors"
)

type translator struct {
	lt *leveldb.Translator
}

func newTranslator(storedir string) (*translator, error) {
	lt, err := leveldb.NewTranslator(storedir, []string{"c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1"}...)
	if err != nil {
		return nil, err
	}
	return &translator{
		lt: lt,
	}, nil
}

func (t *translator) Get(frame string, id uint64) (interface{}, error) {
	switch frame {
	case "c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1":
		val, err := t.lt.Get(frame, id)
		if err != nil {
			return nil, errors.Wrap(err, "string from level translator")
		}
		return string(val.([]byte)), nil
	case "lo_month":
		return monthsSlice[id], nil
	case "lo_weeknum", "lo_year", "lo_quantity_b", "lo_discount_b":
		return id, nil
	default:
		return nil, errors.Errorf("Unimplemented in ssb.Translator.Get frame: %v, id: %v", frame, id)
	}
}

func (t *translator) GetID(frame string, val interface{}) (uint64, error) {
	switch frame {
	case "c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1":
		return t.lt.GetID(frame, []byte(val.(string)))
	case "lo_month":
		valstring := val.(string)
		m, ok := months[valstring]
		if !ok {
			return 0, fmt.Errorf("Val '%s' is not a month", val)
		}
		return m, nil
	case "lo_weeknum", "lo_quantity_b", "lo_discount_b":
		val8, ok := val.(uint8)
		if !ok {
			return 0, fmt.Errorf("Val '%v' is not a valid weeknum/quantity/discount (not uint8)", val)
		}
		return uint64(val8), nil
	case "lo_year":
		val16, ok := val.(uint16)
		if !ok {
			return 0, fmt.Errorf("Val '%v' is not a valid year (not uint16)", val)
		}
		return uint64(val16), nil
	default:
		return 0, fmt.Errorf("Unimplemented in ssb.Translator.GetID frame: %v, val: %v", frame, val)
	}
}

var months = map[string]uint64{
	"January":   0,
	"February":  1,
	"March":     2,
	"April":     3,
	"May":       4,
	"June":      5,
	"July":      6,
	"Augest":    7,
	"September": 8,
	"Octorber":  9,
	"November":  10,
	"December":  11,
}

var monthsSlice = []string{
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"Augest",
	"September",
	"Octorber",
	"November",
	"December",
}
