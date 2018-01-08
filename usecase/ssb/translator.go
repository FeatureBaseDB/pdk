package ssb

import (
	"fmt"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

type Translator struct {
	lt *pdk.LevelTranslator
}

func NewTranslator(storedir string) (*Translator, error) {
	lt, err := pdk.NewLevelTranslator(storedir, []string{"c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1"}...)
	if err != nil {
		return nil, err
	}
	return &Translator{
		lt: lt,
	}, nil
}

func (t *Translator) Get(frame string, id uint64) (interface{}, error) {
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

func (t *Translator) GetID(frame string, val interface{}) (uint64, error) {
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
