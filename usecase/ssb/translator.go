package ssb

import (
	"fmt"
	"log"

	"github.com/pilosa/pdk"
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

func (t *Translator) Get(frame string, id uint64) interface{} {
	switch frame {
	case "c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1":
		return t.lt.Get(frame, id)
	case "lo_month":
		return monthsSlice[id]
	case "lo_weeknum", "lo_year":
		return id
	default:
		log.Printf("Unimplemented in ssb.Translator.Get frame: %v, id: %v", frame, id)
		return nil
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

func (t *Translator) GetID(frame string, val interface{}) (uint64, error) {
	switch frame {
	case "c_city", "c_nation", "c_region", "s_city", "s_nation", "s_region", "p_mfgr", "p_category", "p_brand1":
		return t.lt.GetID(frame, val)
	case "lo_month":
		valbytes := val.([]byte)
		m, ok := months[string(valbytes)]
		if !ok {
			return 0, fmt.Errorf("Val '%s' is not a month", val)
		}
		return m, nil
	case "lo_weeknum":
		val8, ok := val.(uint8)
		if !ok {
			return 0, fmt.Errorf("Val '%v' is not a valid weeknum (not uint8)", val)
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
