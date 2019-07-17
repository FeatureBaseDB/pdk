package datagen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

// KafkaTopic sets the kafka topic
var KafkaTopic = "testtopic"
var RestProxyURL = "localhost:8082"

// PostData generates data to be used by kafka's rest proxy
func PostData(restProxyURL string, kafkaTopic string) (response map[string]interface{}, err error) {
	postURL := fmt.Sprintf("http://%s/topics/%s", restProxyURL, kafkaTopic)
	data := struct {
		Schema  string  `json:"value_schema"`
		Records []Value `json:"records"`
	}{
		Schema:  schema,
		Records: []Value{{Value: map[string]interface{}{"com.pi.Stuff": GenRecord()}}},
	}
	dataBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "marshalling schema")
	}

	resp, err := http.Post(postURL, "application/vnd.kafka.avro.v2+json", bytes.NewBuffer(dataBytes))
	if err != nil {
		return nil, errors.Wrap(err, "posting schema")
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return nil, errors.Errorf("unexpected status posting data: %v, body: %s", resp.StatusCode, respBody)
	}

	respMap := make(map[string]interface{})
	err = json.Unmarshal(respBody, &respMap)
	if err != nil {
		return nil, errors.Wrap(err, "decoding post data response")
	}
	return respMap, nil
}

// Value is utilized in PostData function to create a Record
type Value struct {
	Value map[string]interface{} `json:"value"`
}

// GenRecord is utilized in PostData function to create a Record
func GenRecord() *Record {
	geo := GeoIP{
		TimeZone:     TimeZone(),
		Longitude:    Longitude(),
		Latitude:     Latitude(),
		CountryName:  CountryName(),
		DmaCode:      DmaCode(),
		City:         City(),
		Region:       Region(),
		MetroCode:    MetroCode(),
		PostalCode:   PostalCode(),
		AreaCode:     AreaCode(),
		RegionName:   RegionName(),
		CountryCode3: CountryCode3(),
		CountryCode:  CountryCode(),
	}
	return &Record{
		ABA:        ABA(),
		Db:         Db(),
		UserID:     UserID(),
		CustomerID: CustomerID(),
		GeoIP:      geo,
	}
}

// Db returns a db
func Db() string {
	return text(1, 6, true, true, true, true)
}

// UserID returns a user ID
func UserID() int {
	return rand.Intn(10000000) // 10 mil
}

// CustomerID returns a customer ID
func CustomerID() int {
	return rand.Intn(1000000) // 1 mil
}

// TimeZone returns a time zone
func TimeZone() string {
	idx := rand.Intn(len(tzs))
	return tzs[idx]
}

// Longitude returns a longitude
func Longitude() float64 {
	ran := rand.ExpFloat64() * 10.0
	for ran > 360.0 {
		ran = rand.ExpFloat64() * 10.0
	}
	return ran - 180.0
}

// Latitude returns a latitude
func Latitude() float64 {
	ran := rand.ExpFloat64() * 10.0
	for ran > 180.0 {
		ran = rand.ExpFloat64() * 5.0
	}
	return ran - 90.0
}

// CountryName returns a country name
func CountryName() string {
	base := text(1, 3, true, false, false, false)
	return base + base
}

// DmaCode returns a dma code
func DmaCode() int {
	return rand.Intn(100000)
}

// City returns a city
func City() string {
	base := text(1, 4, true, false, false, false)
	return base + base
}

// Region returns a region
func Region() string {
	base := text(1, 3, false, true, false, false)
	return base
}

// MetroCode returns a metro code
func MetroCode() int {
	return rand.Intn(10000)
}

// PostalCode returns a postal code
func PostalCode() string {
	return strconv.Itoa(rand.Intn(98999) + 10000)
}

// AreaCode returns a area code
func AreaCode() int {
	return rand.Intn(899) + 100
}

// RegionName returns a region name
func RegionName() string {
	return ""
}

// CountryCode3 returns a country code
func CountryCode3() string {
	return ""
}

// CountryCode returns a country code
func CountryCode() string {
	return text(3, 3, false, true, false, false)
}

// ABA returns a random 9 numeric digit string with about 27000 possible values.
func ABA() string {
	num := rand.Intn(27000) + 22213
	num2 := num/10 - 1213
	numstr := strconv.Itoa(num)
	num2str := strconv.Itoa(num2)
	numstrbytes := append([]byte(numstr), num2str[3], numstr[0], numstr[1], numstr[2])
	return string(numstrbytes)
}

var lowerLetters = []rune("abcdefghijklmnopqrstuvwxyz")
var upperLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var numeric = []rune("0123456789")
var specialChars = []rune(`!'@#$%^&*()_+-=[]{};:",./?`)
var hexDigits = []rune("0123456789abcdef")

func text(atLeast, atMost int, allowLower, allowUpper, allowNumeric, allowSpecial bool) string {
	allowedChars := []rune{}
	if allowLower {
		allowedChars = append(allowedChars, lowerLetters...)
	}
	if allowUpper {
		allowedChars = append(allowedChars, upperLetters...)
	}
	if allowNumeric {
		allowedChars = append(allowedChars, numeric...)
	}
	if allowSpecial {
		allowedChars = append(allowedChars, specialChars...)
	}

	result := []rune{}
	nTimes := rand.Intn(atMost-atLeast+1) + atLeast
	for i := 0; i < nTimes; i++ {
		result = append(result, allowedChars[rand.Intn(len(allowedChars))])
	}
	return string(result)
}

// DigitsN returns n digits as a string
func DigitsN(n int) string {
	digits := make([]rune, n)
	for i := 0; i < n; i++ {
		digits[i] = numeric[rand.Intn(len(numeric))]
	}
	return string(digits)
}

// Digits returns from 1 to 5 digits as a string
func Digits() string {
	return DigitsN(rand.Intn(5) + 1)
}

func hexDigitsStr(n int) string {
	var num []rune
	for i := 0; i < n; i++ {
		num = append(num, hexDigits[rand.Intn(len(hexDigits))])
	}
	return string(num)
}

// HexColor generates hex color name
func HexColor() string {
	return hexDigitsStr(6)
}

// HexColorShort generates short hex color name
func HexColorShort() string {
	return hexDigitsStr(3)
}

var tzs = []string{
	"ACDT", "ACST", "ACT", "ACT", "ACWST", "ADT", "AEDT", "AEST", "AFT", "AKDT",
	"AKST", "AMST", "AMT", "AMT", "ART", "AST", "AST", "AWST", "AZOST", "AZOT",
	"AZT", "BDT", "BIOT", "BIT", "BOT", "BRST", "BRT", "BST", "BST", "BST",
	"BTT", "CAT", "CCT", "CDT", "CDT", "CEST", "CET", "CHADT", "CHAST", "CHOT",
	"CHOST", "CHST", "CHUT", "CIST", "CIT", "CKT", "CLST", "CLT", "COST", "COT",
	"CST", "CST", "ACST", "ACDT", "CST", "CT", "CVT", "CWST", "CXT", "DAVT",
	"DDUT", "DFT", "EASST", "EAST", "EAT", "ECT", "ECT", "EDT", "AEDT", "EEST",
	"EET", "EGST", "EGT", "EIT", "EST", "AEST", "FET", "FJT", "FKST", "FKT",
	"FNT", "GALT", "GAMT", "GET", "GFT", "GILT", "GIT", "GMT", "GST", "GST",
	"GYT", "HDT", "HAEC", "HST", "HKT", "HMT", "HOVST", "HOVT", "ICT", "IDT",
	"IOT", "IRDT", "IRKT", "IRST", "IST", "IST", "IST", "JST", "KGT", "KOST",
	"KRAT", "KST", "LHST", "LHST", "LINT", "MAGT", "MART", "MAWT", "MDT", "MET",
	"MEST", "MHT", "MIST", "MIT", "MMT", "MSK", "MST", "MST", "MUT", "MVT",
	"MYT", "NCT", "NDT", "NFT", "NPT", "NST", "NT", "NUT", "NZDT", "NZST",
	"OMST", "ORAT", "PDT", "PET", "PETT", "PGT", "PHOT", "PHT", "PKT", "PMDT",
	"PMST", "PONT", "PST", "PST", "PYST", "PYT", "RET", "ROTT", "SAKT", "SAMT",
	"SAST", "SBT", "SCT", "SDT", "SGT", "SLST", "SRET", "SRT", "SST", "SST",
	"SYOT", "TAHT", "THA", "TFT", "TJT", "TKT", "TLT", "TMT", "TRT", "TOT",
	"TVT", "ULAST", "ULAT", "USZ1", "UTC", "UYST", "UYT", "UZT", "VET", "VLAT",
	"VOLT", "VOST", "VUT", "WAKT", "WAST", "WAT", "WEST", "WET", "WIT", "WST",
	"YAKT", "YEKT",
}

// Record is a struct used in PostData
type Record struct {
	ABA        string `json:"aba"`
	Db         string `json:"db"`
	UserID     int    `json:"user_id"`
	CustomerID int    `json:"customer_id"`
	Timestamp  string `json:"timestamp"`
	GeoIP      GeoIP  `json:"geoip"`
}

// GeoIP is a struct used in PostData
type GeoIP struct {
	TimeZone     string  `json:"time_zone"`
	Longitude    float64 `json:"longitude"`
	Latitude     float64 `json:"latitude"`
	CountryName  string  `json:"country_name"`
	DmaCode      int     `json:"dma_code"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	MetroCode    int     `json:"metro_code"`
	PostalCode   string  `json:"postal_code"`
	AreaCode     int     `json:"area_code"`
	RegionName   string  `json:"region_name"`
	CountryCode3 string  `json:"country_code3"`
	CountryCode  string  `json:"country_code"`
}

type stringThing struct {
	String string `json:"string"`
}

type doubleThing struct {
	Double float64 `json:"double"`
}

type intThing struct {
	Int int `json:"int"`
}

// avroJSONGeoIP exists because the JSON to avro converter in Confluent requires
// a somewhat wacky json encoding in order to differentiate between types in
// unions in the case where json would be ambiguous.
// https://stackoverflow.com/a/27499930/2088767
type avroJSONGeoIP struct {
	GeoIP ajgip `json:"com.pi.GeoIPResult"`
}

type ajgip struct {
	TimeZone     stringThing `json:"time_zone"`
	Longitude    doubleThing `json:"longitude"`
	Latitude     doubleThing `json:"latitude"`
	CountryName  stringThing `json:"country_name"`
	DmaCode      intThing    `json:"dma_code"`
	City         stringThing `json:"city"`
	Region       stringThing `json:"region"`
	MetroCode    intThing    `json:"metro_code"`
	PostalCode   stringThing `json:"postal_code"`
	AreaCode     intThing    `json:"area_code"`
	RegionName   stringThing `json:"region_name"`
	CountryCode3 stringThing `json:"country_code3"`
	CountryCode  stringThing `json:"country_code"`
}

// MarshalJSON returns a json Kafka can interpret
func (g GeoIP) MarshalJSON() ([]byte, error) {
	return json.Marshal(avroJSONGeoIP{
		GeoIP: ajgip{
			TimeZone:     stringThing{g.TimeZone},
			Longitude:    doubleThing{g.Longitude},
			Latitude:     doubleThing{g.Latitude},
			CountryName:  stringThing{g.CountryName},
			DmaCode:      intThing{g.DmaCode},
			City:         stringThing{g.City},
			Region:       stringThing{g.Region},
			MetroCode:    intThing{g.MetroCode},
			PostalCode:   stringThing{g.PostalCode},
			AreaCode:     intThing{g.AreaCode},
			RegionName:   stringThing{g.RegionName},
			CountryCode3: stringThing{g.CountryCode3},
			CountryCode:  stringThing{g.CountryCode},
		},
	})
}

var schema = `[{
    "fields": [
        {
            "name": "aba",
            "type": "string"
        },
        {
            "name": "db",
            "type": "string"
        },
        {
            "name": "user_id",
            "type": "int"
        },
        {
            "name": "customer_id",
            "type": "int"
        },
        {
            "name": "timestamp",
            "type": "string"
        },
        {
            "name": "geoip",
            "type": [
                "null",
                {
                    "name": "GeoIPResult",
                    "type": "record",
                    "fields": [
                       {
                            "name": "time_zone",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "longitude",
                            "type": [
                                "null",
                                "double"
                            ]
                        },
                        {
                            "name": "country_code3",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "country_name",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "dma_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        },
                        {
                            "name": "city",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "region",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "country_code",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "metro_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        },
                        {
                            "name": "latitude",
                            "type": [
                                "null",
                                "double"
                            ]
                        },
                        {
                            "name": "region_name",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "postal_code",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "area_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        }
                    ]
                }
            ]
        }
    ],
    "name": "Stuff",
    "namespace": "com.pi",
    "type": "record"
}]`
