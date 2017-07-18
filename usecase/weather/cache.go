package weather

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type WeatherCache struct {
	jsonPath string
	data     map[string]HistoryRecord
}

func NewWeatherCache(path string) *WeatherCache {
	c := &WeatherCache{
		jsonPath: path,
		data:     make(map[string]HistoryRecord),
	}
	c.ReadAll()
	return c
}

func (c *WeatherCache) GetDailyRecord(day time.Time) DailyRecord {
	dayKey := fmt.Sprintf("%d%02d%02d", day.Year(), day.Month(), day.Day())
	return c.data[dayKey].DailyRecord[0]
}

func (c *WeatherCache) GetHourlyRecord(daytime time.Time) HourlyRecord {
	dayKey := fmt.Sprintf("%d%02d%02d", daytime.Year(), daytime.Month(), daytime.Day())
	hourly := c.data[dayKey].HourlyRecords
	hourKey := daytime.Hour()
	if hourKey >= len(hourly) {
		// this should be an error...
		// really should be interpolating the data anyway, at least nearest neighbor
		hourKey = len(hourly) - 1
	}
	return hourly[hourKey]
}

func (c *WeatherCache) ReadAll() error {

	dir, err := os.Open(c.jsonPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	for _, file := range files {
		filename := filepath.Base(file.Name())
		datestr := filename[10:18]
		fullRecord, _ := readWundergroundJson(c.jsonPath + "/" + filename)
		c.data[datestr] = fullRecord.History
	}

	return nil
}

func readWundergroundJson(filename string) (*RecordFile, error) {
	record := new(RecordFile)
	raw, _ := ioutil.ReadFile(filename)
	if err := json.Unmarshal(raw, &record); err != nil {
		return record, err
	}
	return record, nil
}

type RecordFile struct {
	History  HistoryRecord `json:"history"`
	Response ApiResponse   `json:"response"`
}

type ApiResponse struct {
	Version string `json:"version"`
	Tos     string `json:"termsofService"`
}

type HistoryRecord struct {
	DailyRecord   []DailyRecord  `json:"dailysummary"`
	HourlyRecords []HourlyRecord `json:"observations"`
	// Time          TimeRecord     `json:"date"`
}

type TimeRecord struct {
	Year int `json:"year,string"`
	Mon  int `json:"mon,string"`
	Mday int `json:"mday,string"`
	Hour int `json:"hour,string"`
	Min  int `json:"min,string"`
}

type DailyRecord struct {
	// Time TimeRecord `json:"date"`

	Rain    int `json:"rain,string"`
	Fog     int `json:"fog,string"`
	Snow    int `json:"snow,string"`
	Hail    int `json:"hail,string"`
	Thunder int `json:"thunder,string"`
	Tornado int `json:"tornado,string"`

	// Humidity float32 `json:"humidity,string"`  // sometimes ""
	Meantempi     float32 `json:"meantempi,string"`
	Meanpressurei float32 `json:"meanpressurei,string"`
	Precipi       float32 `json:"precipi,string"`
}

type HourlyRecord struct {
	// Time TimeRecord `json:"date"`

	Rain    int `json:"rain,string"`
	Fog     int `json:"fog,string"`
	Snow    int `json:"snow,string"`
	Hail    int `json:"hail,string"`
	Thunder int `json:"thunder,string"`
	Tornado int `json:"tornado,string"`

	Humidity  float32 `json:"hum,string"`
	Tempi     float32 `json:"tempi,string"`
	Pressurei float32 `json:"pressurei,string"`
	Precipi   float32 `json:"precipi,string"`

	// CondString     string `json:"conds"`
	Cond *Condition `json:"conds"`
}

type Condition int

func (c *Condition) UnmarshalJSON(b []byte) error {
	var condString string
	if err := json.Unmarshal(b, &condString); err != nil {
		return err
	}
	*c = Condition(conditionMap[condString])
	return nil
}

func (c Condition) String() string {
	for s, i := range conditionMap {
		if int(c) == i {
			return s
		}
	}
	return "???"
}

var conditionMap = map[string]int{
	"Unknown":                      0,
	"Clear":                        1,
	"Rain":                         2,
	"Light Rain":                   3,
	"Heavy Rain":                   4,
	"Thunderstorm":                 5,
	"Thunderstorms and Rain":       6,
	"Light Thunderstorms and Rain": 7,
	"Heavy Thunderstorms and Rain": 8,
	"Overcast":                     9,
	"Mostly Cloudy":                10,
	"Partly Cloudy":                11,
	"Scattered Clouds":             12,
	"Fog":                          13,
	"Mist":                         14,
	"Haze":                         15,
	"Light Freezing Rain":          16,
	"Snow":                         17,
	"Light Snow":                   18,
	"Heavy Snow":                   19,
	"Squalls":                      20,
}
