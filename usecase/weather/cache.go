package weather

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type WeatherCache struct {
	jsonPath string
	URLFile  string
	data     map[string]HistoryRecord
}

func NewWeatherCache(path string) *WeatherCache {
	c := &WeatherCache{
		URLFile: path,
		data:    make(map[string]HistoryRecord),
	}
	err := c.ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	return c
}

func (c *WeatherCache) GetDailyRecord(day time.Time) (DailyRecord, error) {
	dayKey := fmt.Sprintf("%d%02d%02d", day.Year(), day.Month(), day.Day())
	if _, ok := c.data[dayKey]; !ok {
		return DailyRecord{}, fmt.Errorf("not found")
	}
	return c.data[dayKey].DailyRecord[0], nil
}

func (c *WeatherCache) GetHourlyRecord(daytime time.Time) (HourlyRecord, error) {
	dayKey := fmt.Sprintf("%d%02d%02d", daytime.Year(), daytime.Month(), daytime.Day())
	if _, ok := c.data[dayKey]; !ok {
		return HourlyRecord{}, fmt.Errorf("day not found")
	}

	hourly := c.data[dayKey].HourlyRecords
	hourKey := daytime.Hour()
	// find first record with matching hour
	// should interpolate with minute data as well
	for _, rec := range hourly {
		if rec.Time.Hour == hourKey {
			return rec, nil
		}
	}
	return HourlyRecord{}, fmt.Errorf("hour not found")
}

func (c *WeatherCache) ReadAll() error {
	// read URL file
	if c.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(c.URLFile)
	if err != nil {
		return err
	}
	urls := make([]string, 0)
	s := bufio.NewScanner(f)
	for s.Scan() {
		urls = append(urls, s.Text())
	}
	if err := s.Err(); err != nil {
		return err
	}
	fmt.Printf("read %d URLs from %s\n", len(urls), c.URLFile)

	// read URLs
	for _, url := range urls {
		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				fmt.Printf("error opening url: %v\n", url)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				fmt.Printf("error opening url: %v\n", url)
				continue
			}
			content = f
		}
		body, err := ioutil.ReadAll(content)
		if err != nil {
			fmt.Printf("error reading content: %v\n", url)
			continue
		}

		record := new(RecordFile)
		if err := json.Unmarshal(body, &record); err != nil {
			// fmt.Printf("error unmarshalling json %v: %v\n", url, err)
			continue
		}
		datestr := url[len(url)-13 : len(url)-5]
		c.data[datestr] = record.History
	}
	return nil
}

func (c *WeatherCache) ReadAllLocal() error {

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
		// check ends with json
		filename := filepath.Base(file.Name())
		datestr := filename[10:18]
		fullRecord, _ := readWundergroundJsonFile(c.jsonPath + "/" + filename)
		c.data[datestr] = fullRecord.History
	}

	return nil
}

func readWundergroundJsonFile(filename string) (*RecordFile, error) {
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
	Meantempi     DumbFloat `json:"meantempi,string"`
	Meanpressurei DumbFloat `json:"meanpressurei,string"`
	Precipi       DumbFloat `json:"precipi,string"`
}

type HourlyRecord struct {
	Time TimeRecord `json:"date"`

	Rain    int `json:"rain,string"`
	Fog     int `json:"fog,string"`
	Snow    int `json:"snow,string"`
	Hail    int `json:"hail,string"`
	Thunder int `json:"thunder,string"`
	Tornado int `json:"tornado,string"`

	Humidity  DumbFloat `json:"hum,string"`
	Tempi     DumbFloat `json:"tempi,string"`
	Pressurei DumbFloat `json:"pressurei,string"`
	Precipi   DumbFloat `json:"precipi,string"`

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

type DumbFloat float32

func (f *DumbFloat) UnmarshalJSON(b []byte) error {
	var floatVal float32
	if err := json.Unmarshal(b, &floatVal); err != nil {
		*f = 0
	} else {
		*f = DumbFloat(floatVal)
	}
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
