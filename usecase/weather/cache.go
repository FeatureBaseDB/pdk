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

package weather

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

type weatherCache struct {
	URLFile string
	data    map[string]historyRecord
}

func newWeatherCache() *weatherCache {
	c := &weatherCache{
		data: make(map[string]historyRecord),
	}
	return c
}

func (c *weatherCache) GetDailyRecord(day time.Time) (dailyRecord, error) {
	dayKey := fmt.Sprintf("%d%02d%02d", day.Year(), day.Month(), day.Day())
	if _, ok := c.data[dayKey]; !ok {
		return dailyRecord{}, fmt.Errorf("not found")
	}
	return c.data[dayKey].DailyRecord[0], nil
}

func (c *weatherCache) GetHourlyRecord(daytime time.Time) (hourlyRecord, error) {
	dayKey := fmt.Sprintf("%d%02d%02d", daytime.Year(), daytime.Month(), daytime.Day())
	if _, ok := c.data[dayKey]; !ok {
		return hourlyRecord{}, fmt.Errorf("day not found")
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
	return hourlyRecord{}, fmt.Errorf("hour not found")
}

func (c *weatherCache) ReadAll() error {
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

		record := new(recordFile)
		if err := json.Unmarshal(body, &record); err != nil {
			// fmt.Printf("error unmarshalling json %v: %v\n", url, err)
			continue
		}
		datestr := url[len(url)-13 : len(url)-5]
		c.data[datestr] = record.History
	}
	return nil
}

type recordFile struct {
	History  historyRecord `json:"history"`
	Response apiResponse   `json:"response"`
}

type apiResponse struct {
	Version string `json:"version"`
	Tos     string `json:"termsofService"`
}

type historyRecord struct {
	DailyRecord   []dailyRecord  `json:"dailysummary"`
	HourlyRecords []hourlyRecord `json:"observations"`
	// Time          TimeRecord     `json:"date"`
}

type timeRecord struct {
	Year int `json:"year,string"`
	Mon  int `json:"mon,string"`
	Mday int `json:"mday,string"`
	Hour int `json:"hour,string"`
	Min  int `json:"min,string"`
}

type dailyRecord struct {
	// Time TimeRecord `json:"date"`

	Rain    int `json:"rain,string"`
	Fog     int `json:"fog,string"`
	Snow    int `json:"snow,string"`
	Hail    int `json:"hail,string"`
	Thunder int `json:"thunder,string"`
	Tornado int `json:"tornado,string"`

	// Humidity float32 `json:"humidity,string"`  // sometimes ""
	Meantempi     dumbFloat `json:"meantempi,string"`
	Meanpressurei dumbFloat `json:"meanpressurei,string"`
	Precipi       dumbFloat `json:"precipi,string"`
}

type hourlyRecord struct {
	Time timeRecord `json:"date"`

	Rain    int `json:"rain,string"`
	Fog     int `json:"fog,string"`
	Snow    int `json:"snow,string"`
	Hail    int `json:"hail,string"`
	Thunder int `json:"thunder,string"`
	Tornado int `json:"tornado,string"`

	Humidity  dumbFloat `json:"hum,string"`
	Tempi     dumbFloat `json:"tempi,string"`
	Pressurei dumbFloat `json:"pressurei,string"`
	Precipi   dumbFloat `json:"precipi,string"`

	// CondString     string `json:"conds"`
	Cond *condition `json:"conds"`
}

type condition int

func (c *condition) UnmarshalJSON(b []byte) error {
	var condString string
	if err := json.Unmarshal(b, &condString); err != nil {
		return err
	}
	*c = condition(conditionMap[condString])
	return nil
}

type dumbFloat float32

func (f *dumbFloat) UnmarshalJSON(b []byte) error {
	var floatVal float32
	if err := json.Unmarshal(b, &floatVal); err != nil {
		*f = 0
	} else {
		*f = dumbFloat(floatVal)
	}
	return nil
}

func (c condition) String() string {
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
