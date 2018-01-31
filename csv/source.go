package csv

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// Source satisfies the PDK.Source interface for CSV data. Each line in a CSV
// file will be returned by a call to record as a map[string]string where
// the keys are taken from the first line of the CSV.
//
// The Source takes care of retrying failed reads/downloads and making sure not
// to return duplicate data. TODO: this functionality needs more testing.
type Source struct {
	urls []*url
	pos  int

	records chan record
}

// Option is a functional option to pass to NewSource.
type Option func(*Source)

// WithURLs returns an Option which can configure a source to pull data from the
// given URLs. These may be HTTP or local files.
func WithURLs(urls []string) Option {
	return func(s *Source) {
		s.urls = make([]*url, len(urls))
		for i, name := range urls {
			s.urls[i] = &url{name: name}
		}
	}
}

// NewSource creates a pdk.Source for CSV data. The source of the raw data can
// be set by using Options defined in this package. e.g.
//
// src := NewSource(WithURLs([]string{"myfile1.csv", "myfile2.csv", "http://example.com/myfile3.csv"}))
func NewSource(options ...Option) *Source {
	src := &Source{
		records: make(chan record),
	}

	for _, opt := range options {
		opt(src)
	}
	go src.getRecords()
	return src
}

func (c *Source) nextURL() *url {
	startpos := c.pos
	for c.urls[c.pos].done {
		c.pos = (c.pos + 1) % len(c.urls)
		if c.pos == startpos {
			return nil
		}
	}
	return c.urls[c.pos]
}

type url struct {
	name    string
	line    int
	numErrs int
	done    bool
}

// Record returns a map[string]string representing a single data line of a
// CSV file. Each key is taken from the header, and each value is parsed from a
// row - empty fields are skipped.
func (c *Source) Record() (interface{}, error) {
	rec, ok := <-c.records
	if !ok {
		return nil, io.EOF
	}
	return rec.rec, rec.err
}

type record struct {
	rec map[string]string
	err error
}

func (c *Source) getRecords() {
	for url := c.nextURL(); url != nil; url = c.nextURL() {
		content, err := getURL(url.name)
		if err != nil {
			url.numErrs += 1
			if url.numErrs >= 10 {
				url.done = true
				c.records <- record{err: errors.Wrapf(err, "couldn't fetch file '%s' - tried 10 times, latest", url.name)}
			}
		}
		c.getRows(content, url)
	}
	close(c.records)
}

func (c *Source) getRows(content io.ReadCloser, url *url) {
	defer content.Close()
	// scan header line
	scan := bufio.NewScanner(content)
	var header []string
	if scan.Scan() {
		header = strings.Split(scan.Text(), ",")
		if err := validateHeader(header); err != nil {
			c.records <- record{err: errors.Wrapf(err, "validating header of %s", url.name)}
			url.done = true
			return
		}
		if url.line == 0 {
			url.line++
		}
	}
	line := 1
	// catch up to previous location
	for line < url.line && scan.Scan() {
		line++
	}
	for scan.Scan() {
		txt := scan.Text()
		if strings.TrimSpace(txt) == "" {
			continue
		}
		row := strings.Split(txt, ",")
		url.line++
		recordMap, err := parseRecord(header, row)
		if err != nil {
			c.records <- record{
				err: errors.Wrapf(err, "url %v: parsing line %d", url.name, url.line),
			}
			continue
		}
		// add file and line number under the comma header since that can't
		// be a header from the csv file.
		recordMap[","] = fmt.Sprintf("%s:line%d", url.name, url.line)
		c.records <- record{
			rec: recordMap,
		}
	}
	err := scan.Err()
	if err != nil {
		c.records <- record{err: errors.Wrapf(err, "scanning '%s', line %d", url.name, min(line, url.line))}
	} else {
		url.done = true
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseRecord(header []string, row []string) (map[string]string, error) {
	if len(header) > len(row) {
		return nil, errors.Errorf("header/row len mismatch: %dvs%d, %v and %v", len(header), len(row), header, row)
	} else if len(row) > len(header) {
		for i := len(header); i < len(row); i++ {
			if strings.TrimSpace(row[i]) != "" {
				log.Printf("data in non headered field: %v, %d", row, i)
			}
		}
	}
	ret := make(map[string]string, len(header))
	for i := 0; i < len(header); i++ {
		if row[i] == "" {
			continue
		}
		ret[header[i]] = row[i]
	}
	return ret, nil
}

func getURL(name string) (io.ReadCloser, error) {
	var content io.ReadCloser
	if strings.HasPrefix(name, "http") {
		resp, err := http.Get(name)
		if err != nil {
			return nil, errors.Wrap(err, "getting via http")
		}
		content = resp.Body
	} else {
		f, err := os.Open(name)
		if err != nil {
			return nil, errors.Wrap(err, "opening file")
		}
		content = f
	}
	return content, nil
}

func validateHeader(header []string) error {
	fields := make(map[string]int)
	for i, h := range header {
		if h == "" {
			return errors.Errorf("header contains empty string at %d: %v", i, header)
		}
		if pos, exists := fields[h]; exists {
			return errors.Errorf("%s appeared at both %d and %d in header", h, pos, i)
		}
		fields[h] = i
	}
	return nil
}
