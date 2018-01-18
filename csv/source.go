package csv

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type CSVSource struct {
	urls []*url
	pos  int

	records chan record
}

func NewCSVSource(urls []string, options ...func(*CSVSource)) *CSVSource {
	src := &CSVSource{
		urls:    make([]*url, len(urls)),
		records: make(chan record),
	}
	for i, name := range urls {
		src.urls[i] = &url{name: name}
	}

	for _, opt := range options {
		opt(src)
	}
	go src.getRecords()
	return src
}

func (c *CSVSource) nextURL() *url {
	startpos := c.pos
	for c.urls[c.pos].done {
		c.pos = (c.pos + 1) % len(c.urls)
		if c.pos == startpos {
			return nil
		}
	}
	if c.pos == 0 {
		return c.urls[len(c.urls)-1]
	}
	return c.urls[c.pos-1]
}

type url struct {
	name    string
	line    int
	numErrs int
	done    bool
}

func (c *CSVSource) Record() (interface{}, error) {
	rec := <-c.records
	return rec.rec, rec.err
}

type record struct {
	rec map[string]interface{}
	err error
}

func (c *CSVSource) getRecords() {
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
}

func (c *CSVSource) getRows(content io.ReadCloser, url *url) {
	defer content.Close()
	// scan header line
	scan := bufio.NewScanner(content)
	var header []string
	if scan.Scan() {
		header = strings.Split(scan.Text(), ",")
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
		row := strings.Split(scan.Text(), ",")
		url.line++
		recordMap, err := parseRecord(header, row)
		c.records <- record{
			rec: recordMap,
			err: errors.Wrapf(err, "parsing line %d from %s", url.line, url.name),
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

func parseRecord(header []string, row []string) (map[string]interface{}, error) {
	if len(header) != len(row) {
		return nil, errors.Errorf("header/row len mismatch: %v and %v", header, row)
	}
	ret := make(map[string]interface{}, len(header))
	for i := 0; i < len(header); i++ {
		if row[i] == "" {
			continue
		}
		ret[header[i]] = parseString(row[i])
	}
	return ret, nil
}

// TODO add datetime, timestamp, etc.
func parseString(s string) interface{} {
	intVal, err := strconv.Atoi(s)
	if err == nil {
		return intVal
	}
	boolVal, err := strconv.ParseBool(s)
	if err == nil {
		return boolVal
	}
	floatVal, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return floatVal
	}
	return s
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
