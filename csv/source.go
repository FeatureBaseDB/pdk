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

package csv

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// Source satisfies the PDK.Source interface for CSV data. Each line in a CSV
// file will be returned by a call to record as a map[string]string where the
// keys are taken from the first line of the CSV. Source is safe for concurrent
// use.
//
// The Source takes care of retrying failed reads/downloads and making sure not
// to return duplicate data. TODO: this functionality needs more testing.
type Source struct {
	files       []*file
	maxRetries  int
	concurrency int

	records chan record
}

// NewSource creates a pdk.Source for CSV data. The source of the raw data can
// be set by using Options defined in this package. e.g.
//
// src := NewSource(WithURLs([]string{"myfile1.csv", "myfile2.csv", "http://example.com/myfile3.csv"}))
func NewSource(options ...Option) *Source {
	src := &Source{
		records:     make(chan record),
		maxRetries:  3,
		concurrency: 1,
	}

	for _, opt := range options {
		opt(src)
	}
	go src.getRecords()
	return src
}

// Option is a functional option to pass to NewSource.
type Option func(*Source)

// WithURLs returns an Option which adds the slice of URLs to the set of data
// sources a Source will read from. The URLs may be HTTP or local files.
func WithURLs(urls []string) Option {
	return func(s *Source) {
		if s.files == nil {
			s.files = make([]*file, 0)
		}
		for _, url := range urls {
			s.files = append(s.files, &file{OpenStringer: urlOpener(url)})
		}
	}
}

// WithOpenStringers returns an Option which adds the slice of OpenStringers to
// the set of data sources a Source will read from.
func WithOpenStringers(os []OpenStringer) Option {
	return func(s *Source) {
		if s.files == nil {
			s.files = make([]*file, 0)
		}
		for _, os := range os {
			s.files = append(s.files, &file{OpenStringer: os})
		}
	}
}

// WithMaxRetries returns an Option which sets the max number of retries per file on
// a Source.
func WithMaxRetries(maxRetries int) Option {
	return func(s *Source) {
		s.maxRetries = maxRetries
	}
}

// WithConcurrency returns an Option which sets the number of goroutines fetching
// files simultaneously.
func WithConcurrency(c int) Option {
	return func(s *Source) {
		if c > 0 {
			s.concurrency = c
		}
	}
}

// file tracks the use of an OpenStringer.
type file struct {
	OpenStringer
	line int // tracks how many lines of this file we've read.
}

// Opener is an interface to a resource which can be repeatedly Opened (and the
// returned ReadCloser can be subsequently read). Each call to Open should
// return a ReadCloser which reads from the beginning of the resource. In the
// case of an error while reading, Open will be called again to retry reading
// the entire resource.
type Opener interface {
	Open() (io.ReadCloser, error)
}

// OpenStringer is an Opener which also has a String method which should return
// the name of the resource being opened (e.g. a file or URL).
type OpenStringer interface {
	fmt.Stringer
	Opener
}

// urlOpener turns a URL or file (string) into an OpenStringer.
type urlOpener string

func (u urlOpener) Open() (io.ReadCloser, error) {
	url := string(u)
	var content io.ReadCloser
	if strings.HasPrefix(url, "http") {
		resp, err := http.Get(url)
		if err != nil {
			return nil, errors.Wrap(err, "getting via http")
		}
		content = resp.Body
	} else {
		f, err := os.Open(url)
		if err != nil {
			return nil, errors.Wrap(err, "opening file")
		}
		content = f
	}
	return content, nil
}

func (u urlOpener) String() string {
	return string(u)
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
	fileChan := make(chan *file, c.concurrency)
	wg := sync.WaitGroup{}
	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			for file := range fileChan {
				c.getRows(file)
			}
			wg.Done()
		}()
	}
	for _, file := range c.files {
		fileChan <- file
	}
	close(fileChan)
	wg.Wait()
	close(c.records)
}

func (c *Source) getRows(file *file) {
	var err error
	for try := 0; try < c.maxRetries; try++ {
		err = c.getRowTry(file)
		if err == nil {
			return
		}
	}
	c.records <- record{err: errors.Wrapf(err, "couldn't fetch '%s' - tried %d times, latest", file, c.maxRetries)}
}

func (c *Source) getRowTry(file *file) error {
	content, err := file.Open()
	if err != nil {
		return errors.Wrap(err, "opening")
	}

	// scan header line
	scan := bufio.NewScanner(content)
	var header []string
	if scan.Scan() && scan.Err() == nil {
		header = strings.Split(scan.Text(), ",")
		if err := validateHeader(header); err != nil {
			c.records <- record{err: errors.Wrapf(err, "validating header of %s", file)}
			return nil // error is permanent so we don't return to getRows for retry
		}
		if file.line == 0 {
			file.line++
		}
	}
	line := 1
	// catch up to previous location
	for line < file.line && scan.Scan() {
		line++
	}
	for scan.Scan() && scan.Err() == nil {
		txt := scan.Text()
		if strings.TrimSpace(txt) == "" {
			continue // skip empty lines. TODO: add stats tracking
		}
		row := strings.Split(txt, ",")
		file.line++
		recordMap, err := parseRecord(header, row)
		if err != nil {
			c.records <- record{
				err: errors.Wrapf(err, "file %s: parsing line %d", file, file.line),
			}
			continue
		}
		// add file and line number under the comma header since that can't
		// be a header from the csv file.
		recordMap[","] = fmt.Sprintf("%s:line%d", file, file.line)
		c.records <- record{
			rec: recordMap,
		}
	}
	return errors.Wrapf(scan.Err(), "scanning '%s', line %d", file, min(line, file.line))
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
