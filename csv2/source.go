package csv2

import (
	"bufio"
	"io"
	"log"
	"strings"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

type Source struct {
	rs pdk.RawSource

	cur    pdk.NamedReadCloser
	scan   *bufio.Scanner
	header []string
	line   int
}

func NewSourceFromRawSource(rs pdk.RawSource) *Source {
	return &Source{
		rs: rs,
	}
}

func (s *Source) Record() (record interface{}, err error) {
	if s.cur == nil {
		s.cur, err = s.rs.NextReader()
		if err != nil {
			return nil, err
		}
		// scan header line
		s.scan = bufio.NewScanner(s.cur)
		if s.scan.Scan() && s.scan.Err() == nil {
			s.header = strings.Split(s.scan.Text(), ",")
			if err := validateHeader(s.header); err != nil {
				s.cur = nil
				s.scan = nil
				s.line = 0
				return nil, errors.Wrap(err, "validating header")
			}
		}
	}
	scan := s.scan
	for scan.Scan() {
		s.line++
		if err := scan.Err(); err != nil {
			return nil, err
		}
		txt := scan.Text()
		if strings.TrimSpace(txt) == "" {
			continue // skip empty lines. TODO: add stats tracking
		}
		row := strings.Split(txt, ",")
		recordMap, err := parseRecord(s.header, row)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing")
		}
		return recordMap, nil
	}
	return nil, io.EOF

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
