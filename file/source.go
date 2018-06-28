package file

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/pilosa/pdk/json"
	"github.com/pkg/errors"
)

// Source is a pdk.Source which reads json objects from files on disk.
type Source struct {
	files     []string
	records   chan record
	subjectAt string
}

// SrcOption is a functional option for the file Source.
type SrcOption func(s *Source) error

// OptSrcSubjectAt tells the source to add a new key to each record whose value
// will be <filename>#<record number>.
func OptSrcSubjectAt(key string) SrcOption {
	return func(s *Source) error {
		s.subjectAt = key
		return nil
	}
}

// OptSrcPath sets the path name for the file or directory to use for source
// data.
func OptSrcPath(pathname string) SrcOption {
	return func(s *Source) error {
		info, err := os.Stat(pathname)
		if err != nil {
			return errors.Wrap(err, "statting path")
		}
		if info.IsDir() {
			infos, err := ioutil.ReadDir(pathname)
			if err != nil {
				return errors.Wrap(err, "reading directory")
			}
			s.files = make([]string, 0, len(infos))
			for _, info = range infos {
				s.files = append(s.files, path.Join(pathname, info.Name()))
			}
		} else {
			s.files = []string{pathname}
		}
		return nil
	}
}

func (s *Source) run() {
	for _, pathname := range s.files {
		file, err := os.Open(pathname)
		if err != nil {
			s.records <- record{err: errors.Wrap(err, "opening file")}
			continue
		}
		src := json.NewSource(file)
		r := record{}
		for i := 0; true; i++ {
			r.data, r.err = src.Record()
			if r.err == io.EOF {
				break
			}
			if s.subjectAt != "" {
				r.data.(map[string]interface{})[s.subjectAt] = fmt.Sprintf("%s#%d", pathname, i)
			}
			s.records <- r
		}
	}
	close(s.records)
}

// NewSource gets a new file source which will index json data from a file or
// all files in a directory.
func NewSource(opts ...SrcOption) (*Source, error) {
	s := &Source{
		records: make(chan record, 100),
	}
	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			return nil, err
		}
	}
	go s.run()
	return s, nil
}

// Record implements pdk.Record returning a map[string]interface{} for each json
// object in the source files.
func (s *Source) Record() (interface{}, error) {
	rec, ok := <-s.records
	if !ok {
		return nil, io.EOF
	}
	return rec.data, rec.err
}

type record struct {
	data interface{}
	err  error
}
