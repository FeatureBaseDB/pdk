package file

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/json"
	"github.com/pkg/errors"
)

// Source is a pdk.Source which reads json objects from files on disk.
type Source struct {
	rawSource *RawSource
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
	return func(s *Source) (err error) {
		s.rawSource, err = NewRawSource(pathname)
		if err != nil {
			return errors.Wrap(err, "getting raw source")
		}
		return nil
	}
}

func (s *Source) run() {
	reader, err := s.rawSource.NextReader()
	for ; err == nil; reader, err = s.rawSource.NextReader() {
		src := json.NewSource(reader)
		r := record{}
		for i := 0; true; i++ {
			r.data, r.err = src.Record()
			if r.err == io.EOF {
				reader.Close()
				break
			}
			if s.subjectAt != "" {
				r.data.(map[string]interface{})[s.subjectAt] = fmt.Sprintf("%s#%d", reader.Name(), i)
			}
			s.records <- r
		}
	}
	if err != nil {
		s.records <- record{err: errors.Wrap(err, "getting next reader")}
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

type RawSource struct {
	files   []string
	fileIdx *uint64
}

func NewRawSource(pathname string) (*RawSource, error) {
	fileIdx := uint64(0)
	s := &RawSource{
		fileIdx: &fileIdx,
	}
	info, err := os.Stat(pathname)
	if err != nil {
		return nil, errors.Wrap(err, "statting path")
	}
	if info.IsDir() {
		infos, err := ioutil.ReadDir(pathname)
		if err != nil {
			return nil, errors.Wrap(err, "reading directory")
		}
		s.files = make([]string, 0, len(infos))
		for _, info = range infos {
			s.files = append(s.files, path.Join(pathname, info.Name()))
		}
	} else {
		s.files = []string{pathname}
	}
	return s, nil
}

type metaFile struct {
	*os.File
}

func (m *metaFile) Name() string {
	return filepath.Base(m.File.Name())
}

func (m *metaFile) Meta() map[string]interface{} { return nil }

func (s *RawSource) NextReader() (pdk.NamedReadCloser, error) {
	idx := atomic.AddUint64(s.fileIdx, 1) - 1
	if int(idx) >= len(s.files) {
		return nil, io.EOF
	}

	file, err := os.Open(s.files[idx])
	if err != nil {
		return nil, errors.Wrapf(err, "opening %s", s.files[idx])
	}

	mf := metaFile{file}
	return &mf, nil
}
