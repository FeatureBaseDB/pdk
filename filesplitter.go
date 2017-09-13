package pdk

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

type FileFragment struct {
	file     *os.File
	startLoc int64
	endLoc   int64
}

func NewFileFragment(f *os.File, startLoc, endLoc int64) (*FileFragment, error) {
	thisF, err := os.Open(f.Name())
	if err != nil {
		return nil, errors.Wrap(err, "opening file fragment")
	}
	_, err = thisF.Seek(startLoc, io.SeekStart)
	if err != nil {
		return nil, errors.Wrap(err, "seeking to start location in new file handle")
	}
	return &FileFragment{
		file:     thisF,
		startLoc: startLoc,
		endLoc:   endLoc,
	}, nil
}

func (ff *FileFragment) Read(b []byte) (n int, err error) {
	offset, err := ff.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	if int64(len(b)) > ff.endLoc-offset {
		n, err := ff.file.Read(b[:ff.endLoc-offset])
		if int64(n) == ff.endLoc-offset {
			return n, io.EOF
		}
		return n, err
	}
	return ff.file.Read(b)
}

func (ff *FileFragment) Close() error {
	return nil // TODO
}

func SplitFileLines(f *os.File, numParts int64) ([]*FileFragment, error) {
	stats, err := f.Stat()
	if err != nil {
		return nil, err
	}
	splitSize := stats.Size() / numParts

	ret := make([]*FileFragment, 0)
	var startLoc int64 = 0
	for {
		endLoc, errSeek := seekAndSearch(f, splitSize, '\n')
		if errSeek != nil && errSeek != io.EOF {
			return nil, errors.Wrap(errSeek, "searching for next split location")
		}
		ff, err := NewFileFragment(f, startLoc, endLoc)
		if err != nil {
			return nil, errors.Wrap(err, "creating new file fragment")
		}
		ret = append(ret, ff)
		if errSeek == io.EOF {
			break
		}
		startLoc = endLoc
	}
	return ret, nil
}

func seekAndSearch(f io.ReadSeeker, splitSize int64, b byte) (newOffset int64, err error) {
	off, err := f.Seek(splitSize, io.SeekCurrent)
	if err != nil {
		return off, err
	}
	idx, err := searchReader(f, b)
	if err == io.EOF {
		return off + idx, io.EOF
	} else if err != nil {
		return 0, err
	}
	newOffset, err = f.Seek(off+idx, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return newOffset, nil
}

// searchReader returns the number of bytes until byte b or io.EOF is
// encountered in Reader r. It is not idempotent and is not guaranteed to leave
// the reader in any particular state. The returned error will be io.EOF, only
// if EOF was encountered idx bytes into the Reader.
func searchReader(r io.Reader, b byte) (idx int64, err error) {
	buf := make([]byte, 1000)
	idx = 0
	var n int
	for err == nil {
		n, err = r.Read(buf)
		for i := 0; i < n; i++ {
			if buf[i] == b {
				idx += int64(i) + 1
				return idx, nil
			}
		}
		idx += int64(n)
	}
	if err == io.EOF {
		return idx, io.EOF
	}
	return 0, err
}
