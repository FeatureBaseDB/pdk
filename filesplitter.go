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

package pdk

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

// FileFragment implements io.ReadCloser for part of a file.
type FileFragment struct {
	file     *os.File
	startLoc int64
	endLoc   int64
}

// NewFileFragment returns a FileFragment which will read only from startLoc to
// endLoc in a file.
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

// Read implements io.Reader for FileFragment.
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

// Close implements io.Closer for a FileFragment.
func (ff *FileFragment) Close() error {
	return nil // TODO
}

// SplitFileLines returns a slice of file fragments which is numParts in length.
// Each FileFragment will read a different section of the file, but the split
// points are guaranteed to be on line breaks.
func SplitFileLines(f *os.File, numParts int64) ([]*FileFragment, error) {
	stats, err := f.Stat()
	if err != nil {
		return nil, err
	}
	splitSize := stats.Size() / numParts

	ret := make([]*FileFragment, 0)
	var startLoc int64
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
