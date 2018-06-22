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
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/pilosa/pdk/test"
	"github.com/pkg/errors"
)

func TestMapTranslator(t *testing.T) {
	mt := NewMapTranslator()
	id, err := mt.GetID("frame1", "thing")
	test.MustBe(t, id, uint64(0), "first")
	test.MustBe(t, err, nil)
	id, err = mt.GetID("frame1", "thing")
	test.MustBe(t, id, uint64(0), "repeat")
	test.MustBe(t, err, nil)

	id, err = mt.GetID("frame1", "thing1")
	test.MustBe(t, id, uint64(1), "third")
	test.MustBe(t, err, nil)

	id, err = mt.GetID("frame2", "thing3")
	test.MustBe(t, id, uint64(0), "fourth")
	test.MustBe(t, err, nil)

	val, err := mt.Get("frame1", 0)
	test.ErrNil(t, err, "Get1-0")
	test.MustBe(t, "thing", val, "Get1-0")
	val, err = mt.Get("frame1", 1)
	test.ErrNil(t, err, "get Get1-1")
	test.MustBe(t, "thing1", val, "Get1-1")
	val, err = mt.Get("frame2", 0)
	test.ErrNil(t, err, "get Get2-0")
	test.MustBe(t, "thing3", val, "Get2-0")
}

func TestConcMapTranslator(t *testing.T) {
	bt := NewMapTranslator()

	wg := &sync.WaitGroup{}
	rets := make([][]uint64, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		rets[i] = make([]uint64, 1000)
		wg.Add(1)
		go func(ret []uint64) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				id, err := bt.GetID("f1", []byte(strconv.Itoa(j)))
				if err != nil {
					errs <- errors.Wrapf(err, "error getting id")
					return
				}
				ret[j] = id
			}
		}(rets[i])
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for i, ret := range rets {
		if i != 0 {
			if !reflect.DeepEqual(ret, rets[i-1]) {
				t.Fatalf("returned ids different in different threads: %v, %v", ret, rets[i-1])
			}
		}
		sort.Sort(test.Uint64Slice(ret))
		for j := 0; j < 1000; j++ {
			if ret[j] != uint64(j) {
				t.Fatalf("returned ids are not monotonic, pos: %v, val: %v, arr: %v", j, ret[j], ret)
			}
		}
	}

}
