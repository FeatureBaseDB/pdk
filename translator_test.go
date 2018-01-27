package pdk

import (
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/pilosa/pdk/test"
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
	for i := 0; i < 8; i++ {
		rets[i] = make([]uint64, 1000)
		wg.Add(1)
		go func(ret []uint64) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				id, err := bt.GetID("f1", []byte(strconv.Itoa(j)))
				if err != nil {
					t.Fatalf("error getting id: %v", err)
				}
				ret[j] = id
			}
		}(rets[i])
	}

	wg.Wait()
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
