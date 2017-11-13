package pdk

import (
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func MustBe(t *testing.T, thing1, thing2 interface{}, context ...string) {
	var ctx string
	if len(context) == 0 {
		ctx = ""
	} else {
		ctx = context[0] + ": "
	}
	if !reflect.DeepEqual(thing1, thing2) {
		t.Fatalf("%v'%#v' != '%#v'", ctx, thing1, thing2)
	}
}

func TestMapTranslator(t *testing.T) {
	mt := NewMapTranslator()
	id, err := mt.GetID("frame1", "thing")
	MustBe(t, id, uint64(0), "first")
	MustBe(t, err, nil)
	id, err = mt.GetID("frame1", "thing")
	MustBe(t, id, uint64(0), "repeat")
	MustBe(t, err, nil)

	id, err = mt.GetID("frame1", "thing1")
	MustBe(t, id, uint64(1), "third")
	MustBe(t, err, nil)

	id, err = mt.GetID("frame2", "thing3")
	MustBe(t, id, uint64(0), "fourth")
	MustBe(t, err, nil)

	val := mt.Get("frame1", 0)
	MustBe(t, "thing", val, "Get1-0")
	val = mt.Get("frame1", 1)
	MustBe(t, "thing1", val, "Get1-0")
	val = mt.Get("frame2", 0)
	MustBe(t, "thing3", val, "Get1-0")
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
		sort.Sort(Uint64Slice(ret))
		for j := 0; j < 1000; j++ {
			if ret[j] != uint64(j) {
				t.Fatalf("returned ids are not monotonic, pos: %v, val: %v, arr: %v", j, ret[j], ret)
			}
		}
	}

}
