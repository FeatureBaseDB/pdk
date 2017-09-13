package pdk

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func TestLevelTranslator(t *testing.T) {
	levelDir := tempDirName(t)
	bt, err := NewLevelTranslator(levelDir, "f1", "f2")
	if err != nil {
		t.Fatalf("couldn't get level translator: %v", err)
	}
	id1, err := bt.GetID("f1", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id for hello f1: %v", err)
	}
	id2, err := bt.GetID("f2", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id for hello in f2: %v", err)
	}

	val := bt.Get("f1", id1)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("unexpected value for hello id in f1: %s", val)
	}

	val = bt.Get("f2", id2)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("unexpected value for hello id in f2: %s", val)
	}

	err = bt.Close()
	if err != nil {
		t.Fatalf("closing level translator: %v", err)
	}

	bt, err = NewLevelTranslator(levelDir, "f1", "f2")
	if err != nil {
		t.Fatalf("couldn't get level translator after closing: %v", err)
	}
	val = bt.Get("f1", id1)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("after reopen, unexpected value for hello id in f1: %s", val)
	}

	val = bt.Get("f2", id2)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("after reopen, unexpected value for hello id in f2: %s", val)
	}

	id1again, err := bt.GetID("f1", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id again for hello f1: %v", err)
	}
	id2again, err := bt.GetID("f2", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id again for hello in f2: %v", err)
	}

	if id1again != id1 || id2again != id2 {
		t.Fatalf("didn't get same ids for same values id1: %v, 1again: %v, 2: %v, 2again: %v", id1, id1again, id2, id2again)
	}
}

func TestConcLevelTranslator(t *testing.T) {
	levelDir := tempDirName(t)
	bt, err := NewLevelTranslator(levelDir, "f1", "f2")
	if err != nil {
		t.Fatalf("couldn't get level translator: %v", err)
	}

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

func BenchmarkLevelTranslatorGetID(b *testing.B) {
	levelDir := tempDirName(b)
	bt, err := NewLevelTranslator(levelDir, "f1", "f2")
	if err != nil {
		b.Fatalf("couldn't get level translator: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bt.GetID("f1", []byte(strconv.Itoa(i)))
	}
}

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func tempDirName(t testing.TB) string {
	tf, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("couldn't get temp file: %v", err)
	}
	return tf
}
