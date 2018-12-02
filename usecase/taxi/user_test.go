package taxi

import (
	"fmt"
	"testing"
)

func TestUserGetter(t *testing.T) {
	ug := newUserGetter(0)
	results := make(map[uint64]int)
	for i := 0; i < 120000000; i++ {
		results[ug.ID()]++
	}

	fmt.Println("Number of users generated", len(results))
	last := uint64(0)
	for i := uint64(0); i < 5000000; i += 100000 {
		if results[i] > 0 {
			last = i
		}
		fmt.Printf("%d: %d\n", i, results[i])
	}
	fmt.Println("last:", last, " results[last] ", results[last])

}
