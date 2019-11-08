package csv

import (
	"encoding/csv"
	"strings"
	"testing"
)

func TestProcessHeader(t *testing.T) {
	config := NewConfig()
	file := `a,b,c
`
	reader := csv.NewReader(strings.NewReader(file))
	t.Run("invalid IDType", func(t *testing.T) {
		config.IDField = "a"
		config.IDType = "nope"
		_, _, err := processHeader(config, nil, nil, reader, 10, nil)
		if err == nil || !strings.Contains(err.Error(), "unknown IDType") {
			t.Fatalf("unknown IDType gave: %v", err)
		}
	})
}
