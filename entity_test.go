package pdk_test

import (
	"fmt"

	"github.com/pilosa/pdk"

	"testing"
)

func TestToAndFromBytes(t *testing.T) {
	tests := []pdk.Literal{
		pdk.B(false),
		pdk.B(true),
		pdk.S(""),
		pdk.S("hel+lorésumé."),
		pdk.F32(0),
		pdk.F32(32.34),
		pdk.F64(99.3374),
		pdk.F64(0),
		pdk.I(0),
		pdk.I(190),
		pdk.I(3123456789),
		pdk.I(5123456789),
		pdk.I(8446744100000000000),
		pdk.U(0),
		pdk.U(190),
		pdk.U(3123456789),
		pdk.U(5123456789),
		pdk.U(6744100000000000),
		pdk.I8(0),
		pdk.I8(127),
		pdk.I8(-127),
		pdk.I16(0),
		pdk.I16(100),
		pdk.I16(-32000),
		pdk.I32(0),
		pdk.I32(100),
		pdk.I32(1123456789),
		pdk.I32(-1123456789),
		pdk.I64(0),
		pdk.I64(1234567890),
		pdk.I64(-8446744100000000000),
		pdk.U8(0),
		pdk.U8(100),
		pdk.U8(255),
		pdk.U16(0),
		pdk.U16(100),
		pdk.U16(65535),
		pdk.U32(0),
		pdk.U32(100),
		pdk.U32(3123456789),
		pdk.U64(0),
		pdk.U64(1234567890),
		pdk.U64(18446744000000000000),
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d: ", i), func(t *testing.T) {
			bs := pdk.ToBytes(tst)
			nl := pdk.FromBytes(bs)
			if nl != tst {
				t.Fatalf("expected: %#v, actual: %#v", tst, nl)
			}
		})
	}
}
