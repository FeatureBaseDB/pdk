package pdk

import (
	"fmt"
	"time"
)

// Mapper represents a single method for mapping a specific data type to a slice of row IDs.
// A data type might be composed of multiple fields (e.g. a 2D point).
// A data type might use multiple mappers.
type Mapper interface {
	ID(...interface{}) ([]int64, error)
}

// BoolMapper is a trivial Mapper for boolean types
type BoolMapper struct {
}

// IntMapper is a Mapper for integer types, mapping each int in the range to a row
type IntMapper struct {
	Min           int64
	Max           int64
	Res           int64 // number of bins
	allowExternal bool  // true: outside range -> 'other'; false: outside range -> error
	// TODO: support both "above" and "below" ranges, instead of just "external"
}

//BinaryIntMapper is a Mapper for int types, mapping to a set of buckets representing the value in a binary sense
type BinaryIntMapper struct {
	Min           int64
	Max           int64
	BitDepth      int
	allowExternal bool
}

// TimeOfDayMapper is a Mapper for timestamps, mapping the time component only
// TODO: consider putting all time buckets in same frame
// pros: single frame
// cons: would have to abandon the simple ID interface. also single frame may not be a good thing
type TimeOfDayMapper struct {
	Res int64
}

// DayOfWeekMapper is a Mapper for timestamps, mapping the day of week only
type DayOfWeekMapper struct {
}

type DayOfMonthMapper struct {
}

// MonthMapper is a Mapper for timestamps, mapping the month only
type MonthMapper struct {
}

// YearMapper is a Mapper for timestamps, mapping the year only
type YearMapper struct {
	MinYear int64 // TODO? use this to eliminate empty rows for year < 2000 or whatever
}

// SparseIntMapper is a Mapper for integer types, mapping only relevant ints
type SparseIntMapper struct {
	Min           int64
	Max           int64
	Map           map[int64]int64
	allowExternal bool
	// maintain a map of int->rowID, return existing value or allocate new one
}

// LinearFloatMapper is a Mapper for float types, mapping to regularly spaced buckets
// TODO: consider defining this in terms of a linear mapping
// ID = floor(a*value + b)
type LinearFloatMapper struct {
	Min           float64
	Max           float64
	Res           float64
	Scale         string // linear, logarithmic
	allowExternal bool
}

// FloatMapper is a Mapper for float types, mapping to arbitrary buckets
type FloatMapper struct {
	Buckets       []float64 // slice representing bucket intervals [left0 left1 ... leftN-1 rightN-1]
	allowExternal bool
}

// BinaryFloatMapper is a Mapper for float types, mapping to a set of buckets representing the value in a binary sense
type BinaryFloatMapper struct {
	Min           float64
	Max           float64
	BitDepth      int
	allowExternal bool
}

// StringContainsMapper is a Mapper for string types...
type StringContainsMapper struct {
	Matches []string // slice of strings to check for containment
}

// StringMatchesMapper is a Mapper for string types...
type StringMatchesMapper struct {
	Matches []string // slice of strings to check for match
}

// CustomMapper is a Mapper that applies a function to a slice of fields,
// then applies a simple Mapper to the result of that, returning a rowID.
// This is a generic way to support mappings which span multiple fields.
// It is not supported by the importing config system.
type CustomMapper struct {
	Func   func(...interface{}) interface{}
	Mapper Mapper
}

// GridMapper is a Mapper for a 2D grid (e.g. small-scale latitude/longitude)
type GridMapper struct {
	Xmin          float64
	Xmax          float64
	Xres          int64
	Ymin          float64
	Ymax          float64
	Yres          int64
	allowExternal bool
}

type GridToFloatMapper struct {
	gm       GridMapper
	lfm      LinearFloatMapper
	gridVals []float64
}

func (m GridToFloatMapper) ID(vals ...interface{}) ([]int64, error) {
	gridID, err := m.gm.ID(vals...)
	if err != nil {
		return nil, err
	}
	if gridID[0] >= int64(len(m.gridVals)) {
		return nil, fmt.Errorf("grid mapper returned id out of range: %v", gridID)
	}
	fval := m.gridVals[gridID[0]]
	return m.lfm.ID(fval)
}

func NewGridToFloatMapper(gm GridMapper, lfm LinearFloatMapper, gridVals []float64) GridToFloatMapper {
	return GridToFloatMapper{
		gm:       gm,
		lfm:      lfm,
		gridVals: gridVals,
	}
}

// Point is a point in a 2D space
type Point struct {
	X float64
	Y float64
}

// Region is a simple polygonal region of R2 space
type Region struct {
	Vertices []Point
}

// RegionMapper is a Mapper for a set of geometric regions (e.g. neighborhoods or states)
// TODO: generate regions by reading shapefile
type RegionMapper struct {
	Regions       []Region
	allowExternal bool
}

// ID maps a set of fields using a custom function
func (m CustomMapper) ID(fields ...interface{}) (rowIDs []int64, err error) {
	return m.Mapper.ID(m.Func(fields...))
}

// ID maps a timestamp to a time of day bucket
func (m TimeOfDayMapper) ID(ti ...interface{}) (rowIDs []int64, err error) {
	t := ti[0].(time.Time)
	daySeconds := int64(t.Second() + t.Minute()*60 + t.Hour()*3600)
	return []int64{int64(float64(daySeconds*m.Res) / 86400)}, nil // TODO eliminate extraneous casts
}

// ID maps a timestamp to a day of week bucket
func (m DayOfWeekMapper) ID(ti ...interface{}) (rowIDs []int64, err error) {
	t := ti[0].(time.Time)
	return []int64{int64(t.Weekday())}, nil
}

// ID maps a timestamp to a day of month bucket (1-31)
func (m DayOfMonthMapper) ID(ti ...interface{}) (rowIDs []int64, err error) {
	t := ti[0].(time.Time)
	return []int64{int64(t.Day())}, nil
}

// ID maps a timestamp to a month bucket (1-12)
func (m MonthMapper) ID(ti ...interface{}) (rowIDs []int64, err error) {
	t := ti[0].(time.Time)
	return []int64{int64(t.Month())}, nil
}

// ID maps a timestamp to a year bucket
func (m YearMapper) ID(ti ...interface{}) (rowIDs []int64, err error) {
	t := ti[0].(time.Time)
	return []int64{int64(t.Year())}, nil
}

// ID maps a bool to a rowID (identity mapper)
func (m BoolMapper) ID(bi ...interface{}) (rowIDs []int64, err error) {
	return []int64{bi[0].(int64)}, nil
}

// ID maps an int range to a rowID range
func (m IntMapper) ID(ii ...interface{}) (rowIDs []int64, err error) {
	i := ii[0].(int64)
	externalID := m.Res
	if i < m.Min || i > m.Max {
		if m.allowExternal {
			return []int64{externalID}, nil
		}
		return []int64{0}, fmt.Errorf("int %v out of range", i)
	}
	return []int64{i - m.Min}, nil
}

// ID maps floats to binary bit sets
func (m BinaryIntMapper) ID(ii ...interface{}) (rowIDs []int64, err error) {
	// TODO implement (have to redo the Mapper interface to return slice)
	return []int64{0}, nil
}

// ID maps arbitrary ints to a rowID range
func (m SparseIntMapper) ID(ii ...interface{}) (rowIDs []int64, err error) {
	i := ii[0].(int64)
	if _, ok := m.Map[i]; !ok {
		m.Map[i] = int64(len(m.Map))
	}
	return []int64{m.Map[i]}, nil
}

// ID maps floats to regularly spaced buckets
func (m LinearFloatMapper) ID(fi ...interface{}) (rowIDs []int64, err error) {
	f := fi[0].(float64)
	externalID := int64(m.Res)

	// bounds check
	if f < m.Min || f > m.Max {
		if m.allowExternal {
			return []int64{externalID}, nil
		}
		return []int64{0}, fmt.Errorf("float %v out of range", f)
	}

	// compute bin
	rowID := int64(m.Res * (f - m.Min) / (m.Max - m.Min))
	return []int64{rowID}, nil
}

// ID maps floats to arbitrary buckets
func (m FloatMapper) ID(fi ...interface{}) (rowIDs []int64, err error) {
	f := fi[0].(float64)
	externalID := int64(len(m.Buckets))
	if f < m.Buckets[0] || f > m.Buckets[len(m.Buckets)-1] {
		if m.allowExternal {
			return []int64{externalID}, nil
		}
		return []int64{0}, fmt.Errorf("float %v out of range", f)
	}
	// TODO: make clear decision about which way the equality goes, and document it
	// TODO: use binary search if there are a lot of buckets
	for i, v := range m.Buckets {
		if f < v {
			return []int64{int64(i)}, nil
		}
	}

	// this should be unreachable (TODO test)
	return []int64{0}, nil
}

// ID maps floats to binary bit sets
func (m BinaryFloatMapper) ID(fi ...interface{}) (rowIDs []int64, err error) {
	// TODO implement (have to redo the Mapper interface to handle slice)
	return []int64{0}, nil
}

// ID maps pairs of floats to regular buckets
func (m GridMapper) ID(xyi ...interface{}) (rowIDs []int64, err error) {
	x := xyi[0].(float64)
	y := xyi[1].(float64)
	externalID := m.Xres * m.Yres

	// bounds check
	if x < m.Xmin || x > m.Xmax || y < m.Ymin || y > m.Ymax {
		if m.allowExternal {
			return []int64{externalID}, nil
		}
		return []int64{0}, fmt.Errorf("point (%v, %v) out of range", x, y)
	}

	// compute x bin
	xInt := int64(float64(m.Xres) * (x - m.Xmin) / (m.Xmax - m.Xmin))
	// compute y bin
	yInt := int64(float64(m.Yres) * (y - m.Ymin) / (m.Ymax - m.Ymin))

	rowID := (m.Yres * xInt) + yInt
	return []int64{rowID}, nil

}
