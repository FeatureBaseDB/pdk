package pdk

import (
	"strings"

	"github.com/pkg/errors"
)

// Framer is an interface for extracting frame names from paths denoted by
// []string. The path could be (e.g.) a list of keys in a nested map which
// arrives at a non-container value (string, int, etc).
type Framer interface {
	// The Frame method should return an empty string and a nil error if the value
	// at the given path should be ignored. It should return an error, only if
	// something unexpected has occurred which means the record cannot be properly
	// processed.
	Frame(path []string) (frame string, err error)
	Field(path []string) (frame, field string, err error)
}

// FramerFunc is similar to http.HandlerFunc in that you can make a bare
// function satisfy the Framer interface by doing FramerFunc(yourfunc).
type FramerFunc func([]string) (string, error)

// Frame on FramerFunc simply calls the wrapped function.
func (f FramerFunc) Frame(path []string) (string, error) {
	return f(path)
}

// Field on FramerFunc calls the wrapped function on the path to get the frame
// (just as Frame does), and calls it again on a slice containing only the last
// string in the path to get the field.
func (f FramerFunc) Field(path []string) (string, string, error) {
	if len(path) == 0 {
		return "", "", errors.New("can't get a field from an empty path")
	}
	frame, err := f(path)
	if err != nil {
		return "", "", errors.Wrap(err, "getting frame")
	}
	if frame == "" {
		return "", "", nil
	}
	field, err := f([]string{path[len(path)-1]})
	return frame, field, errors.Wrap(err, "getting field")
}

// DashFrame creates a frame name from the path by joining the path elements with
// the "-" character.
type DashFrame struct {
	Ignore   []string
	Collapse []string
}

func (d *DashFrame) clean(path []string) []string {
	np := []string{}
OUTER:
	for _, p := range path {
		for _, ig := range d.Ignore {
			if ig == p {
				return nil
			}
		}
		for _, coll := range d.Collapse {
			if coll == p {
				continue OUTER
			}
		}
		np = append(np, strings.TrimSpace(p))
	}
	return np
}

// Frame gets a frame from a path by joining the path elements with dashes.
func (d *DashFrame) Frame(path []string) (frame string, err error) {
	np := d.clean(path)
	return strings.ToLower(strings.Join(np, "-")), nil
}

// Field gets a frame and field from a path dash framing the beginning elements
// and returning the last element as the field.
func (d *DashFrame) Field(path []string) (frame, field string, err error) {
	np := d.clean(path)
	if len(np) == 0 {
		return
	}
	frame = strings.ToLower(strings.Join(np[:len(np)-1], "-"))
	field = strings.ToLower(np[len(np)-1])
	return frame, field, nil
}
