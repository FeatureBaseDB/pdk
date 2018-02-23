package pdk

import (
	"fmt"

	"github.com/pkg/errors"
)

// Walk recursively visits every Object in the Entity and calls "call" with
// every Literal and it's path.
func Walk(e *Entity, call func(path []string, l Literal) error) error {
	for prop, val := range e.Objects {
		err := walkObj(val, []string{string(prop)}, call)
		if err != nil {
			return errors.Wrap(err, "walking object")
		}
	}
	return nil
}

func walkObj(val Object, path []string, call func(path []string, l Literal) error) error {
	if objs, ok := val.(Objects); ok {
		// treat lists as sets
		//
		// should add an option to add list index as a path component when order
		// matters. Actually, mapper should have a context which has this
		// information on a per-list basis.
		for _, obj := range objs {
			err := walkObj(obj, path, call)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if ent, ok := val.(*Entity); ok {
		for prop, obj := range ent.Objects {
			err := walkObj(obj, append(path, string(prop)), call)
			if err != nil {
				return err
			}

		}
		return nil
	}
	if lit, ok := val.(Literal); ok {
		return call(path, lit)
	}
	panic(fmt.Sprintf("%#v of type %T at %v should be an \"Objects\", a *Entity, or a Literal... getting here should be impossible", val, val, path))
}
