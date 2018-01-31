package pdk

import "fmt"

// Walk recursively visits every Object in the Entity and calls "call" with
// every Literal and it's path.
func Walk(e *Entity, call func(path []string, l Literal)) {
	for prop, val := range e.Objects {
		walkObj(val, []string{string(prop)}, call)
	}
}

func walkObj(val Object, path []string, call func(path []string, l Literal)) {
	if objs, ok := val.(Objects); ok {
		// treat lists as sets
		//
		// should add an option to add list index as a path component when order
		// matters. Actually, mapper should have a context which has this
		// information on a per-list basis.
		for _, obj := range objs {
			walkObj(obj, path, call)
		}
		return
	}
	if ent, ok := val.(*Entity); ok {
		for prop, obj := range ent.Objects {
			walkObj(obj, append(path, string(prop)), call)
		}
		return
	}
	if lit, ok := val.(Literal); ok {
		call(path, lit)
		return
	}
	panic(fmt.Sprintf("%#v of type %T at %v should be an \"Objects\", a *Entity, or a Literal... getting here should be impossible", val, val, path))
}
