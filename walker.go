// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
