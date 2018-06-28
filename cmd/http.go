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

package cmd

import (
	"io"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/pdk/http"
	"github.com/spf13/cobra"
)

// NewHTTPCommand returns a new cobra command which wraps http.Main
func NewHTTPCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	com, err := cobrafy.Command(http.NewMain())
	if err != nil {
		panic(err)
	}
	com.Use = `http`
	com.Short = `listens for and indexes arbitrary JSON data in Pilosa`
	com.Long = `
pdk http listens for and indexes arbitrary JSON data in Pilosa.

It starts an HTTP server and tries to decode JSON data from any post request
made to it. Every path to a value in the JSON data becomes a Pilosa field.
`[1:]

	return com
}

func init() {
	subcommandFns["http"] = NewHTTPCommand
}
