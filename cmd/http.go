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
made to it. Every path to a value in the JSON data becomes a Pilosa frame.
`[1:]

	return com
}

func init() {
	subcommandFns["http"] = NewHTTPCommand
}
