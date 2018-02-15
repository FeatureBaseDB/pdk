package cmd

import (
	"io"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/pdk/http"
	"github.com/spf13/cobra"
)

func NewHTTPCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	com, err := cobrafy.Command(http.NewMain())
	if err != nil {
		panic(err)
	}
	return com
}

func init() {
	subcommandFns["http"] = NewHTTPCommand
}
