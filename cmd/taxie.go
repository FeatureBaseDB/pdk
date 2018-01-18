package cmd

import (
	"io"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/pdk/usecase/taxie"
	"github.com/spf13/cobra"
)

func NewTaxiCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	com, err := cobrafy.Command(taxie.NewMain())
	if err != nil {
		panic(err)
	}
	return com
}

func init() {
	subcommandFns["taxie"] = NewTaxiCommand
}
