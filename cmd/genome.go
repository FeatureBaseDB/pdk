package cmd

import (
	"io"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/pdk/usecase/genome"
	"github.com/spf13/cobra"
)

// NewGenomeCommand returns a new cobra command which wraps genome.Main
func NewGenomeCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	com, err := cobrafy.Command(genome.NewMain())
	if err != nil {
		panic(err)
	}
	com.Use = `genome`
	com.Short = `pdk genome imports reference genomes from a FASTA file.`
	com.Long = `
pdk genome imports reference genomes from a FASTA file.
`[1:]

	return com
}

func init() {
	subcommandFns["genome"] = NewGenomeCommand
}
