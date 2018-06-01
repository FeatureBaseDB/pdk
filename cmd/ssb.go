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
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/ssb"
	"github.com/spf13/cobra"
)

// SSBMain is wrapped by NewSSBCommand. It is exported for testing purposes.
var SSBMain *ssb.Main

// NewSSBCommand wraps ssb.Main with cobra.Command for use from a CLI.
func NewSSBCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	SSBMain, err = ssb.NewMain()
	ssbCommand := &cobra.Command{
		Use:   "ssb",
		Short: "run star schema benchmark",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err != nil {
				return err
			}
			start := time.Now()
			err = SSBMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	if err != nil {
		return ssbCommand
	}
	flags := ssbCommand.Flags()
	flags.StringVarP(&SSBMain.Dir, "data-dir", "d", "ssb1", "Directory containing ssb data files.")
	flags.StringSliceVarP(&SSBMain.Hosts, "pilosa-hosts", "p", []string{"localhost:10101"}, "Pilosa cluster.")
	flags.IntVarP(&SSBMain.MapConcurrency, "map-concurrency", "m", 1, "Number of goroutines mapping parsed records.")
	flags.IntVarP(&SSBMain.RecordBuf, "record-buffer", "r", 1000000, "Channel buffer size for parsed records.")

	return ssbCommand
}

func init() {
	subcommandFns["ssb"] = NewSSBCommand
}
