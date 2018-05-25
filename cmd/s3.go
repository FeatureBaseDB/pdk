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

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/aws/s3"
	"github.com/spf13/cobra"
)

// S3Main is wrapped by NewS3Command and only exported for testing purposes.
var S3Main *s3.Main

// NewS3Command returns a new cobra command wrapping S3Main.
func NewS3Command(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	S3Main = s3.NewMain()
	s3Command := &cobra.Command{
		Use:   "s3",
		Short: "s3 - index line separated json from objects in an S3 bucket",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = S3Main.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := s3Command.Flags()
	err = commandeer.Flags(flags, S3Main)
	if err != nil {
		panic(err)
	}
	return s3Command
}

func init() {
	subcommandFns["s3"] = NewS3Command
}
