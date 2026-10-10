// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/sorrell-consulting/amends/sim/transport"
)

func runSimDelivery(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("amendsctl sim-delivery", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seed := flags.Uint64("seed", 1, "deterministic history and delivery schedule seed")
	replay := flags.String("replay", "", "replay an exact delivery JSON trace")
	path := flags.String("trace", "", "save exact input and fault choices to this path")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "amendsctl sim-delivery: unexpected arguments")
		return 2
	}
	trace := transport.Generate(*seed, false)
	if *replay != "" {
		trace = transport.Trace{}
		if err := readTrace(*replay, &trace); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	if *path != "" {
		if err := writeTrace(*path, trace); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	result, runErr := transport.Run(context.Background(), trace)
	if result != nil {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result.Report); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	if runErr != nil {
		fmt.Fprintln(stderr, runErr)
		if *path != "" {
			fmt.Fprintf(stderr, "Replay: amendsctl sim-delivery -replay %s\n", *path)
		}
		return 1
	}
	if result.Report.Status != "PASS" {
		return 1
	}
	return 0
}
