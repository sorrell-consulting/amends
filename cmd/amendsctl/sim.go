// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sorrell-consulting/amends/sim"
)

func runSim(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("amendsctl sim", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seed := flags.Uint64("seed", 1, "deterministic history and schedule seed")
	replay := flags.String("replay", "", "replay an exact JSON trace instead of generating")
	path := flags.String("trace", "", "save exact input/schedule trace to this path")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "amendsctl sim: unexpected arguments")
		return 2
	}
	trace := sim.Generate(*seed, false)
	if *replay != "" {
		trace = sim.Trace{}
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
	m, err := sim.Run(trace)
	if err != nil {
		fmt.Fprintln(stderr, err)
		if *path != "" {
			fmt.Fprintf(stderr, "Replay: amendsctl sim -replay %s\n", *path)
		}
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	report := m.Check()
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !report.Accepts(nil, nil) {
		return 1
	}
	return 0
}

func readTrace(path string, trace any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(trace)
	if err == nil {
		var extra any
		if trailing := decoder.Decode(&extra); trailing != io.EOF {
			err = fmt.Errorf("trailing trace content")
		}
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func writeTrace(path string, trace any) error {
	raw, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0644)
}
