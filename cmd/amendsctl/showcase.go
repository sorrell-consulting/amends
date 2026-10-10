// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sorrell-consulting/amends/internal/showcase"
)

func runShowcase(args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("demo", flag.ContinueOnError)
	f.SetOutput(stderr)
	mode := f.String("mode", "all", "all, correction-only, or crash-mid-correction")
	brokers := f.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	artifacts := f.String("artifacts", "artifacts/showcase", "parent directory for fresh retained run artifacts")
	timeout := f.Duration("timeout", 3*time.Minute, "deadline per scenario, excluding bounded child cleanup")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || (*mode != "all" && *mode != "correction-only" && *mode != "crash-mid-correction") || *timeout <= 0 || *artifacts == "" || os.Getenv("AMENDS_DATABASE_URL") == "" {
		fmt.Fprintln(stderr, "demo requires AMENDS_DATABASE_URL and valid options; use demo -h")
		return 2
	}
	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	modes := []showcase.Scenario{showcase.CorrectionOnly, showcase.CrashMidCorrection}
	if *mode != "all" {
		modes = []showcase.Scenario{showcase.Scenario(*mode)}
	}
	for _, scenario := range modes {
		bounded, done := context.WithTimeout(ctx, *timeout)
		report, err := showcase.Run(bounded, showcase.Options{DatabaseURL: os.Getenv("AMENDS_DATABASE_URL"), Brokers: strings.Split(*brokers, ","), ControlBinary: binary, WorkerBinary: filepath.Join(filepath.Dir(binary), "amends"), ArtifactParent: *artifacts, Scenario: scenario, Output: stdout})
		done()
		if err != nil {
			fmt.Fprintf(stderr, "demo failed: %v\nArtifacts: %s\n", err, report.Directory)
			return 1
		}
		if _, err = fmt.Fprintf(stdout, "%s PASS (%d ms); owned processes stopped and joined.\n", report.Mode, report.ElapsedMillis); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	return 0
}
