// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Command amendsctl provides the core example, trace runner, and source setup.
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/sorrell-consulting/amends/internal/demo"
)

const usage = `Usage: amendsctl <command>

Start here:
  core-demo      Run the embedded correction example and independent core checks
  demo           Run durable correction / owned worker-crash stories; use demo -h
  help           Show this help (also -h or --help)
  version        Show the development version and Go toolchain (also --version)

Create and load a local pipeline:
  source-create  Create a fresh retained source topic and print its manifest
  source-load    Publish a transaction JSONL file using a source manifest
  output-create  Create/bind the namespace output topic and print a pipeline manifest

Inspect and verify:
  inspect        Show live progress and diagnostics; -format html saves a snapshot dashboard
  view-show      Read the durable view, versions, tombstones, and progress
  quarantine     Read durable rejected input; use quarantine -h
  conflicts      Read current and superseded conflict evidence; use conflicts -h
  verify         Compare a guarded source/output boundary with the independent oracle

Recover and exercise faults:
  replay         Check a consumed range; --execute installs an administrative fence
  rebuild        Reconstruct and verify a fresh namespace; defaults to a dry run
  drill          Run replay/rebuild, diagnostics, delivery, input, history, ownership,
                 commit-uncertainty, and rebalance drills; use drill -h for every name
                 Worker-crash story: demo -mode crash-mid-correction

Advanced deterministic schedules:
  sim            Run a seeded memory schedule; use sim -h for trace options
  sim-delivery   Schedule production relay/view recovery with memory I/O; use sim-delivery -h
  sim-worker     Schedule production worker lifecycle with memory I/O; use sim-worker -h

Verification requires --quiesce and a stopped, joined relay; use verify -h.
Inspect and view-show report live observations, not verifier PASS.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run keeps argument handling testable without exiting the test process.
// Exit codes are 0 for success, 1 for runtime/output failure, and 2 for invalid usage.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "quarantine" || args[0] == "conflicts") {
		return runReview(args[0], args[1:], stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "replay" || args[0] == "rebuild") {
		return runAdmin(args[0], args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "drill" {
		return runAdminDrill(args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "demo" {
		return runShowcase(args[1:], stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "verify" || args[0] == "inspect") {
		return runOperator(args[0], args[1:], stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "output-create" || args[0] == "view-show") {
		return runOutput(args[0], args[1:], stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "source-create" || args[0] == "source-load") {
		return runSource(args[0], args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "sim" {
		return runSim(args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "sim-delivery" {
		return runSimDelivery(args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "sim-worker" {
		return runSimWorker(args[1:], stdout, stderr)
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "amendsctl: expected at most one command\nRun 'amendsctl help' for usage.")
		return 2
	}

	command := "help"
	if len(args) == 1 {
		command = args[0]
	}

	var err error
	switch command {
	case "help", "-h", "--help":
		_, err = fmt.Fprint(stdout, usage)
	case "version", "--version":
		_, err = fmt.Fprintf(stdout, "amendsctl devel (%s)\n", runtime.Version())
	case "core-demo":
		err = demo.Run(stdout)
	default:
		fmt.Fprintf(stderr, "amendsctl: unknown command %q\nRun 'amendsctl help' for usage.\n", command)
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl: command/write output: %v\n", err)
		return 1
	}
	return 0
}
