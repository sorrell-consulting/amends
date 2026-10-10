// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantOutput string
		wantError  string
	}{
		{name: "no arguments", wantOutput: "Usage: amendsctl <command>"},
		{name: "help command", args: []string{"help"}, wantOutput: "Usage: amendsctl <command>"},
		{name: "short help flag", args: []string{"-h"}, wantOutput: "Usage: amendsctl <command>"},
		{name: "long help flag", args: []string{"--help"}, wantOutput: "Usage: amendsctl <command>"},
		{name: "version command", args: []string{"version"}, wantOutput: "amendsctl devel (" + runtime.Version() + ")\n"},
		{name: "version flag", args: []string{"--version"}, wantOutput: "amendsctl devel (" + runtime.Version() + ")\n"},
		{name: "core example", args: []string{"core-demo"}, wantOutput: "CORE_AGREEMENT"},
		{name: "demo help", args: []string{"demo", "-h"}, wantCode: 0, wantError: "Usage of demo:"},
		{name: "demo invalid mode", args: []string{"demo", "-mode", "unknown"}, wantCode: 2, wantError: "demo requires"},
		{name: "verify needs boundary options", args: []string{"verify"}, wantCode: 2, wantError: "requires -manifest"},
		{name: "inspect needs manifest", args: []string{"inspect"}, wantCode: 2, wantError: "requires -manifest"},
		{name: "replay needs range", args: []string{"replay"}, wantCode: 2, wantError: "requires -manifest"},
		{name: "rebuild needs destination", args: []string{"rebuild"}, wantCode: 2, wantError: "requires -manifest"},
		{name: "replay help", args: []string{"replay", "-h"}, wantError: "Usage of replay:"},
		{name: "rebuild help", args: []string{"rebuild", "-h"}, wantError: "Usage of rebuild:"},
		{name: "drill help", args: []string{"drill", "-h"}, wantError: "Usage of drill:"},
		{name: "diagnostic drills listed", args: []string{"drill", "-h"}, wantError: "source-conflict"},
		{name: "relay stall drill listed", args: []string{"drill", "-h"}, wantError: "relay-stall"},
		{name: "disappearing day drill listed", args: []string{"drill", "-h"}, wantError: "disappearing-day"},
		{name: "materializer crash drill listed", args: []string{"drill", "-h"}, wantError: "materializer-crash"},
		{name: "duplicate flood drill listed", args: []string{"drill", "-h"}, wantError: "duplicate-flood"},
		{name: "stale amendment storm drill listed", args: []string{"drill", "-h"}, wantError: "stale-amendment-storm"},
		{name: "missing history drill listed", args: []string{"drill", "-h"}, wantError: "history-missing"},
		{name: "zombie worker drill listed", args: []string{"drill", "-h"}, wantError: "zombie-worker"},
		{name: "ambiguous commit drill listed", args: []string{"drill", "-h"}, wantError: "ambiguous-commit"},
		{name: "rebalance drill listed", args: []string{"drill", "-h"}, wantError: "rebalance-under-load"},
		{name: "missing history owned scope", args: []string{"drill", "-h"}, wantError: "fresh owned topic"},
		{name: "drill rejects unknown", args: []string{"drill", "-name", "unknown"}, wantCode: 2, wantError: "drill requires"},
		{name: "unknown flag", args: []string{"--unknown"}, wantCode: 2, wantError: `unknown command "--unknown"`},
		{name: "empty command", args: []string{""}, wantCode: 2, wantError: `unknown command ""`},
		{name: "extra argument", args: []string{"version", "extra"}, wantCode: 2, wantError: "expected at most one command"},
		{name: "help with extra argument", args: []string{"help", "verify"}, wantCode: 2, wantError: "expected at most one command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(tt.args, &stdout, &stderr); got != tt.wantCode {
				t.Fatalf("exit code = %d, want %d; stderr = %q", got, tt.wantCode, stderr.String())
			}
			if tt.wantCode != 0 {
				if stdout.Len() != 0 || !strings.Contains(stderr.String(), tt.wantError) {
					t.Fatalf("stdout = %q, stderr = %q; want only diagnostic containing %q", stdout.String(), stderr.String(), tt.wantError)
				}
				return
			}
			if tt.wantError != "" {
				if stdout.Len() != 0 || !strings.Contains(stderr.String(), tt.wantError) {
					t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
				}
				return
			}
			if stderr.Len() != 0 || !strings.Contains(stdout.String(), tt.wantOutput) {
				t.Fatalf("stdout = %q, stderr = %q; want only output containing %q", stdout.String(), stderr.String(), tt.wantOutput)
			}
			if strings.HasPrefix(tt.wantOutput, "Usage:") && !strings.Contains(stdout.String(), "Verification requires --quiesce") {
				t.Fatal("help must disclose the verification boundary precondition")
			}
		})
	}
}

type failingWriter struct{}

func TestAdminExecutionRequiresQuiescence(t *testing.T) {
	t.Setenv("AMENDS_DATABASE_URL", "postgres://unused")
	for _, args := range [][]string{
		{"replay", "-manifest", "unused.json", "-partition", "0", "-from-offset", "0", "-until-offset", "1", "--execute"},
		{"rebuild", "-manifest", "unused.json", "-new-namespace", "new", "-output-topic", "new-output", "--execute"},
	} {
		var output, diagnostic bytes.Buffer
		if code := run(args, &output, &diagnostic); code != 2 || output.Len() != 0 || !strings.Contains(diagnostic.String(), "--quiesce") {
			t.Fatalf("execution without stopped-owner confirmation: code=%d output=%s diagnostic=%s", code, output.String(), diagnostic.String())
		}
	}
}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("output unavailable")
}

func TestRunOutputFailure(t *testing.T) {
	for _, command := range []string{"help", "version", "core-demo"} {
		t.Run(command, func(t *testing.T) {
			var stderr bytes.Buffer
			if got := run([]string{command}, failingWriter{}, &stderr); got != 1 {
				t.Fatalf("exit code = %d, want 1", got)
			}
			if !strings.Contains(stderr.String(), "write output: output unavailable") {
				t.Fatalf("missing output failure diagnostic: %q", stderr.String())
			}
		})
	}
}

func TestSimulationTraceCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.json")
	var output, diagnostic bytes.Buffer
	if code := run([]string{"sim", "-seed", "3", "-trace", path}, &output, &diagnostic); code != 0 {
		t.Fatalf("code %d: %s", code, diagnostic.String())
	}
	first := output.String()
	if !strings.Contains(first, `"scope": "memory-model-only"`) {
		t.Fatal("scope missing")
	}
	output.Reset()
	diagnostic.Reset()
	if code := run([]string{"sim", "-replay", path}, &output, &diagnostic); code != 0 || output.String() != first {
		t.Fatalf("replay code %d: %s", code, diagnostic.String())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var trace map[string]json.RawMessage
	if err := json.Unmarshal(raw, &trace); err != nil {
		t.Fatal(err)
	}
	if string(trace["implementation"]) != `"core-schedule-v1"` {
		t.Fatalf("unexpected trace implementation: %s", trace["implementation"])
	}
	trace["implementation"] = json.RawMessage(`"incompatible-schedule"`)
	raw, err = json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	diagnostic.Reset()
	if code := run([]string{"sim", "-replay", path}, &output, &diagnostic); code != 1 || output.Len() != 0 || !strings.Contains(diagnostic.String(), "incompatible trace provenance") {
		t.Fatalf("incompatible trace: code=%d output=%q diagnostic=%q", code, output.String(), diagnostic.String())
	}
	if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"sim", "-replay", path}, &output, &diagnostic); code == 0 {
		t.Fatal("invalid trace accepted")
	}
	if code := run([]string{"sim", "-seed", "-1"}, &output, &diagnostic); code != 2 {
		t.Fatal("invalid seed accepted")
	}
}
