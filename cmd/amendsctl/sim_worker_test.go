// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sorrell-consulting/amends/sim/ownership"
)

func TestWorkerTraceCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.json")
	var output, diagnostic bytes.Buffer
	if code := run([]string{"sim-worker", "-seed", "17", "-trace", path}, &output, &diagnostic); code != 0 {
		t.Fatalf("code %d: %s", code, diagnostic.String())
	}
	first := output.String()
	if !strings.Contains(first, ownership.Scope) {
		t.Fatal("simulation scope missing")
	}
	output.Reset()
	if code := run([]string{"sim-worker", "-replay", path}, &output, &diagnostic); code != 0 || output.String() != first {
		t.Fatalf("replay differs: code %d: %s", code, diagnostic.String())
	}
	for _, invalid := range []bool{false, true} {
		trace := ownership.Generate(17, invalid)
		if !invalid {
			trace.Steps, trace.Drain = nil, false
		}
		if err := writeTrace(path, trace); err != nil {
			t.Fatal(err)
		}
		output.Reset()
		if code := run([]string{"sim-worker", "-replay", path}, &output, &diagnostic); code != 1 {
			t.Fatal("incomplete/qualified history returned success", output.String())
		}
		if !invalid && !strings.Contains(output.String(), `"status": "INCOMPLETE"`) {
			t.Fatal("missing incomplete boundary")
		}
	}
	for _, raw := range []string{`{}`, `{"unsupported":true}`, `{} {}`} {
		if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
		if code := run([]string{"sim-worker", "-replay", path}, &output, &diagnostic); code != 1 {
			t.Fatal("malformed worker trace accepted")
		}
	}
	if code := run([]string{"sim-worker", "-seed", "-1"}, &output, &diagnostic); code != 2 {
		t.Fatal("invalid seed accepted")
	}
	if code := run([]string{"sim-worker"}, failingWriter{}, &diagnostic); code != 1 {
		t.Fatal("output failure ignored")
	}
}
