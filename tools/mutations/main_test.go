// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A child supplies an actual exit status; it does not run a mutated test.
func TestMutationExitStatusHelper(t *testing.T) {
	if os.Getenv("AMENDS_MUTATION_EXIT_HELPER") == "1" {
		os.Exit(1)
	}
}

func TestClassificationRequiresNamedAssertion(t *testing.T) {
	child := exec.Command(os.Args[0], "-test.run=^TestMutationExitStatusHelper$")
	child.Env = append(os.Environ(), "AMENDS_MUTATION_EXIT_HELPER=1")
	failure := child.Run()
	if failure == nil {
		t.Fatal("helper did not return a test failure exit code")
	}
	encode := func(events ...event) []byte {
		var b bytes.Buffer
		for _, e := range events {
			if err := json.NewEncoder(&b).Encode(e); err != nil {
				t.Fatal(err)
			}
		}
		return b.Bytes()
	}
	m := mutation{Test: "TestGuard/case", Assertion: "[amends:test-guard]"}
	bad := encode(event{Action: "run", Test: m.Test}, event{Action: "output", Test: m.Test, Output: "    guard_test.go:12: [amends:test-guard] contract violated\n"}, event{Action: "fail", Test: m.Test}, event{Action: "fail", Test: "TestGuard"}, event{Action: "fail"})
	good := encode(event{Action: "run", Test: m.Test}, event{Action: "pass", Test: m.Test}, event{Action: "pass", Test: "TestGuard"}, event{Action: "pass"})
	for _, tc := range []struct {
		name, want string
		output     []byte
		err        error
		baseline   bool
		equivalent bool
	}{
		{"caught", "CAUGHT", bad, failure, false, false},
		{"reworded diagnostic", "CAUGHT", bytes.ReplaceAll(bad, []byte("contract violated"), []byte("a clearer explanation")), failure, false, false},
		{"marker prefix is insufficient", "ERROR", bytes.ReplaceAll(bad, []byte("[amends:test-guard]"), []byte("[amends:test-guard]suffix")), failure, false, false},
		{"baseline failure", "ERROR", bad, failure, true, false},
		{"baseline pass", "PASS", good, nil, true, false},
		{"survivor", "SURVIVED", good, nil, false, false},
		{"declared equivalent", "EQUIVALENT", good, nil, false, true},
		{"equivalent failed", "ERROR", bad, failure, false, true},
		{"wrong assertion", "ERROR", bytes.ReplaceAll(bad, []byte("[amends:test-guard]"), []byte("[amends:other]")), failure, false, false},
		{"wrong test", "ERROR", bytes.ReplaceAll(bad, []byte(m.Test), []byte("TestOther")), failure, false, false},
		{"panic", "ERROR", append(bad, encode(event{Action: "output", Output: "panic: crash"})...), failure, false, false},
		{"race", "ERROR", append(bad, encode(event{Action: "output", Output: "WARNING: DATA RACE"})...), failure, false, false},
		{"timeout", "ERROR", bad, context.DeadlineExceeded, false, false},
		{"build error", "ERROR", []byte("compile error"), failure, false, false},
		{"missing test", "ERROR", encode(event{Action: "pass"}), nil, false, true},
		{"skipped", "ERROR", encode(event{Action: "run", Test: m.Test}, event{Action: "skip", Test: m.Test}, event{Action: "pass"}), nil, false, true},
		{"unfinished package", "ERROR", encode(event{Action: "run", Test: m.Test}, event{Action: "pass", Test: m.Test}), nil, false, true},
		{"other failure", "ERROR", append(bad, encode(event{Action: "fail", Test: "TestOther"})...), failure, false, false},
		{"additional error", "ERROR", append(bad, encode(event{Action: "output", Test: m.Test, Output: "    db_test.go:20: database unavailable\n"})...), failure, false, false},
		{"parent cleanup error", "ERROR", append(bad, encode(event{Action: "output", Test: "TestGuard", Output: "    db_test.go:20: database unavailable\n"})...), failure, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := m
			candidate.Equivalent = tc.equivalent
			if got, reason := classify(candidate, tc.output, tc.err, tc.baseline); got != tc.want {
				t.Fatalf("got %s (%s), want %s", got, reason, tc.want)
			}
		})
	}
}

func TestCopyAndPatchIsolation(t *testing.T) {
	source, target, evidence := t.TempDir(), t.TempDir(), t.TempDir()
	path := filepath.Join(source, "guard.go")
	if err := os.WriteFile(path, []byte("guard enabled"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := copyTree(source, target); err != nil {
		t.Fatal(err)
	}
	e := edit{Path: "guard.go", Before: "enabled", After: "disabled"}
	if err := applyEdits(target, evidence, []edit{e}); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil || string(original) != "guard enabled" {
		t.Fatal("patch changed original source", err)
	}
	changed, err := os.ReadFile(filepath.Join(target, "guard.go"))
	if err != nil || string(changed) != "guard disabled" {
		t.Fatal("patch did not change private copy", err)
	}
	if err = applyEdits(target, evidence, []edit{e}); err == nil {
		t.Fatal("stale anchor silently accepted")
	}
	for _, invalid := range []string{"../guard.go", "/guard.go", "guard_test.go", "internal/oracle/oracle.go"} {
		e.Path = invalid
		if err = applyEdits(target, evidence, []edit{e}); err == nil {
			t.Fatalf("invalid path accepted: %s", invalid)
		}
	}
	if err = os.Symlink(path, filepath.Join(source, "link.go")); err != nil {
		t.Fatal(err)
	}
	if err = copyTree(source, t.TempDir()); err == nil {
		t.Fatal("source link followed")
	}
}

func TestManifestAnchors(t *testing.T) {
	var cases []mutation
	if err := json.Unmarshal(manifest, &cases); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, m := range cases {
		if seen[m.ID] || m.ID == "" || len(m.Edits) == 0 || m.Test == "" || (!m.Equivalent && m.Assertion == "") {
			t.Fatalf("incomplete/duplicate case: %+v", m)
		}
		seen[m.ID] = true
		// Check anchors without writing or executing any mutant in ordinary CI.
		for _, e := range m.Edits {
			source, err := os.ReadFile(filepath.Join("../..", e.Path))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := editSource(source, e); err != nil {
				t.Fatalf("%s: invalid anchor in %s: %v", m.ID, e.Path, err)
			}
		}
	}
}
