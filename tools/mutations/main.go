// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Command mutations runs the explicit, isolated guard campaign. Mutation source
// is data in campaign.json; no production package has a broken execution mode.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

//go:embed campaign.json
var manifest []byte

type edit struct {
	Path       string          `json:"path"`
	Before     string          `json:"before,omitempty"`
	After      string          `json:"after,omitempty"`
	Comparison *comparisonEdit `json:"comparison,omitempty"`
}
type mutation struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Package     string `json:"package"`
	Test        string `json:"test"`
	Assertion   string `json:"assertion"`
	Integration bool   `json:"integration"`
	Equivalent  bool   `json:"equivalent"`
	Edits       []edit `json:"edits"`
}
type result struct {
	ID        string   `json:"id"`
	Status    string   `json:"status"`
	Reason    string   `json:"reason"`
	Test      string   `json:"test"`
	Assertion string   `json:"assertion,omitempty"`
	Command   []string `json:"command"`
}
type report struct {
	Started   time.Time         `json:"started"`
	Seconds   float64           `json:"seconds"`
	Planned   int               `json:"planned"`
	Complete  bool              `json:"complete"`
	Passed    bool              `json:"passed"`
	Revision  string            `json:"revision"`
	Worktree  string            `json:"worktree"`
	Toolchain string            `json:"toolchain"`
	Inputs    map[string]string `json:"input_sha256"`
	Results   []result          `json:"results"`
}

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "artifacts/mutations", "parent directory for a new evidence run")
	only := flag.String("only", "", "run one mutation ID (default: full campaign)")
	flag.Parse()
	if err := run(*root, *out, *only); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root, out, only string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	var cases []mutation
	if err = json.Unmarshal(manifest, &cases); err != nil {
		return err
	}
	if only != "" {
		cases = slices.DeleteFunc(cases, func(m mutation) bool { return m.ID != only })
		if len(cases) != 1 {
			return fmt.Errorf("unknown mutation ID %q", only)
		}
	}
	if err = os.MkdirAll(out, 0755); err != nil {
		return err
	}
	out, err = os.MkdirTemp(out, "run-")
	if err != nil {
		return err
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}
	fmt.Println("Evidence:", out)
	if err = writeJSON(filepath.Join(out, "campaign.json"), cases); err != nil {
		return err
	}
	work, err := os.MkdirTemp("", "amends-mutations-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work) // Only this runner's private temporary tree.
	base := filepath.Join(work, "baseline")
	// Explicit build inputs avoid copying Git credentials, .env, artifacts,
	// binaries, or files elsewhere in a parent workspace. Archives work too.
	inputs := []string{"go.mod", "go.sum", "Makefile", "cmd", "internal", "fixtures", "migrations", "sim", "test", "tools"}
	for _, path := range inputs {
		if err = copyTree(filepath.Join(root, path), filepath.Join(base, path)); err != nil {
			return err
		}
	}
	r := report{Started: time.Now().UTC(), Planned: len(cases), Inputs: map[string]string{}}
	if err = archiveSource(base, filepath.Join(out, "source.zip"), r.Inputs); err != nil {
		return err
	}
	mod, err := os.ReadFile(filepath.Join(base, "go.mod"))
	if err != nil {
		return err
	}
	version := regexp.MustCompile(`(?m)^go ([0-9.]+)$`).FindSubmatch(mod)
	if len(version) != 2 {
		return fmt.Errorf("go.mod has no pinned toolchain")
	}
	env := append(os.Environ(), "GOWORK=off", "GOENV=off", "GOFLAGS=", "GOTOOLCHAIN=go"+string(version[1]))
	git := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		b, e := cmd.CombinedOutput()
		if e != nil {
			return "unavailable (source archive or Git error)"
		}
		return strings.TrimSpace(string(b))
	}
	r.Revision = git("rev-parse", "HEAD")
	r.Worktree = git("status", "--short")
	versionOutput, err := command(base, env, "go", "version")
	if err != nil {
		return fmt.Errorf("toolchain: %w: %s", err, versionOutput)
	}
	r.Toolchain = strings.TrimSpace(string(versionOutput))
	if err = writeJSON(filepath.Join(out, "report.json"), r); err != nil {
		return err
	}
	failed := false
	for _, m := range cases {
		got := result{ID: m.ID, Test: m.Test, Assertion: m.Assertion, Status: "ERROR"}
		args := []string{"test", "-mod=readonly", "-buildvcs=false", "-race", "-count=1", "-timeout=90s", "-json"}
		if m.Integration {
			args = append(args, "-tags=integration")
		}
		parts := strings.Split(m.Test, "/")
		for i, part := range parts {
			parts[i] = "^" + regexp.QuoteMeta(part) + "$"
		}
		args = append(args, "-run", strings.Join(parts, "/"), m.Package)
		got.Command = append([]string{"go"}, args...)
		dir := filepath.Join(out, m.ID)
		if err = os.Mkdir(dir, 0755); err != nil {
			return err
		}
		caseEnv := append(slices.Clone(env), "AMENDS_ARTIFACT_DIR="+filepath.Join(dir, "failures"))
		baseline, baselineErr := command(base, caseEnv, "go", args...)
		if err = os.WriteFile(filepath.Join(dir, "baseline.jsonl"), baseline, 0644); err != nil {
			return err
		}
		if status, reason := classify(m, baseline, baselineErr, true); status != "PASS" {
			got.Reason = "baseline: " + reason
		} else {
			mutated := filepath.Join(work, m.ID)
			if err = copyTree(base, mutated); err != nil {
				return err
			}
			if err = applyEdits(mutated, dir, m.Edits); err != nil {
				got.Reason = err.Error()
			} else {
				output, testErr := command(mutated, caseEnv, "go", args...)
				if err = os.WriteFile(filepath.Join(dir, "mutant.jsonl"), output, 0644); err != nil {
					return err
				}
				got.Status, got.Reason = classify(m, output, testErr, false)
			}
		}
		r.Results = append(r.Results, got)
		r.Seconds = time.Since(r.Started).Seconds()
		// Save partial results too; a later interrupted run cannot look complete.
		if err = writeJSON(filepath.Join(out, "report.json"), r); err != nil {
			return err
		}
		fmt.Printf("%s %-10s %s\n", got.ID, got.Status, got.Reason)
		failed = failed || (got.Status != "CAUGHT" && got.Status != "EQUIVALENT")
	}
	r.Complete, r.Passed = true, !failed
	if err = writeJSON(filepath.Join(out, "report.json"), r); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("campaign failed; see %s", filepath.Join(out, "report.json"))
	}
	return nil
}

func command(dir string, env []string, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir, cmd.Env, cmd.WaitDelay = dir, env, 5*time.Second
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return output, ctx.Err()
	}
	return output, err
}

type event struct {
	Action string
	Test   string
	Output string
}

func classify(m mutation, output []byte, runErr error, baseline bool) (string, string) {
	if runErr != nil {
		var exit *exec.ExitError
		if !errors.As(runErr, &exit) || exit.ExitCode() != 1 {
			return "ERROR", "command did not finish with an ordinary test result"
		}
	}
	for _, fatal := range []string{"panic:", "fatal error:", "WARNING: DATA RACE", "test timed out"} {
		if bytes.Contains(output, []byte(fatal)) {
			return "ERROR", "panic, race, or timeout is not a semantic assertion"
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	ran, matched, unrelated, skipped := false, false, false, false
	terminal, pkgTerminal := "", ""
	diagnostic := regexp.MustCompile(`(?m)^\s+[^\n]+\.go:[0-9]+: (.*)$`)
	for {
		var e event
		if err := decoder.Decode(&e); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return "ERROR", "invalid test JSON (possibly a build or infrastructure failure)"
		}
		if e.Action == "skip" {
			skipped = true
		}
		if e.Action == "fail" && e.Test != "" && e.Test != m.Test && !strings.HasPrefix(m.Test, e.Test+"/") {
			unrelated = true
		}
		if e.Test == "" && (e.Action == "pass" || e.Action == "fail") {
			pkgTerminal = e.Action
		}
		for _, line := range diagnostic.FindAllStringSubmatch(e.Output, -1) {
			if e.Test == m.Test && m.Assertion != "" && strings.HasPrefix(line[1], m.Assertion+" ") {
				matched = true
			} else if !baseline && !m.Equivalent {
				unrelated = true
			}
		}
		if e.Test != m.Test {
			continue
		}
		ran = ran || e.Action == "run"
		if e.Action == "pass" || e.Action == "fail" {
			terminal = e.Action
		}
	}
	if !ran || skipped || unrelated || terminal == "" {
		return "ERROR", "missing/skipped test or unrelated failure/diagnostic"
	}
	if runErr == nil && terminal == "pass" && pkgTerminal == "pass" {
		if baseline {
			return "PASS", "named baseline passed"
		}
		if m.Equivalent {
			return "EQUIVALENT", "declared daily-close equivalence; named checks passed"
		}
		return "SURVIVED", "named check passed despite a non-equivalent mutation"
	}
	if !baseline && !m.Equivalent && runErr != nil && terminal == "fail" && pkgTerminal == "fail" && matched {
		return "CAUGHT", m.Assertion
	}
	return "ERROR", "test failed without the declared semantic assertion"
}

func applyEdits(root, evidence string, edits []edit) error {
	for i, e := range edits {
		if !filepath.IsLocal(e.Path) || !strings.HasSuffix(e.Path, ".go") || strings.HasSuffix(e.Path, "_test.go") || strings.HasPrefix(e.Path, "internal/oracle/") {
			return fmt.Errorf("invalid mutation source path %q", e.Path)
		}
		path := filepath.Join(root, e.Path)
		original, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		changed, err := editSource(original, e)
		if err != nil {
			return fmt.Errorf("mutation anchor in %s: %w", e.Path, err)
		}
		for suffix, content := range map[string][]byte{"before": original, "after": changed} {
			if err = os.WriteFile(filepath.Join(evidence, fmt.Sprintf("edit-%d.%s.txt", i, suffix)), content, 0644); err != nil {
				return err
			}
		}
		if err = os.WriteFile(path, changed, 0644); err != nil {
			return err
		}
	}
	return nil
}

// Reject links and special files, rather than following input outside the repo.
func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("source is not a regular file: %s", path)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		return os.WriteFile(target, content, 0644)
	})
}

func archiveSource(root, path string, hashes map[string]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	archive := zip.NewWriter(file)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		hashes[name] = fmt.Sprintf("%x", sha256.Sum256(content))
		writer, err := archive.Create(name)
		if err != nil {
			return err
		}
		_, err = writer.Write(content)
		return err
	})
	return errors.Join(err, archive.Close(), file.Close())
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
