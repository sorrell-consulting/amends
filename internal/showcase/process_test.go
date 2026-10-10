// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessHelper(t *testing.T) {
	if os.Getenv("AMENDS_SHOWCASE_PROCESS_HELPER") != "1" {
		return
	}
	if err := os.WriteFile(os.Getenv("AMENDS_SHOWCASE_HELPER_READY"), []byte("ready"), 0600); err != nil {
		os.Exit(2)
	}
	switch os.Getenv("AMENDS_SHOWCASE_HELPER_EXIT") {
	case "one":
		os.Exit(1)
	case "zero":
		os.Exit(0)
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestCanceledRunJoinsOwnedChild(t *testing.T) {
	t.Setenv("AMENDS_SHOWCASE_PROCESS_HELPER", "1")
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	t.Setenv("AMENDS_SHOWCASE_HELPER_READY", ready)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{options: Options{DatabaseURL: "postgres://synthetic@127.0.0.1/example"}, report: &Report{Directory: dir, Namespace: "process-test"}}
	p, err := r.start("owned", binary, false, "-test.run=^TestProcessHelper$")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.cleanup() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = r.until(ctx, "child readiness", func() (bool, error) { _, e := os.Stat(ready); return e == nil, nil }); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if p.wait(canceled) != context.Canceled {
		t.Fatal("wait ignored cancellation")
	}
	if err := p.expectExit(canceled, 1, func() error { return nil }); !errors.Is(err, context.Canceled) || p.finite || p.stopped {
		t.Fatalf("canceled expected exit lost cleanup ownership: %v", err)
	}
	// This helper uses the default SIGTERM action, so shutdown must be an
	// error, not a clean broker handoff. It must nevertheless be joined.
	if r.cleanup() == nil {
		t.Fatal("unclean exit accepted as a clean handoff")
	}
	select {
	case <-p.done:
	default:
		t.Fatal("cleanup left a child running")
	}
}

func TestExpectedProcessExit(t *testing.T) {
	t.Setenv("AMENDS_SHOWCASE_PROCESS_HELPER", "1")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, actual := range []string{"one", "zero"} {
		t.Run(actual, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("AMENDS_SHOWCASE_HELPER_READY", filepath.Join(dir, "ready"))
			t.Setenv("AMENDS_SHOWCASE_HELPER_EXIT", actual)
			r := &runner{report: &Report{Directory: dir, Namespace: "expected-exit-test"}}
			p, err := r.start("owned", binary, false, "-test.run=^TestProcessHelper$")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.cleanup() })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = p.expectExit(ctx, 1, r.healthy)
			if (err == nil) != (actual == "one") {
				t.Fatalf("expected exit 1, actual %s: %v", actual, err)
			}
			if !p.stopped || p.finite {
				t.Fatal("joined child retained temporary health exemption or cleanup ownership")
			}
			if err := r.cleanup(); err != nil {
				t.Fatalf("cleanup reinterpreted an already joined exit: %v", err)
			}
		})
	}
}
