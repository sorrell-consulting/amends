//go:build integration

package showcase

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

func showcaseTestOptions(t *testing.T) Options {
	t.Helper()
	o := Options{DatabaseURL: os.Getenv("AMENDS_TEST_DATABASE_URL"), Brokers: strings.Split(os.Getenv("AMENDS_TEST_BROKERS"), ","), ArtifactParent: os.Getenv("AMENDS_SHOWCASE_ARTIFACT_DIR")}
	if o.DatabaseURL == "" || o.Brokers[0] == "" {
		t.Fatal("requires AMENDS_TEST_DATABASE_URL and AMENDS_TEST_BROKERS; use make test-showcase")
	}
	var err error
	o.ControlBinary, err = filepath.Abs("../../bin/amendsctl")
	if err != nil {
		t.Fatal(err)
	}
	o.WorkerBinary = filepath.Join(filepath.Dir(o.ControlBinary), "amends")
	if o.ArtifactParent == "" {
		o.ArtifactParent = "../../artifacts/showcase"
	}
	return o
}

func retainTestRun(t *testing.T, o Options, report Report, transcript *bytes.Buffer) {
	t.Helper()
	t.Logf("artifacts: %s\n%s", report.Directory, transcript.String())
	if report.Namespace != "" {
		t.Cleanup(func() { cleanupRun(t, o.DatabaseURL, o.Brokers, report.Namespace) })
	}
	if report.Directory == "" {
		t.Fatalf("missing artifacts: %+v", report)
	}
	path := filepath.Join(report.Directory, "transcript.txt")
	if err := os.WriteFile(path, transcript.Bytes(), 0600); err != nil {
		t.Fatalf("save %s: %v", path, err)
	}
	var saved Report
	readRunJSON(t, report.Directory, "run.json", &saved)
	if !reflect.DeepEqual(saved, report) {
		t.Fatalf("%s differs from returned report", filepath.Join(report.Directory, "run.json"))
	}
}

func readRunJSON(t *testing.T, directory, name string, target any) {
	t.Helper()
	path := filepath.Join(directory, name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err = json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

// Expected exits belong to the scenario, including signals and verifier refusals.
func checkChildExit(t *testing.T, child Process, want int) {
	t.Helper()
	if !child.Joined || syscall.Kill(child.PID, 0) != syscall.ESRCH {
		t.Errorf("owned child not joined or still exists: %+v", child)
	}
	if child.ExitCode != want {
		t.Errorf("child %s exit=%d; want %d", child.Name, child.ExitCode, want)
	}
}

type checkpointCancellation struct {
	point  Checkpoint
	cancel context.CancelFunc
	fired  bool
}

func newCheckpointCancellation(t *testing.T) *checkpointCancellation {
	t.Helper()
	c := &checkpointCancellation{}
	t.Cleanup(func() {
		if c.cancel != nil && !c.fired {
			t.Errorf("cancellation checkpoint %q was not reached", c.point)
		}
	})
	return c
}

func (c *checkpointCancellation) observe(point Checkpoint) {
	if c.cancel != nil && point == c.point {
		c.fired = true
		c.cancel()
	}
}
