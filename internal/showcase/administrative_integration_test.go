//go:build integration

package showcase

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/internal/admin"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func TestAdministrativeDrills(t *testing.T) {
	base := showcaseTestOptions(t)
	for _, name := range []string{"replay-intact", "full-rebuild"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			var transcript bytes.Buffer
			options := base
			options.Output = &transcript
			report, err := RunAdministrative(ctx, options, name)
			if name == "full-rebuild" && report.Namespace != "" {
				t.Cleanup(func() { cleanupRun(t, base.DatabaseURL, base.Brokers, report.Namespace+"-rebuilt") })
			}
			retainTestRun(t, base, report, &transcript)
			for _, child := range report.Processes {
				checkChildExit(t, child, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			if report.Mode != "correction-only" || report.Status != "PASS" || report.FinalVerification != "PASS" || len(report.Stages) != 2 {
				t.Fatalf("lost correction baseline: %+v", report)
			}
			var operation struct {
				Drill             string `json:"drill"`
				Status            string `json:"status"`
				OriginalPreserved bool   `json:"original_preserved_except_replay_epoch"`
			}
			readRunJSON(t, report.Directory, "operation.json", &operation)
			if operation.Drill != name || operation.Status != "PASS" || !operation.OriginalPreserved {
				t.Fatalf("administrative checks not retained: %+v", operation)
			}
			var original verify.Report
			readRunJSON(t, report.Directory, "original-after-operation.json", &original)
			if original.Status != "PASS" {
				t.Fatalf("original failed independent verification: %+v", original)
			}
			if name == "replay-intact" {
				var replay admin.ReplayReport
				readRunJSON(t, report.Directory, "replay.json", &replay)
				if replay.Status != "REPLAYED" || replay.Records != 4 {
					t.Fatalf("lost replay evidence: %+v", replay)
				}
			} else {
				var rebuild admin.RebuildReport
				readRunJSON(t, report.Directory, "rebuild.json", &rebuild)
				if rebuild.Status != "PASS" || rebuild.Manifest.Config.Namespace != report.Namespace+"-rebuilt" {
					t.Fatalf("lost isolated rebuild evidence: %+v", rebuild)
				}
			}
		})
	}
}
