package showcase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/admin"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

// RunAdministrative verifies a correction baseline, joins its owned processes,
// then demonstrates replay or rebuild against retained state. run.json describes
// the baseline; operation.json is written only after the administrative checks pass.
func RunAdministrative(ctx context.Context, options Options, name string) (Report, error) {
	if name != "replay-intact" && name != "full-rebuild" {
		return Report{}, fmt.Errorf("unsupported administrative scenario %q", name)
	}
	if options.Scenario != "" && options.Scenario != CorrectionOnly {
		return Report{}, fmt.Errorf("administrative scenario requires a correction-only baseline")
	}
	if options.Output == nil {
		options.Output = io.Discard
	}
	baseline, err := Run(ctx, options)
	if err != nil {
		return baseline, err
	}
	raw, err := os.ReadFile(filepath.Join(baseline.Directory, "manifest.json"))
	if err != nil {
		return baseline, err
	}
	retained := &runner{report: &baseline}
	manifest, err := broker.ReadManifest(bytes.NewReader(raw))
	if err != nil {
		return baseline, err
	}
	pool, err := pgxpool.New(ctx, options.DatabaseURL)
	if err != nil {
		return baseline, err
	}
	defer pool.Close()
	original, err := postgres.New(pool, manifest.Config)
	if err != nil {
		return baseline, err
	}
	before, err := original.Snapshot(ctx)
	if err != nil {
		return baseline, err
	}
	beforeView, err := original.View(ctx)
	if err != nil {
		return baseline, err
	}
	adminOptions := admin.Options{Brokers: options.Brokers, Manifest: manifest, Pool: pool, Execute: true, Quiesce: true}
	checked := original
	if name == "replay-intact" {
		report, operationErr := admin.Replay(ctx, adminOptions, 0, 0, before.Partitions[0].Next)
		if err = retained.save("replay.json", report); err != nil {
			return baseline, errors.Join(operationErr, err)
		}
		if operationErr != nil {
			return baseline, operationErr
		}
		if report.Status != "REPLAYED" || report.Records != 4 || report.Epoch != before.Partitions[0].Epoch+1 {
			return baseline, fmt.Errorf("replay drill fence/count assertion failed")
		}
		partition := before.Partitions[0]
		partition.Epoch = report.Epoch
		before.Partitions[0] = partition
	} else {
		report, operationErr := admin.Rebuild(ctx, adminOptions, baseline.Namespace+"-rebuilt", baseline.Namespace+"-rebuilt-output", func(destination broker.Manifest) error {
			return retained.save("rebuilt-manifest.json", destination)
		})
		if err = retained.save("rebuild.json", report); err != nil {
			return baseline, errors.Join(operationErr, err)
		}
		if operationErr != nil {
			return baseline, operationErr
		}
		if report.Status != "PASS" {
			return baseline, fmt.Errorf("rebuild drill requires strict PASS, got %s", report.Status)
		}
		checked, err = postgres.New(pool, report.Manifest.Config)
		if err != nil {
			return baseline, err
		}
	}
	after, err := original.Snapshot(ctx)
	if err != nil {
		return baseline, err
	}
	afterView, err := original.View(ctx)
	if err != nil {
		return baseline, err
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeView, afterView) {
		return baseline, fmt.Errorf("original state, outputs, or view changed unexpectedly")
	}
	actual, err := checked.View(ctx)
	if err != nil {
		return baseline, err
	}
	expected, err := fixtures.Expected("expected-after.json")
	if err != nil {
		return baseline, err
	}
	projection := actual.Projection(expected.Key)
	if projection.Status != expected.Status || !slices.Equal(projection.Reasons, expected.Reasons) || !slices.Equal(projection.Balances, expected.Balances) || !slices.Equal(projection.Alerts, expected.Alerts) {
		return baseline, fmt.Errorf("drill differs from hand-derived corrected fixture")
	}
	verification := verify.Run(ctx, verify.Options{Brokers: options.Brokers, Manifest: manifest, Store: original, Quiesce: true, Timeout: time.Minute})
	if err = retained.save("original-after-operation.json", verification); err != nil {
		return baseline, err
	}
	if verification.Status != "PASS" {
		return baseline, fmt.Errorf("original namespace requires strict PASS, got %s", verification.Status)
	}
	if err = retained.save("operation.json", struct {
		Drill             string `json:"drill"`
		Status            string `json:"status"`
		OriginalPreserved bool   `json:"original_preserved_except_replay_epoch"`
	}{name, "PASS", true}); err != nil {
		return baseline, err
	}
	_, err = fmt.Fprintf(options.Output, "%s PASS; hand-derived fixture and original namespace checked.\nArtifacts: %s\n", name, baseline.Directory)
	return baseline, err
}
