// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func runOperator(command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	path := f.String("manifest", "", "saved source/pipeline manifest (required)")
	brokers := f.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	timeout := f.Duration("timeout", 30*time.Second, "bounded operation time")
	var quiesce *bool
	var expected *string
	format := "json"
	if command == "inspect" {
		f.StringVar(&format, "format", "json", "json or html (read-only snapshot, not verification)")
	}
	if command == "verify" {
		quiesce = f.Bool("quiesce", false, "required: finish owned source loading and stop/join relay; exclude new publication during comparison")
		expected = f.String("expected-diagnostics", "", "exact named diagnostics fixture; never suppresses unknown exclusions")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || *path == "" || *timeout <= 0 || os.Getenv("AMENDS_DATABASE_URL") == "" || (command == "verify" && !*quiesce) {
		fmt.Fprintln(stderr, "requires -manifest FILE, AMENDS_DATABASE_URL, a positive timeout, and --quiesce for verify")
		return 2
	}
	if format != "json" && format != "html" {
		fmt.Fprintln(stderr, "inspect -format must be json or html")
		return 2
	}
	code := 1
	err := func() error {
		file, err := os.Open(*path)
		if err != nil {
			return err
		}
		m, err := broker.ReadManifest(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		pool, err := pgxpool.New(ctx, os.Getenv("AMENDS_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer pool.Close()
		state, err := postgres.New(pool, m.Config)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if command == "inspect" {
			bounded, done := context.WithTimeout(ctx, *timeout)
			defer done()
			result, err := inspect(bounded, strings.Split(*brokers, ","), m, state)
			if err != nil {
				return err
			}
			if format == "html" {
				err = renderDashboard(stdout, result)
			} else {
				err = enc.Encode(result)
			}
			if err != nil {
				return err
			}
			code = 0
			return nil
		}
		var fixture *verify.ExpectedDiagnostics
		if *expected != "" {
			file, err := os.Open(*expected)
			if err != nil {
				return err
			}
			fixture, err = verify.ReadExpected(file)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		// Builds omit VCS stamping. Hash the executable to identify the code used
		// for this report, including local edits.
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		binary, err := os.Open(executable)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, err = io.Copy(hash, binary)
		closeErr = binary.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		report := verify.Run(ctx, verify.Options{Brokers: strings.Split(*brokers, ","), Manifest: m, Store: state, Timeout: *timeout, Quiesce: *quiesce})
		report.BinarySHA256 = hex.EncodeToString(hash.Sum(nil))
		accepted := report.Accepts(fixture)
		if err = enc.Encode(report); err != nil {
			return err
		}
		if accepted {
			code = 0
		}
		return nil
	}()
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl %s: %v\n", command, err)
		return 1
	}
	return code
}

type inspectedPartition struct {
	postgres.PartitionProgress
	SourceEnd         *int64   `json:"observed_source_end"`
	OutputEnd         *int64   `json:"observed_output_end"`
	InputLag          *int64   `json:"input_offset_distance"`
	ViewLag           *int64   `json:"view_offset_distance"`
	PendingAgeSeconds *float64 `json:"oldest_pending_age_seconds"`
}

type inspection struct {
	Scope               string               `json:"scope"`
	Namespace           string               `json:"namespace"`
	ConfigDigest        string               `json:"config_digest"`
	SourceTopic         string               `json:"source_topic"`
	SourceIncarnation   string               `json:"source_incarnation"`
	OutputTopic         string               `json:"output_topic,omitempty"`
	OutputIncarnation   string               `json:"output_incarnation,omitempty"`
	ObservedAt          time.Time            `json:"progress_observed_at"`
	WorkerReadAt        time.Time            `json:"worker_snapshot_read_at"`
	ViewReadAt          time.Time            `json:"view_snapshot_read_at"`
	Partitions          []inspectedPartition `json:"partitions"`
	WorkerKeys          []ledger.Projection  `json:"worker_keys"`
	ViewKeys            []ledger.Projection  `json:"view_keys"`
	Conflicts           []ledger.Conflict    `json:"conflicts"`
	ConflictDetails     []reviewedConflict   `json:"conflict_details"`
	Rejections          []ledger.Rejection   `json:"quarantine"`
	ObservationalErrors []string             `json:"observation_errors"`
}

func inspect(ctx context.Context, seeds []string, m broker.Manifest, s *postgres.Store) (inspection, error) {
	r := inspection{Scope: "live-observations; not a verification result", Namespace: m.Config.Namespace, ConfigDigest: m.Config.Digest(), SourceTopic: m.Topic, SourceIncarnation: m.Config.Source}
	if m.Output != nil {
		r.OutputTopic, r.OutputIncarnation = m.Output.Topic, m.Output.Incarnation
	}
	progress, err := s.Progress(ctx)
	if err != nil {
		return r, err
	}
	r.ObservedAt = progress.ObservedAt
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return r, err
	}
	r.WorkerReadAt = time.Now().UTC()
	r.ConflictDetails = reviewConflicts(snapshot)
	v, err := s.View(ctx)
	if err != nil {
		return r, err
	}
	r.ViewReadAt = time.Now().UTC()
	for _, key := range m.Config.Keys {
		state := snapshot.Keys[key.StockKey]
		r.WorkerKeys = append(r.WorkerKeys, state.Projection)
		r.ViewKeys = append(r.ViewKeys, v.Projection(key.StockKey))
		r.Conflicts = append(r.Conflicts, state.Conflicts()...)
	}
	r.Rejections = snapshot.Rejections
	cl, err := broker.Client(seeds)
	if err != nil {
		return r, err
	}
	defer cl.Close()
	source, err := broker.Validate(ctx, cl, m.Topic, m.Config)
	if err != nil {
		r.ObservationalErrors = append(r.ObservationalErrors, err.Error())
		source = nil
	}
	var output []int64
	if m.Output != nil {
		bound, err := s.OutputBinding(ctx)
		if err == nil && bound != *m.Output {
			err = store.ErrFence
		}
		if err == nil {
			output, err = broker.ValidateOutput(ctx, cl, *m.Output, m.Config)
		}
		if err != nil {
			r.ObservationalErrors = append(r.ObservationalErrors, err.Error())
			output = nil
		}
	}
	for _, p := range progress.Partitions {
		row := inspectedPartition{PartitionProgress: p}
		if source != nil {
			end := source[p.Partition]
			lag := end - p.SourceNext
			row.SourceEnd = &end
			row.InputLag = &lag
		}
		if output != nil {
			end := output[p.Partition]
			lag := end - p.ViewNext
			row.OutputEnd = &end
			row.ViewLag = &lag
		}
		if p.OldestPending != nil {
			age := max(0, progress.ObservedAt.Sub(*p.OldestPending).Seconds())
			row.PendingAgeSeconds = &age
		}
		r.Partitions = append(r.Partitions, row)
	}
	return r, nil
}
