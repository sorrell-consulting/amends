// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

func runOutput(command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	path := f.String("manifest", "", "source/pipeline manifest (required)")
	topic := f.String("topic", "", "new output topic (required for output-create)")
	brokers := f.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || *path == "" || (command == "output-create" && *topic == "") || os.Getenv("AMENDS_DATABASE_URL") == "" {
		fmt.Fprintln(stderr, "requires -manifest FILE, AMENDS_DATABASE_URL, and -topic NAME for output-create")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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
		pool, err := pgxpool.New(ctx, os.Getenv("AMENDS_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer pool.Close()
		s, err := postgres.New(pool, m.Config)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if command == "view-show" {
			v, err := s.View(ctx)
			if err != nil {
				return err
			}
			rows := make([]ledger.Envelope, 0, len(v.Rows))
			for _, e := range v.Rows {
				rows = append(rows, e)
			}
			slices.SortFunc(rows, func(a, b ledger.Envelope) int { return strings.Compare(a.ID.String(), b.ID.String()) })
			projections := make([]ledger.Projection, 0, len(m.Config.Keys))
			for _, k := range m.Config.Keys {
				projections = append(projections, v.Projection(k.StockKey))
			}
			return enc.Encode(struct {
				Scope     string              `json:"scope"`
				Namespace string              `json:"namespace"`
				Next      map[int]int64       `json:"view_next"`
				Errors    map[int]string      `json:"protocol_errors"`
				Rows      []ledger.Envelope   `json:"rows_including_tombstones"`
				Keys      []ledger.Projection `json:"keys"`
			}{"live-view-only; not a verification result", m.Config.Namespace, v.Next, v.Errors, rows, projections})
		}
		if err = postgres.Migrate(ctx, pool); err != nil {
			return err
		}
		if err = s.Ensure(ctx); err != nil {
			return err
		}
		cl, err := broker.Client(strings.Split(*brokers, ","))
		if err != nil {
			return err
		}
		defer cl.Close()
		if _, err = broker.Validate(ctx, cl, m.Topic, m.Config); err != nil {
			return err
		}
		binding, err := s.OutputBinding(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			if m.Output != nil || *topic == m.Topic {
				return fmt.Errorf("output must be a fresh distinct topic")
			}
			created, err := broker.CreateSource(ctx, cl, *topic, m.Config.Partitions)
			if err != nil {
				return fmt.Errorf("output topic creation (may require inspection after an unknown result): %w", err)
			}
			binding = store.OutputBinding{Topic: created.Topic, Incarnation: created.ID, Partitions: created.Partitions}
			ends, err := broker.ValidateOutput(ctx, cl, binding, m.Config)
			if err != nil {
				return err
			}
			for _, end := range ends {
				if end != 0 {
					return fmt.Errorf("new output topic is not empty")
				}
			}
			if err = s.BindOutput(ctx, binding); errors.Is(err, store.ErrCommitUnknown) {
				got, readErr := s.OutputBinding(ctx)
				if readErr != nil || got != binding {
					return fmt.Errorf("output binding commit unresolved: %w", err)
				}
			} else if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if binding.Topic != *topic || (m.Output != nil && *m.Output != binding) {
			return store.ErrFence
		}
		if _, err = broker.ValidateOutput(ctx, cl, binding, m.Config); err != nil {
			return err
		}
		m.Output = &binding
		return enc.Encode(m)
	}()
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl %s: %v\n", command, err)
		return 1
	}
	return 0
}
