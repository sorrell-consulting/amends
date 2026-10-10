// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Command amends runs a source worker, relay, or durable view consumer.
// Two source instances exercise assignment and PostgreSQL epoch replacement;
// relay and view each run as a single service for their namespace.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/delivery"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/worker"
)

func main() {
	manifest := flag.String("manifest", "", "manifest from amendsctl source-create or output-create (required)")
	brokers := flag.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	role := flag.String("role", "source", "service role: source, relay, or view")
	logFormat := flag.String("log-format", "text", "log format: text or json")
	flag.Parse()
	if *logFormat != "text" && *logFormat != "json" {
		fmt.Fprintln(os.Stderr, "-log-format must be text or json")
		os.Exit(2)
	}
	if *logFormat == "json" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	}
	if *manifest == "" || flag.NArg() != 0 || os.Getenv("AMENDS_DATABASE_URL") == "" || (*role != "source" && *role != "relay" && *role != "view") {
		fmt.Fprintln(os.Stderr, "amends requires -manifest FILE and AMENDS_DATABASE_URL; use -h for options")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *role, *manifest, strings.Split(*brokers, ","), os.Getenv("AMENDS_DATABASE_URL")); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("service stopped; restart after resolving the reported condition", "role", *role, "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, role, path string, brokers []string, url string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	m, err := broker.ReadManifest(f)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	init, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err = postgres.Migrate(init, pool); err != nil {
		return err
	}
	s, err := postgres.New(pool, m.Config)
	if err != nil {
		return err
	}
	if err = s.Ensure(init); err != nil {
		return err
	}
	o := delivery.Options{Brokers: brokers, Manifest: m, Store: s}
	if role == "relay" {
		return delivery.RunRelay(ctx, o)
	}
	if role == "view" {
		return delivery.RunView(ctx, o)
	}
	slog.Info("starting source worker", "namespace", m.Config.Namespace, "topic", m.Topic, "source", m.Config.Source, "group", worker.Group(m.Config.Namespace))
	return worker.Run(ctx, worker.Options{Brokers: brokers, Topic: m.Topic, Config: m.Config, State: s})
}
