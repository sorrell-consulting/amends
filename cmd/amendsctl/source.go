// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
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
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
)

func runSource(command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	brokers := f.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	var namespace, topic, manifest, file *string
	var partitions *int
	if command == "source-create" {
		namespace = f.String("namespace", "", "fresh namespace (required)")
		topic = f.String("topic", "", "fresh source topic (required)")
		partitions = f.Int("partitions", 2, "topic partitions; demo key maps to partition zero")
		f.Usage = func() {
			fmt.Fprintln(stderr, "Usage: amendsctl source-create -namespace NAME -topic NAME\nCreates a retained source topic and writes a JSON manifest to stdout. Save it for workers.")
			f.PrintDefaults()
		}
	} else {
		manifest = f.String("manifest", "", "saved source manifest (required)")
		file = f.String("file", "", "transaction JSONL file (required)")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || (command == "source-create" && (*namespace == "" || *topic == "" || *partitions < 1)) || (command == "source-load" && (*manifest == "" || *file == "")) {
		fmt.Fprintln(stderr, "missing/invalid source options; use "+command+" -h")
		return 2
	}
	if command == "source-load" && os.Getenv("AMENDS_DATABASE_URL") == "" {
		fmt.Fprintln(stderr, "source-load requires AMENDS_DATABASE_URL for the source-incarnation producer guard")
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var err error
	if command == "source-load" {
		err = loadSource(ctx, strings.Split(*brokers, ","), *manifest, *file, stdout)
	} else {
		var cl *kgo.Client
		cl, err = broker.Client(strings.Split(*brokers, ","))
		if err == nil {
			defer cl.Close()
			bounded, done := context.WithTimeout(ctx, 20*time.Second)
			defer done()
			err = createSource(bounded, cl, *namespace, *topic, *partitions, stdout)
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl %s: %v\n", command, err)
		return 1
	}
	return 0
}

func createSource(ctx context.Context, cl *kgo.Client, namespace, topic string, partitions int, output io.Writer) error {
	c, err := fixtures.Config(namespace)
	if err != nil {
		return err
	}
	s, err := broker.CreateSource(ctx, cl, topic, partitions)
	if err != nil {
		return fmt.Errorf("create %s (on an unknown result, inspect this topic before choosing a new namespace/topic): %w", topic, err)
	}
	c.Source, c.Partitions = s.ID, s.Partitions
	if _, err = broker.Validate(ctx, cl, topic, c); err != nil {
		return err
	}
	enc := json.NewEncoder(output)
	enc.SetIndent("", "  ")
	return enc.Encode(broker.Manifest{Topic: topic, Config: c})
}

func loadSource(ctx context.Context, brokers []string, manifestPath, filePath string, output io.Writer) error {
	f, err := os.Open(manifestPath)
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
	pool, err := pgxpool.New(ctx, os.Getenv("AMENDS_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err = postgres.Migrate(ctx, pool); err != nil {
		return err
	}
	state, err := postgres.New(pool, m.Config)
	if err != nil {
		return err
	}
	if err = state.Ensure(ctx); err != nil {
		return err
	}
	producer, err := state.AcquireService(ctx, "producer")
	if err != nil {
		return fmt.Errorf("source writer not admitted: %w", err)
	}
	defer producer.Close()
	cl, err := broker.Client(brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(15*time.Second))
	if err != nil {
		return err
	}
	defer cl.Close() // Join the owned client before releasing the producer guard.
	f, err = os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	// Validate the finite file before sending any records. Source-load is an
	// owned, nontransactional fixture producer, not a quarantine repair command.
	var records []*kgo.Record
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		raw := append([]byte(nil), scanner.Bytes()...)
		t, err := ledger.Decode(raw)
		if err != nil {
			return fmt.Errorf("input line %d: %w", len(records)+1, err)
		}
		key, ok := m.Config.Key(t.StockKey)
		if !ok {
			return fmt.Errorf("input line %d: unconfigured key", len(records)+1)
		}
		records = append(records, &kgo.Record{Topic: m.Topic, Partition: int32(key.Partition), Key: broker.EncodeKey(t.StockKey), Value: raw})
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	for _, record := range records {
		bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = producer.Check(bounded)
		if err == nil {
			_, err = broker.Validate(bounded, cl, m.Topic, m.Config)
		}
		if err == nil {
			err = cl.ProduceSync(bounded, record).FirstErr()
		}
		cancel()
		if err != nil {
			return fmt.Errorf("publication failed or outcome unknown; a retry may redeliver identical revisions: %w", err)
		}
		if _, err = fmt.Fprintf(output, "published %s/%d offset %d\n", m.Topic, record.Partition, record.Offset); err != nil {
			return err
		}
	}
	return nil
}
