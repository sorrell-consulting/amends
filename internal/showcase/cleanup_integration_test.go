//go:build integration

// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCleanupRunRemovesOnlyOwnedTopics(t *testing.T) {
	dsn, brokers := os.Getenv("AMENDS_TEST_DATABASE_URL"), strings.Split(os.Getenv("AMENDS_TEST_BROKERS"), ",")
	if dsn == "" || brokers[0] == "" {
		t.Fatal("requires AMENDS_TEST_DATABASE_URL and AMENDS_TEST_BROKERS; use make test-showcase")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = postgres.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	client, err := broker.Client(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	ns := "amends-cleanup-" + hex.EncodeToString(nonce[:])
	names := []string{ns + "-source", ns + "-output", ns + "-unrelated"}
	// Independently remove this test's fixtures even if cleanupRun regresses.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		request := kmsg.NewDeleteTopicsRequest()
		for _, name := range names {
			topic := kmsg.NewDeleteTopicsRequestTopic()
			topic.Topic = kmsg.StringPtr(name)
			request.Topics = append(request.Topics, topic)
		}
		response, err := request.RequestWith(cleanupCtx, client)
		if err != nil {
			t.Error(err)
			return
		}
		for _, topic := range response.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
				t.Error(err)
			}
		}
	}()
	var unrelated broker.Source
	for _, name := range names {
		source, err := broker.CreateSource(ctx, client, name, 1)
		if err != nil {
			t.Fatal(err)
		}
		if name == names[2] {
			unrelated = source
		}
	}
	cleanupRun(t, dsn, brokers, ns)
	for _, name := range names[:2] {
		if _, err := broker.Inspect(ctx, client, name); !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Errorf("cleanup left topic %s: %v", name, err)
		}
	}
	cleanupRun(t, dsn, brokers, ns) // Already absent topics are safe to clean again.
	if source, err := broker.Inspect(ctx, client, names[2]); err != nil || source != unrelated {
		t.Fatalf("cleanup changed unrelated topic: %+v error=%v; want %+v", source, err, unrelated)
	}
}
