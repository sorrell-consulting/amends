//go:build integration

package delivery

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/sorrell-consulting/amends/internal/verify"
	"github.com/twmb/franz-go/pkg/kgo"
)

type failedObservation struct {
	prefix string
	reads  atomic.Int32
}

func (failure *failedObservation) TraceQueryStart(ctx context.Context, _ *pgx.Conn, query pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(query.SQL, failure.prefix) && len(query.Args) == 2 && query.Args[1] == 1 && failure.reads.Add(1) == 4 {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return canceled
	}
	return ctx
}

func (*failedObservation) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestVerificationRetainsCompleteObservationsOnReadFailure(test *testing.T) {
	environment := setup(test, 2)
	start(test, environment.worker)
	start(test, environment.worker)
	relay := start(test, func(ctx context.Context) error { return RunRelay(ctx, environment.Options) })
	materializer := start(test, func(ctx context.Context) error { return RunView(ctx, environment.Options) })
	history := environment.publish(test, "before.jsonl")
	environment.awaitView(test, 5)
	conflict, err := ledger.Decode(history[3].Value)
	if err != nil {
		test.Fatal(err)
	}
	conflict.Qty = -40
	publish := func(transaction ledger.Transaction) {
		test.Helper()
		raw, err := json.Marshal(transaction)
		if err != nil {
			test.Fatal(err)
		}
		record := &kgo.Record{Topic: environment.Manifest.Topic, Partition: 1, Key: broker.EncodeKey(transaction.StockKey), Value: raw}
		if err = environment.cl.ProduceSync(test.Context(), record).FirstErr(); err != nil {
			test.Fatal(err)
		}
	}
	publish(conflict)
	eventually(test, func() bool {
		view, err := environment.Store.View(test.Context())
		if err != nil {
			test.Fatal(err)
		}
		return view.Next[1] == 6 && view.Projection(conflict.StockKey).Status == "BLOCKED"
	})
	materializer.stop(test)
	conflict.Revision, conflict.Qty = 2, -30
	publish(conflict)
	eventually(test, func() bool {
		progress, err := environment.Store.Progress(test.Context())
		if err != nil {
			test.Fatal(err)
		}
		return progress.Partitions[1].SourceNext == 5 && progress.Partitions[1].Pending == 0
	})
	relay.stop(test)
	for _, selection := range []struct{ name, prefix string }{
		{"progress", "SELECT p.epoch"},
		{"view", "SELECT next_offset,protocol_error FROM amends_view_progress"},
	} {
		test.Run(selection.name, func(test *testing.T) {
			failure := &failedObservation{prefix: selection.prefix}
			config := environment.pool.Config()
			config.ConnConfig.Tracer = failure
			pool, err := pgxpool.NewWithConfig(test.Context(), config)
			if err != nil {
				test.Fatal(err)
			}
			defer pool.Close()
			state, err := postgres.New(pool, environment.Manifest.Config)
			if err != nil {
				test.Fatal(err)
			}
			report := verify.Run(test.Context(), verify.Options{Brokers: environment.Brokers, Manifest: environment.Manifest, Store: state, Quiesce: true, Timeout: 5 * time.Second})
			requireStatus(test, report, "ERROR")
			if failure.reads.Load() != 4 || !strings.Contains(report.Reason, "context canceled") || !report.Checks.PublicationDrained || report.Checks.ViewReached || report.Checks.BoundariesStable || len(report.Progress.Partitions) != 2 || report.Progress.Partitions[0].ViewNext != 5 || report.Progress.Partitions[1].ViewNext != 6 {
				test.Fatalf("failed partial %s observation replaced last complete progress: %+v, reads=%d", selection.name, report, failure.reads.Load())
			}
			found := false
			for _, key := range report.Keys {
				if key.Key == conflict.StockKey {
					found = true
					if key.AuthorityStatus != "READY" || key.ViewStatus != "BLOCKED" || key.Compared || key.Agrees {
						test.Fatalf("failed read discarded the last complete BLOCKED view observation: %+v", key)
					}
				}
			}
			if !found {
				test.Fatal("failed read lost the repaired key")
			}
		})
	}
}
