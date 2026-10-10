//go:build integration

package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/delivery"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type environment struct {
	Options
	state      *postgres.Store
	client     *kgo.Client
	namespaces []string
	topics     []string
}

func setup(t *testing.T, extra string) *environment {
	t.Helper()
	database, seeds := os.Getenv("AMENDS_TEST_DATABASE_URL"), os.Getenv("AMENDS_TEST_BROKERS")
	if database == "" || seeds == "" {
		t.Fatal("requires PostgreSQL and Redpanda; use make test-admin")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = postgres.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	client, err := broker.Client(strings.Split(seeds, ","), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "amends-admin-" + hex.EncodeToString(nonce[:])
	environment := &environment{Options: Options{Pool: pool, Brokers: strings.Split(seeds, ","), Quiesce: true}, client: client}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		request := kmsg.NewDeleteTopicsRequest()
		for _, topic := range environment.topics {
			entry := kmsg.NewDeleteTopicsRequestTopic()
			entry.Topic = kmsg.StringPtr(topic)
			request.Topics = append(request.Topics, entry)
		}
		if len(request.Topics) > 0 {
			response, err := request.RequestWith(cleanup, client)
			if err != nil {
				t.Error(err)
			} else {
				for _, topic := range response.Topics {
					if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
						t.Error(err)
					}
				}
			}
		}
		for _, namespace := range environment.namespaces {
			raw, _ := json.Marshal(namespace)
			for _, table := range []string{"amends_view_rows", "amends_view_progress", "amends_quarantine", "amends_outbox", "amends_output_state", "amends_key_state", "amends_partitions", "amends_namespaces"} {
				if _, err := pool.Exec(cleanup, "DELETE FROM "+table+" WHERE namespace_id=$1", string(raw)); err != nil {
					t.Error(err)
					break
				}
			}
		}
	})
	source, err := broker.CreateSource(ctx, client, name+"-source", 2)
	if err != nil {
		t.Fatal(err)
	}
	environment.topics = append(environment.topics, source.Topic)
	output, err := broker.CreateSource(ctx, client, name+"-output", 2)
	if err != nil {
		t.Fatal(err)
	}
	environment.topics = append(environment.topics, output.Topic)
	config, err := fixtures.Config(name)
	if err != nil {
		t.Fatal(err)
	}
	config.Source, config.Partitions = source.ID, 2
	environment.Manifest = broker.Manifest{Topic: source.Topic, Config: config, Output: &store.OutputBinding{Topic: output.Topic, Incarnation: output.ID, Partitions: 2}}
	environment.namespaces = append(environment.namespaces, name)
	environment.state, err = postgres.New(pool, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = environment.state.CreateFresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err = environment.state.BindOutput(ctx, *environment.Manifest.Output); err != nil {
		t.Fatal(err)
	}
	records, err := fixtures.Records(config, "before.jsonl", "correction.jsonl", "cancel-last-day.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if extra != "" {
		record := records[0].Clone()
		if extra == "poison" {
			record.Value = []byte("{")
		} else {
			transaction, err := ledger.Decode(records[3].Value)
			if err != nil {
				t.Fatal(err)
			}
			transaction.Qty = -90
			record.Value, err = json.Marshal(transaction)
			if err != nil {
				t.Fatal(err)
			}
		}
		records = append(records, record)
	}
	for _, record := range records {
		if err = client.ProduceSync(ctx, &kgo.Record{Topic: source.Topic, Partition: 0, Key: broker.EncodeKey(record.Key), Value: record.Value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	ends, err := broker.Validate(ctx, client, source.Topic, config)
	if err != nil {
		t.Fatal(err)
	}
	history, err := broker.Scan(ctx, environment.Brokers, environment.Manifest, ends)
	if err != nil {
		t.Fatal(err)
	}
	token, err := environment.state.Claim(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(0)
	for _, record := range history {
		if err = environment.state.Process(ctx, token, frontier, record); err != nil {
			t.Fatal(err)
		}
		frontier = record.Offset + 1
	}
	options := delivery.Options{Brokers: environment.Brokers, Manifest: environment.Manifest, Store: environment.state, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err = delivery.DrainRelay(ctx, options); err != nil {
		t.Fatal(err)
	}
	if err = materialize(ctx, options); err != nil {
		t.Fatal(err)
	}
	return environment
}

func (environment *environment) destination(suffix string) (string, string) {
	namespace := environment.Manifest.Config.Namespace + "-" + suffix
	topic := namespace + "-output"
	environment.namespaces = append(environment.namespaces, namespace)
	environment.topics = append(environment.topics, topic)
	return namespace, topic
}

// These tests own and join every writer. Local session close does not await
// PostgreSQL backend cleanup, so observe producer-lock release before starting
// the next administrative operation. Do not retry the operation itself: errors
// after mutation must remain visible to the test.
func (environment *environment) awaitProducerRelease(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sum := sha256.Sum256([]byte(environment.Manifest.Config.Source + "/producer"))
	bits := binary.BigEndian.Uint64(sum[:8])
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var held bool
		err := environment.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype='advisory'
			AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1 AND granted
		)`, int64(bits>>32), int64(uint32(bits))).Scan(&held)
		if err != nil {
			t.Fatalf("observe stopped producer lock release: %v", err)
		}
		if !held {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stopped producer lock was not released: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestSC23_DurableReplayAndRefusedRanges(t *testing.T) {
	environment := setup(t, "poison")
	before, err := environment.state.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	frontier := before.Partitions[0].Next
	report, err := Replay(t.Context(), environment.Options, 0, 0, frontier)
	if err != nil || report.Status != "DRY_RUN" || report.Records != 6 {
		t.Fatalf("dry run: %+v %v", report, err)
	}
	after, err := environment.state.Snapshot(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("dry run changed state", err)
	}
	environment.Execute = true
	environment.awaitProducerRelease(t)
	report, err = Replay(t.Context(), environment.Options, 0, 0, frontier)
	if err != nil || report.Status != "REPLAYED" || report.Frontier != frontier {
		t.Fatalf("replay: %+v %v", report, err)
	}
	partition := before.Partitions[0]
	partition.Epoch++
	before.Partitions[0] = partition
	after, err = environment.state.Snapshot(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("replay changed more than ownership", err)
	}
	environment.awaitProducerRelease(t)
	if _, err = Replay(t.Context(), environment.Options, 0, 0, frontier+1); err == nil {
		t.Fatal("unprocessed range accepted")
	}
	incompatible := environment.Options
	incompatible.Manifest.Config = incompatible.Manifest.Config.Clone()
	incompatible.Manifest.Config.Source = "kafka:11111111111111111111111111111111"
	environment.awaitProducerRelease(t)
	if _, err = Replay(t.Context(), incompatible, 0, 0, frontier); !errors.Is(err, broker.ErrBoundary) {
		t.Fatalf("recreated source accepted: %v", err)
	}
}

func TestSC24_RebuildAndPreserveOriginal(t *testing.T) {
	environment := setup(t, "")
	before, err := environment.state.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	beforeView, err := environment.state.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	namespace, topic := environment.destination("rebuilt")
	report, err := Rebuild(t.Context(), environment.Options, namespace, topic, nil)
	if err != nil || report.Status != "DRY_RUN" {
		t.Fatalf("dry run: %+v %v", report, err)
	}
	destination, err := postgres.New(environment.Pool, report.Manifest.Config)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := destination.Exists(t.Context()); err != nil || exists {
		t.Fatalf("dry run created namespace: %v %v", exists, err)
	}
	environment.Execute = true
	environment.awaitProducerRelease(t)
	report, err = Rebuild(t.Context(), environment.Options, namespace, topic, func(broker.Manifest) error { return nil })
	if err != nil || report.Status != "PASS" || report.Verification == nil || !slices.Equal(report.SourceEnd, []int64{5, 0}) {
		t.Fatalf("rebuild: %+v %v", report, err)
	}
	after, err := environment.state.Snapshot(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rebuild changed original processing state", err)
	}
	afterView, err := environment.state.View(t.Context())
	if err != nil || !reflect.DeepEqual(beforeView, afterView) {
		t.Fatal("rebuild changed original view", err)
	}
	view, err := destination.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	expected, err := fixtures.Expected("expected-after-cancel.json")
	if err != nil {
		t.Fatal(err)
	}
	actual := view.Projection(expected.Key)
	if actual.Status != "READY" || !slices.Equal(actual.Balances, expected.Balances) || !slices.Equal(actual.Alerts, expected.Alerts) {
		t.Fatalf("rebuild differs from hand-derived fixture: %+v", actual)
	}
	for identity := range view.Rows {
		if identity.Namespace != namespace {
			t.Fatal("rebuild reused old output identity")
		}
	}
	environment.awaitProducerRelease(t)
	if _, err = Rebuild(t.Context(), environment.Options, namespace, topic, func(broker.Manifest) error { return nil }); err == nil {
		t.Fatal("existing rebuild destination adopted")
	}
	for _, envelope := range beforeView.Rows {
		if err := destination.ApplyViewInput(t.Context(), view.Next[0], store.ViewInput{Partition: 0, Offset: view.Next[0], Envelope: envelope}); !errors.Is(err, store.ErrProtocol) {
			t.Fatalf("old namespace output accepted: %v", err)
		}
		break
	}
}

func TestSC08SC22SC24_RebuildRetainsNegativeAndQualifiedResults(t *testing.T) {
	for _, test := range []struct{ input, status string }{{"poison", "PASS_WITH_EXCLUSIONS"}, {"conflict", "BLOCKED"}} {
		t.Run(test.input, func(t *testing.T) {
			environment := setup(t, test.input)
			environment.Execute = true
			namespace, topic := environment.destination("rebuilt")
			report, err := Rebuild(t.Context(), environment.Options, namespace, topic, func(broker.Manifest) error { return nil })
			if err != nil || report.Status != test.status || report.Verification == nil || report.Verification.Accepts(nil) {
				t.Fatalf("qualified/negative status lost: %+v %v", report, err)
			}
		})
	}
}

func TestSC24_RebuildCancellationDoesNotReportSuccess(t *testing.T) {
	environment := setup(t, "")
	environment.Execute = true
	namespace, topic := environment.destination("canceled")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	report, err := Rebuild(ctx, environment.Options, namespace, topic, func(manifest broker.Manifest) error {
		if manifest.Output != nil {
			cancel()
		}
		return nil
	})
	if err == nil || report.Status == "PASS" || report.Verification != nil {
		t.Fatalf("canceled rebuild reported success: %+v %v", report, err)
	}
}

func TestSC23SC24SC25_MissingHistoryRefusedBeforeMutation(t *testing.T) {
	environment := setup(t, "")
	environment.Execute = true
	before, err := environment.state.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request := kmsg.NewDeleteRecordsRequest()
	topic := kmsg.NewDeleteRecordsRequestTopic()
	topic.Topic = environment.Manifest.Topic
	partition := kmsg.NewDeleteRecordsRequestTopicPartition()
	partition.Partition, partition.Offset = 0, 1
	topic.Partitions = append(topic.Partitions, partition)
	request.Topics = append(request.Topics, topic)
	response, err := request.RequestWith(t.Context(), environment.client)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				t.Fatal(err)
			}
		}
	}
	starts, err := broker.Offsets(t.Context(), environment.client, environment.Manifest.Topic, 2, -2)
	if err != nil || starts[0] != 1 {
		t.Fatalf("test did not remove the required prefix: %v %v", starts, err)
	}
	replay, err := Replay(t.Context(), environment.Options, 0, 1, 5)
	if !errors.Is(err, broker.ErrBoundary) || replay.Status != "INCOMPLETE" {
		t.Fatalf("replay accepted missing history: %+v %v", replay, err)
	}
	namespace, output := environment.destination("missing-history")
	saved := false
	environment.awaitProducerRelease(t)
	rebuild, err := Rebuild(t.Context(), environment.Options, namespace, output, func(broker.Manifest) error { saved = true; return nil })
	if !errors.Is(err, broker.ErrBoundary) || rebuild.Status != "INCOMPLETE" || saved {
		t.Fatalf("rebuild created resources with missing history: %+v %v", rebuild, err)
	}
	after, err := environment.state.Snapshot(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("missing history changed original durable state", err)
	}
}

func TestSC24SC25_RebuildRefusesAdvancingSource(t *testing.T) {
	environment := setup(t, "")
	environment.Execute = true
	namespace, topic := environment.destination("advancing")
	report, err := Rebuild(t.Context(), environment.Options, namespace, topic, func(manifest broker.Manifest) error {
		if manifest.Output == nil {
			return nil
		}
		return environment.client.ProduceSync(t.Context(), &kgo.Record{Topic: environment.Manifest.Topic, Partition: 0, Key: []byte("invalid"), Value: []byte("{")}).FirstErr()
	})
	if !errors.Is(err, broker.ErrBoundary) || report.Status != "INCOMPLETE" || report.Verification != nil {
		t.Fatalf("advancing source certified: %+v %v", report, err)
	}
}

func TestSC24_RebuildNeedsSourceNotOriginalProcessingState(t *testing.T) {
	environment := setup(t, "")
	schema := strings.ReplaceAll(environment.Manifest.Config.Namespace, "-", "_")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := environment.Pool.Exec(t.Context(), "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := environment.Pool.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(os.Getenv("AMENDS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	options := environment.Options
	options.Pool = pool
	namespace, topic := environment.destination("empty-database")
	dry, err := Rebuild(t.Context(), options, namespace, topic, nil)
	if err != nil || dry.Status != "DRY_RUN" {
		t.Fatalf("dry run required original tables: %+v %v", dry, err)
	}
	var installed bool
	if err := pool.QueryRow(t.Context(), "SELECT to_regclass('amends_namespaces') IS NOT NULL").Scan(&installed); err != nil || installed {
		t.Fatalf("dry run installed schema: %v %v", installed, err)
	}
	options.Execute = true
	environment.awaitProducerRelease(t)
	report, err := Rebuild(t.Context(), options, namespace, topic, func(broker.Manifest) error { return nil })
	if err != nil || report.Status != "PASS" {
		t.Fatalf("rebuild depended on lost processing state: %+v %v", report, err)
	}
	missingConfig := environment.Manifest.Config.Clone()
	missingConfig.Namespace += "-metadata-only"
	metadata, err := postgres.New(pool, missingConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.CreateFresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	namespaceID, _ := json.Marshal(missingConfig.Namespace)
	for _, table := range []string{"amends_key_state", "amends_view_progress", "amends_partitions"} {
		if _, err := pool.Exec(t.Context(), "DELETE FROM "+table+" WHERE namespace_id=$1", string(namespaceID)); err != nil {
			t.Fatal(err)
		}
	}
	options.Manifest.Config = missingConfig
	namespace, topic = environment.destination("missing-frontier")
	environment.awaitProducerRelease(t)
	report, err = Rebuild(t.Context(), options, namespace, topic, func(broker.Manifest) error { return nil })
	if err != nil || report.Status != "PASS" {
		t.Fatalf("rebuild depended on original frontier: %+v %v", report, err)
	}
}
