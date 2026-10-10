package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/sorrell-consulting/amends/fixtures"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
)

type measuredAttemptState struct {
	token          store.Token
	next           int64
	firstError     error
	unknownCommits bool
	calls          int
}

func (state *measuredAttemptState) Claim(context.Context, int) (store.Token, error) {
	return state.token, nil
}

func (state *measuredAttemptState) Observe(context.Context, int) (store.Token, store.Partition, error) {
	return state.token, store.Partition{Epoch: state.token.Epoch, Next: state.next}, nil
}

func (state *measuredAttemptState) Process(_ context.Context, _ store.Token, _ int64, record ledger.Record) error {
	state.calls++
	if state.calls == 1 && state.firstError != nil {
		if state.unknownCommits {
			state.next = record.Offset + 1
		}
		return state.firstError
	}
	state.next = record.Offset + 1
	return nil
}

func TestProcessingAttemptLogsPreserveUnknownOutcomes(test *testing.T) {
	for _, scenario := range []struct {
		name       string
		firstError error
		committed  bool
		want       []string
	}{
		{"acknowledged", nil, false, []string{"acknowledged"}},
		{"unknown_committed", store.ErrCommitUnknown, true, []string{"unknown"}},
		{"unknown_aborted", store.ErrCommitUnknown, false, []string{"unknown", "acknowledged"}},
		{"fenced", store.ErrFence, false, []string{"fenced"}},
		{"error_retry", errors.New("unavailable"), false, []string{"error", "acknowledged"}},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			config, err := fixtures.Config("attempt-logs")
			if err != nil {
				test.Fatal(err)
			}
			state := &measuredAttemptState{token: store.Token{Namespace: config.Namespace, Digest: config.Digest(), Partition: 0, Epoch: 1}, firstError: scenario.firstError, unknownCommits: scenario.committed}
			var output bytes.Buffer
			life, err := NewLifecycle(test.Context(), LifecycleOptions{
				Config: config, State: state, Validate: func(context.Context) ([]int64, error) { return []int64{1}, nil },
				Wait: func(context.Context, time.Duration) error { return nil }, Logger: slog.New(slog.NewJSONHandler(&output, nil)),
			})
			if err != nil {
				test.Fatal(err)
			}
			defer life.Stop(context.Canceled)
			if err = life.Assign(life.Context(), []int{0}); err != nil {
				test.Fatal(err)
			}
			err = life.Process(life.Context(), ledger.Record{Coordinates: ledger.Coordinates{Source: config.Source}})
			if (err != nil) != errors.Is(scenario.firstError, store.ErrFence) {
				test.Fatalf("logging changed recovery outcome: %v", err)
			}
			var outcomes []string
			for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n")) {
				var event struct {
					Event     string `json:"event"`
					Namespace string `json:"namespace"`
					Outcome   string `json:"outcome"`
					Epoch     int64  `json:"epoch"`
					Elapsed   *int64 `json:"storage_call_elapsed_ns"`
				}
				if err = json.Unmarshal(line, &event); err != nil {
					test.Fatal(err)
				}
				if event.Event == ProcessingAttemptEvent {
					if event.Namespace != config.Namespace || event.Epoch != 1 || event.Elapsed == nil || *event.Elapsed < 0 {
						test.Fatalf("missing attribution/timing: %s", line)
					}
					outcomes = append(outcomes, event.Outcome)
				}
			}
			if !reflect.DeepEqual(outcomes, scenario.want) || state.calls != len(outcomes) {
				test.Fatalf("attempt observations = %v, want %v (calls=%d)", outcomes, scenario.want, state.calls)
			}
		})
	}
}
