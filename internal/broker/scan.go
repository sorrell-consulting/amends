// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Scan reads the retained read-committed prefix independently of processing
// state. It never interprets a high watermark as evidence of consumed records.
// Trailing invisible/control offsets cannot complete this adapter's scan.
func Scan(ctx context.Context, brokers []string, m Manifest, end []int64) ([]ledger.Record, error) {
	if len(end) != m.Config.Partitions {
		return nil, fmt.Errorf("invalid scan boundary")
	}
	offsets := map[int32]kgo.Offset{}
	next := make([]int64, len(end))
	for p, until := range end {
		if until < 0 {
			return nil, fmt.Errorf("invalid scan end")
		}
		if until > 0 {
			offsets[int32(p)] = kgo.NewOffset().At(0).WithEpoch(-1)
		}
	}
	cl, err := Client(brokers, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{m.Topic: offsets}), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var records []ledger.Record
	for !slices.Equal(next, end) {
		poll, cancel := context.WithTimeout(ctx, time.Second)
		fetches := cl.PollRecords(poll, 100)
		cancel()
		if ctx.Err() != nil {
			return records, fmt.Errorf("%w: source scan did not reach its boundary: %v", ErrBoundary, ctx.Err())
		}
		for _, e := range fetches.Errors() {
			if !errors.Is(e.Err, context.DeadlineExceeded) {
				return records, e.Err
			}
		}
		for _, fetch := range fetches {
			for _, topic := range fetch.Topics {
				for _, partition := range topic.Partitions {
					for _, record := range partition.Records {
						if topic.Topic != m.Topic || (topic.TopicID != [16]byte{} && Incarnation(topic.TopicID) != m.Config.Source) {
							return records, fmt.Errorf("%w: source incarnation changed during scan", ErrBoundary)
						}
						p := int(record.Partition)
						if p < 0 || p >= len(end) || record.Offset < next[p] || record.Offset >= end[p] {
							return records, fmt.Errorf("%w: source changed or scan coordinates regressed", ErrBoundary)
						}
						records = append(records, DecodeRecord(m.Config.Source, record).Clone())
						next[p] = record.Offset + 1
						if next[p] == end[p] {
							cl.PauseFetchPartitions(map[string][]int32{m.Topic: {int32(p)}})
						}
					}
				}
			}
		}
	}
	return records, nil
}
