// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package broker implements the bounded local source transport. It does not
// interpret revision authority or declare a complete verification boundary.
package broker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrBoundary = errors.New("incompatible retained broker boundary")

// Source is an observed topic incarnation, not just a reusable topic name.
type Source struct {
	Topic      string `json:"topic"`
	ID         string `json:"source_incarnation"`
	Partitions int    `json:"partitions"`
}

func Incarnation(id [16]byte) string { return "kafka:" + hex.EncodeToString(id[:]) }

func Client(brokers []string, options ...kgo.Opt) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.DialTimeout(3 * time.Second),
		kgo.RequestTimeoutOverhead(3 * time.Second), kgo.RetryTimeout(8 * time.Second), kgo.RequestRetries(3)}
	return kgo.NewClient(append(opts, options...)...)
}

// CreateSource never adopts an existing topic. On an unknown creation result,
// inspect the named topic explicitly; retrying must not silently change lineage.
func CreateSource(ctx context.Context, cl *kgo.Client, topic string, partitions int) (Source, error) {
	if topic == "" || partitions < 1 || partitions > math.MaxInt32 {
		return Source{}, fmt.Errorf("invalid source topic or partition count")
	}
	req := kmsg.NewCreateTopicsRequest()
	req.TimeoutMillis = 5000
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = topic, int32(partitions), 1
	for _, kv := range retention {
		v := kmsg.NewCreateTopicsRequestTopicConfig()
		v.Name, v.Value = kv[0], kmsg.StringPtr(kv[1])
		t.Configs = append(t.Configs, v)
	}
	req.Topics = append(req.Topics, t)
	r, err := req.RequestWith(ctx, cl)
	if err != nil {
		return Source{}, err
	}
	if len(r.Topics) != 1 || r.Topics[0].Topic != topic {
		return Source{}, fmt.Errorf("incomplete create-topic response")
	}
	if err = kerr.ErrorForCode(r.Topics[0].ErrorCode); err != nil {
		return Source{}, err
	}
	return Inspect(ctx, cl, topic)
}

var retention = [][2]string{{"cleanup.policy", "delete"}, {"retention.ms", "-1"}, {"retention.bytes", "-1"}}

func Inspect(ctx context.Context, cl *kgo.Client, topic string) (Source, error) {
	req := kmsg.NewMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	req.AllowAutoTopicCreation = false
	r, err := req.RequestWith(ctx, cl)
	if err != nil {
		return Source{}, err
	}
	if len(r.Topics) != 1 || r.Topics[0].Topic == nil || *r.Topics[0].Topic != topic {
		return Source{}, fmt.Errorf("incomplete source metadata")
	}
	got := r.Topics[0]
	if err = kerr.ErrorForCode(got.ErrorCode); err != nil {
		return Source{}, err
	}
	if got.IsInternal || got.TopicID == [16]byte{} || len(got.Partitions) == 0 {
		return Source{}, fmt.Errorf("source requires a non-internal topic with a broker UUID and partitions")
	}
	seen := make(map[int32]bool)
	for _, p := range got.Partitions {
		if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
			return Source{}, err
		}
		if p.Partition < 0 || int(p.Partition) >= len(got.Partitions) || seen[p.Partition] {
			return Source{}, fmt.Errorf("invalid source partition metadata")
		}
		seen[p.Partition] = true
	}
	return Source{Topic: topic, ID: Incarnation(got.TopicID), Partitions: len(got.Partitions)}, nil
}

// Validate checks immutable identity and retention policy before reading input.
// It also requires retained history from zero: later verification must be able
// to reconstruct the namespace independently of worker state.
func Validate(ctx context.Context, cl *kgo.Client, topic string, c ledger.Config) ([]int64, error) {
	s, err := Inspect(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	if s.ID != c.Source || s.Partitions != c.Partitions {
		return nil, fmt.Errorf("%w: source incarnation/partition count differs from namespace; use a fresh namespace", ErrBoundary)
	}
	req := kmsg.NewDescribeConfigsRequest()
	rsrc := kmsg.NewDescribeConfigsRequestResource()
	rsrc.ResourceType, rsrc.ResourceName = kmsg.ConfigResourceTypeTopic, topic
	for _, kv := range retention {
		rsrc.ConfigNames = append(rsrc.ConfigNames, kv[0])
	}
	req.Resources = append(req.Resources, rsrc)
	r, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if len(r.Resources) != 1 || r.Resources[0].ResourceName != topic {
		return nil, fmt.Errorf("incomplete source configuration response")
	}
	if err = kerr.ErrorForCode(r.Resources[0].ErrorCode); err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, v := range r.Resources[0].Configs {
		if v.Value != nil {
			values[v.Name] = *v.Value
		}
	}
	for _, kv := range retention {
		if values[kv[0]] != kv[1] {
			return nil, fmt.Errorf("%w: source requires %s=%s, got %q", ErrBoundary, kv[0], kv[1], values[kv[0]])
		}
	}
	starts, err := Offsets(ctx, cl, topic, c.Partitions, -2)
	if err != nil {
		return nil, err
	}
	for p, start := range starts {
		if start != 0 {
			return nil, fmt.Errorf("%w: required history missing: partition %d starts at %d", ErrBoundary, p, start)
		}
	}
	return Offsets(ctx, cl, topic, c.Partitions, -1)
}

// Offsets returns broker positions with read-committed visibility. Ends are
// bounds for seeking, never authority to advance a durable processing frontier.
func Offsets(ctx context.Context, cl *kgo.Client, topic string, partitions int, timestamp int64) ([]int64, error) {
	req := kmsg.NewListOffsetsRequest()
	req.IsolationLevel = 1
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for i := 0; i < partitions; i++ {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = int32(i), timestamp
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = append(req.Topics, t)
	r, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if len(r.Topics) != 1 || r.Topics[0].Topic != topic || len(r.Topics[0].Partitions) != partitions {
		return nil, fmt.Errorf("incomplete source offset response")
	}
	positions := make([]int64, partitions)
	seen := make(map[int32]bool)
	for _, p := range r.Topics[0].Partitions {
		if err = kerr.ErrorForCode(p.ErrorCode); err != nil {
			return nil, err
		}
		if p.Partition < 0 || int(p.Partition) >= partitions || seen[p.Partition] || p.Offset < 0 {
			return nil, fmt.Errorf("invalid source offset response")
		}
		seen[p.Partition] = true
		positions[p.Partition] = p.Offset
	}
	return positions, nil
}
