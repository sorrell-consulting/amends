// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"fmt"
	"slices"
	"time"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

func validate(c ledger.Config, partition int, e ledger.Envelope) error {
	if e.SchemaVersion != 1 {
		return fmt.Errorf("invalid output envelope: schema_version = %d, want 1", e.SchemaVersion)
	}
	if e.ID.Namespace != c.Namespace {
		return fmt.Errorf("invalid output envelope: namespace = %q, want %q", e.ID.Namespace, c.Namespace)
	}
	k, ok := c.Key(e.ID.Key)
	if !ok {
		return fmt.Errorf("invalid output envelope: unknown stock key item=%q location=%q", e.ID.Key.Item, e.ID.Key.Location)
	}
	if k.Partition != partition {
		return fmt.Errorf("invalid output envelope: stock key belongs to partition %d, received on %d", k.Partition, partition)
	}
	if e.Version < 1 {
		return fmt.Errorf("invalid output envelope: version = %d, want positive", e.Version)
	}
	if e.Cause.Source != c.Source {
		return fmt.Errorf("invalid output envelope: cause source = %q, want %q", e.Cause.Source, c.Source)
	}
	if e.Cause.Partition != partition {
		return fmt.Errorf("invalid output envelope: cause partition = %d, want %d", e.Cause.Partition, partition)
	}
	if e.Cause.Offset < 0 {
		return fmt.Errorf("invalid output envelope: cause offset = %d, want nonnegative", e.Cause.Offset)
	}
	if e.Cause.Revision < 1 {
		return fmt.Errorf("invalid output envelope: cause revision = %d, want positive", e.Cause.Revision)
	}
	if e.Cause.TxnID == "" {
		return fmt.Errorf("invalid output envelope: missing cause transaction ID")
	}
	switch e.ID.Family {
	case ledger.BalanceFamily, ledger.AlertFamily:
		return validateBusiness(k, c.Origin, e)
	case ledger.KeyStatusFamily:
		return validateKeyStatus(e)
	default:
		return fmt.Errorf("unknown output family %q", e.ID.Family)
	}
}

func validateBusiness(k ledger.KeyConfig, origin time.Time, e ledger.Envelope) error {
	day, err := time.Parse("2006-01-02", e.ID.Day)
	if err != nil || day.Format("2006-01-02") != e.ID.Day {
		return fmt.Errorf("invalid business output: day = %q, want YYYY-MM-DD", e.ID.Day)
	}
	if e.ID.Day < origin.UTC().Format("2006-01-02") {
		return fmt.Errorf("invalid business output: day %s precedes ledger origin", e.ID.Day)
	}
	p := e.Payload
	if p.Status != "" || len(p.Reasons) > 0 {
		return fmt.Errorf("invalid business output: status and reasons belong to key-status outputs")
	}
	want := ledger.BalanceStated
	if e.ID.Family == ledger.AlertFamily {
		want = ledger.AlertRaised
	}
	if !p.Present {
		want = ledger.BalanceRetracted
		if e.ID.Family == ledger.AlertFamily {
			want = ledger.AlertRetracted
		}
	}
	if e.Type != want {
		return fmt.Errorf("invalid business payload: type = %q, want %q", e.Type, want)
	}
	if !p.Present {
		if p.Close != 0 || p.Threshold != 0 {
			return fmt.Errorf("invalid business payload: withdrawal must have zero closing balance and threshold")
		}
		return nil
	}
	if e.ID.Family == ledger.BalanceFamily {
		if p.Threshold != 0 {
			return fmt.Errorf("invalid business payload: balance threshold = %d, want 0", p.Threshold)
		}
		return nil
	}
	if p.Threshold != k.Threshold {
		return fmt.Errorf("invalid business payload: alert threshold = %d, want %d", p.Threshold, k.Threshold)
	}
	if p.Close >= k.Threshold {
		return fmt.Errorf("invalid business payload: alert close %d must be below threshold %d", p.Close, k.Threshold)
	}
	return nil
}

func validateKeyStatus(e ledger.Envelope) error {
	if e.ID.Day != "" {
		return fmt.Errorf("invalid key status: day = %q, want empty", e.ID.Day)
	}
	if e.Type != ledger.KeyStatusStated {
		return fmt.Errorf("invalid key status: type = %q, want %q", e.Type, ledger.KeyStatusStated)
	}
	p := e.Payload
	if !p.Present {
		return fmt.Errorf("invalid key status: status cannot be withdrawn")
	}
	if p.Close != 0 || p.Threshold != 0 {
		return fmt.Errorf("invalid key status: closing balance and threshold must be zero")
	}
	switch p.Status {
	case ledger.Ready:
		if len(p.Reasons) > 0 {
			return fmt.Errorf("invalid key status: READY must have no reasons")
		}
	case ledger.Blocked:
		if len(p.Reasons) == 0 {
			return fmt.Errorf("invalid key status: BLOCKED requires reasons")
		}
	default:
		return fmt.Errorf("invalid key status: status = %q, want READY or BLOCKED", p.Status)
	}
	if !slices.IsSorted(p.Reasons) {
		return fmt.Errorf("invalid key status: reasons must be sorted")
	}
	for i := 1; i < len(p.Reasons); i++ {
		if p.Reasons[i] == p.Reasons[i-1] {
			return fmt.Errorf("duplicate status reason %q", p.Reasons[i])
		}
	}
	return nil
}
