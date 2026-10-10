// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

// Package oracle reconstructs the complete history independently. It imports
// only the shared schema/decoder: never fold, diff, memory, view, or their state.
package oracle

import (
	"fmt"
	"math/big"
	"slices"
	"sort"

	"github.com/sorrell-consulting/amends/internal/ledger"
)

// Result contains independently reconstructed projections and source diagnostics.
// BLOCKED keys have empty business facts. Conflicts includes superseded revisions;
// Rejections retains the original invalid records and their reason codes.
type Result struct {
	Keys       []ledger.Projection `json:"keys"`
	Rejections []ledger.Rejection  `json:"rejections"`
	Conflicts  []ledger.Conflict   `json:"conflicts"`
}

type identity struct {
	Key ledger.StockKey
	Txn string
}

// Compute reconstructs authority and daily facts from history without changing
// its inputs. The caller must supply the complete retained source boundary;
// this function cannot prove retention or pipeline completion from a slice.
//
// Rejected input and revision conflicts appear in Result rather than as errors.
// Invalid configuration, incompatible coordinates, or arithmetic outside int64
// return an error; the partial result must not be treated as a complete answer.
func Compute(config ledger.Config, history []ledger.Record) (Result, error) {
	var result Result
	if err := config.Validate(); err != nil {
		return result, err
	}
	groups := map[identity][]ledger.Transaction{}
	for _, record := range history {
		if record.Source != config.Source || record.Partition < 0 || record.Partition >= config.Partitions || record.Offset < 0 {
			return result, fmt.Errorf("incompatible source coordinates")
		}
		t, reason := validate(config, record)
		if reason != "" {
			result.Rejections = append(result.Rejections, ledger.Rejection{Record: record.Clone(), Reason: reason})
			continue
		}
		id := identity{t.StockKey, t.TxnID}
		groups[id] = append(groups[id], t)
	}
	active := map[ledger.StockKey][]ledger.Transaction{}
	blocked := map[ledger.StockKey][]string{}
	for id, assertions := range groups {
		sort.Slice(assertions, func(i, j int) bool { return assertions[i].Revision < assertions[j].Revision })
		for start := 0; start < len(assertions); {
			end := start + 1
			for end < len(assertions) && assertions[end].Revision == assertions[start].Revision {
				end++
			}
			// Compare fields directly, independently of incremental canonical equality.
			first := assertions[start]
			ambiguous := false
			for _, other := range assertions[start+1 : end] {
				if other.Kind != first.Kind || !other.ValidTime.Equal(first.ValidTime) || other.Qty != first.Qty {
					ambiguous = true
				}
			}
			if ambiguous {
				conflict := ledger.Conflict{StockKey: id.Key, TxnID: id.Txn, Revision: first.Revision}
				result.Conflicts = append(result.Conflicts, conflict)
				if end == len(assertions) {
					blocked[id.Key] = append(blocked[id.Key], conflict.Ref())
				}
			} else if end == len(assertions) && first.Kind != ledger.Cancel {
				active[id.Key] = append(active[id.Key], first)
			}
			start = end
		}
	}
	for _, key := range config.Keys {
		p := ledger.Projection{Key: key.StockKey, Status: ledger.Ready, Balances: []ledger.Balance{}, Alerts: []ledger.Alert{}}
		if reasons := blocked[key.StockKey]; len(reasons) > 0 {
			slices.Sort(reasons)
			p.Status = ledger.Blocked
			p.Reasons = reasons
			result.Keys = append(result.Keys, p)
			continue
		}
		totals := map[string]*big.Int{}
		for _, transaction := range active[key.StockKey] {
			// Calendar derivation is independent of fold's formatting/group traversal.
			y, m, d := transaction.ValidTime.UTC().Date()
			day := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			if totals[day] == nil {
				totals[day] = new(big.Int)
			}
			totals[day].Add(totals[day], big.NewInt(transaction.Qty))
		}
		days := make([]string, 0, len(totals))
		for day := range totals {
			days = append(days, day)
		}
		slices.Sort(days)
		balance := big.NewInt(key.Opening)
		threshold := big.NewInt(key.Threshold)
		for _, day := range days {
			atOrAbove := balance.Cmp(threshold) >= 0
			balance.Add(balance, totals[day])
			if !balance.IsInt64() {
				return result, fmt.Errorf("oracle out of domain: %+v %s", key.StockKey, day)
			}
			p.Balances = append(p.Balances, ledger.Balance{Day: day, Close: balance.Int64()})
			if atOrAbove && balance.Cmp(threshold) < 0 {
				p.Alerts = append(p.Alerts, ledger.Alert{Day: day, Threshold: key.Threshold, Close: balance.Int64()})
			}
		}
		result.Keys = append(result.Keys, p)
	}
	slices.SortFunc(result.Conflicts, func(a, b ledger.Conflict) int {
		if a.Ref() < b.Ref() {
			return -1
		}
		if a.Ref() > b.Ref() {
			return 1
		}
		return 0
	})
	return result, nil
}

// Kept separate from fold.Validate so the oracle checks validation independently.
// Only decoding is shared.
func validate(config ledger.Config, record ledger.Record) (ledger.Transaction, string) {
	t, err := ledger.Decode(record.Value)
	if err != nil {
		return t, "MALFORMED"
	}
	if len(t.TxnID) == 0 || len(t.StockKey.Item) == 0 || len(t.StockKey.Location) == 0 {
		return t, "IDENTITY"
	}
	if t.Revision <= 0 {
		return t, "REVISION"
	}
	switch t.Kind {
	case ledger.New, ledger.Amend, ledger.Cancel:
	default:
		return t, "KIND"
	}
	if t.Kind == ledger.Cancel && t.Qty != 0 {
		return t, "CANCEL_QUANTITY"
	}
	if t.ValidTime.UTC().Year() < 1 || t.ValidTime.UTC().Year() > 9999 || config.Origin.After(t.ValidTime) {
		return t, "VALID_TIME"
	}
	var match *ledger.KeyConfig
	for i := range config.Keys {
		if config.Keys[i].StockKey == t.StockKey {
			match = &config.Keys[i]
			break
		}
	}
	if match == nil {
		return t, "UNKNOWN_KEY"
	}
	if t.StockKey.Item != record.Key.Item || t.StockKey.Location != record.Key.Location {
		return t, "KEY_MISMATCH"
	}
	if match.Partition != record.Partition {
		return t, "ROUTING"
	}
	return t, ""
}
