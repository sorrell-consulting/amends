package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/ledger"
	"github.com/sorrell-consulting/amends/internal/store"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

type reviewedCandidate struct {
	Payload ledger.Transaction   `json:"representative_payload"`
	Sources []ledger.Coordinates `json:"source_references"`
}

type reviewedConflict struct {
	Conflict          ledger.Conflict     `json:"conflict"`
	HighestRevision   int64               `json:"highest_observed_revision"`
	AtHighestRevision bool                `json:"at_highest_revision"`
	KeyStatus         ledger.KeyStatus    `json:"key_status"`
	KeyReasons        []string            `json:"key_reasons"`
	Candidates        []reviewedCandidate `json:"candidates"`
}

type reviewedPartition struct {
	Partition  int   `json:"partition"`
	Epoch      int64 `json:"epoch"`
	SourceNext int64 `json:"source_next"`
}

type reviewReport struct {
	Scope             string              `json:"scope"`
	Kind              string              `json:"kind"`
	Namespace         string              `json:"namespace"`
	ConfigDigest      string              `json:"config_digest"`
	SourceIncarnation string              `json:"source_incarnation"`
	ReadAt            time.Time           `json:"snapshot_read_at"`
	EvidenceBasis     string              `json:"evidence_basis"`
	Count             int                 `json:"count"`
	Partitions        []reviewedPartition `json:"partitions"`
	Rejections        []ledger.Rejection  `json:"quarantine,omitempty"`
	Conflicts         []reviewedConflict  `json:"conflicts,omitempty"`
}

type reviewSelector struct {
	Show      bool
	Partition int
	Offset    int64
	Key       ledger.StockKey
	TxnID     string
	Revision  int64
}

func reviewConflicts(snapshot store.Snapshot) []reviewedConflict {
	result := []reviewedConflict{}
	for _, configured := range snapshot.Config.Keys {
		state := snapshot.Keys[configured.StockKey]
		for _, conflict := range state.Conflicts() {
			identity := state.Identities[conflict.TxnID]
			row := reviewedConflict{Conflict: conflict, HighestRevision: identity.Highest, AtHighestRevision: conflict.Revision == identity.Highest, KeyStatus: state.Projection.Status, KeyReasons: slices.Clone(state.Projection.Reasons)}
			for _, candidate := range identity.Revisions[conflict.Revision] {
				row.Candidates = append(row.Candidates, reviewedCandidate{Payload: candidate.Transaction, Sources: slices.Clone(candidate.Sources)})
			}
			result = append(result, row)
		}
	}
	slices.SortFunc(result, func(left, right reviewedConflict) int {
		return strings.Compare(left.Conflict.Ref(), right.Conflict.Ref())
	})
	return result
}

func selectReview(snapshot store.Snapshot, kind string, selector reviewSelector) (reviewReport, error) {
	report := reviewReport{Scope: "durable diagnostic snapshot; not a verification result", Kind: kind, Namespace: snapshot.Config.Namespace, ConfigDigest: snapshot.Config.Digest(), SourceIncarnation: snapshot.Config.Source}
	for partition := 0; partition < snapshot.Config.Partitions; partition++ {
		progress := snapshot.Partitions[partition]
		report.Partitions = append(report.Partitions, reviewedPartition{Partition: partition, Epoch: progress.Epoch, SourceNext: progress.Next})
	}
	switch kind {
	case "quarantine":
		report.EvidenceBasis = "exact rejected raw key/value bytes and reason; byte arrays are base64 in JSON"
		for _, rejection := range snapshot.Rejections {
			if !selector.Show || (rejection.Record.Partition == selector.Partition && rejection.Record.Offset == selector.Offset) {
				report.Rejections = append(report.Rejections, ledger.Rejection{Record: rejection.Record.Clone(), Reason: rejection.Reason})
			}
		}
	case "conflicts":
		report.EvidenceBasis = "retained canonical candidates with representative informational fields and source references; not exact raw accepted records; no candidate is selected as a winner"
		for _, conflict := range reviewConflicts(snapshot) {
			if !selector.Show || (conflict.Conflict.StockKey == selector.Key && conflict.Conflict.TxnID == selector.TxnID && conflict.Conflict.Revision == selector.Revision) {
				report.Conflicts = append(report.Conflicts, conflict)
			}
		}
	default:
		return report, fmt.Errorf("unsupported diagnostic review %q", kind)
	}
	report.Count = len(report.Rejections) + len(report.Conflicts)
	if selector.Show && report.Count == 0 {
		return report, fmt.Errorf("requested %s evidence not found in this namespace snapshot", kind)
	}
	return report, nil
}

func runReview(kind string, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "list" && args[0] != "show") {
		_, err := fmt.Fprintf(stderr, "Usage: amendsctl %s list|show -manifest FILE [options]\nRead-only durable evidence; not verification. Use %s list -h or %s show -h.\n", kind, kind, kind)
		if err != nil {
			return 1
		}
		if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
			return 0
		}
		return 2
	}
	flags := flag.NewFlagSet(kind+" "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifestPath := flags.String("manifest", "", "saved source/pipeline manifest (required)")
	timeout := flags.Duration("timeout", 30*time.Second, "bounded database read time; no broker access")
	selector := reviewSelector{Show: args[0] == "show", Partition: -1, Offset: -1}
	if selector.Show && kind == "quarantine" {
		flags.IntVar(&selector.Partition, "partition", -1, "source partition (required)")
		flags.Int64Var(&selector.Offset, "offset", -1, "source offset (required)")
	} else if selector.Show {
		flags.StringVar(&selector.Key.Item, "item", "", "stock item (required)")
		flags.StringVar(&selector.Key.Location, "location", "", "stock location (required)")
		flags.StringVar(&selector.TxnID, "txn-id", "", "transaction identity (required)")
		flags.Int64Var(&selector.Revision, "revision", 0, "conflicting revision (required)")
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	invalid := flags.NArg() != 0 || *manifestPath == "" || *timeout <= 0 || os.Getenv("AMENDS_DATABASE_URL") == ""
	if selector.Show {
		if kind == "quarantine" {
			invalid = invalid || selector.Partition < 0 || selector.Offset < 0
		} else {
			invalid = invalid || selector.Key.Item == "" || selector.Key.Location == "" || selector.TxnID == "" || selector.Revision < 1
		}
	}
	if invalid {
		fmt.Fprintln(stderr, "review requires -manifest, AMENDS_DATABASE_URL, positive timeout, and exact show coordinates/identity")
		return 2
	}
	err := func() error {
		manifest, err := readAdminManifest(*manifestPath)
		if err != nil {
			return err
		}
		if selector.Show && kind == "quarantine" && selector.Partition >= manifest.Config.Partitions {
			return fmt.Errorf("source partition outside manifest")
		}
		parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(parent, *timeout)
		defer cancel()
		pool, err := pgxpool.New(ctx, os.Getenv("AMENDS_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer pool.Close()
		state, err := postgres.New(pool, manifest.Config)
		if err != nil {
			return err
		}
		snapshot, err := state.Snapshot(ctx)
		if err != nil {
			return err
		}
		readAt := time.Now().UTC()
		report, err := selectReview(snapshot, kind, selector)
		if err != nil {
			return err
		}
		report.ReadAt = readAt
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}()
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl %s: %v\n", kind, err)
		return 1
	}
	return 0
}
