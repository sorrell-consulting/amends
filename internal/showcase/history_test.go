package showcase

import (
	"testing"

	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/ledger"
)

func TestHistoryRemovalRejectsUnownedSource(test *testing.T) {
	for _, manifest := range []broker.Manifest{
		{Topic: "owned-source", Config: ledger.Config{Namespace: "foreign", Partitions: 2}},
		{Topic: "foreign-source", Config: ledger.Config{Namespace: "owned", Partitions: 2}},
		{Topic: "owned-source", Config: ledger.Config{Namespace: "owned", Partitions: 1}},
	} {
		runner := runner{report: &Report{Namespace: "owned"}, manifest: manifest}
		if err := runner.removeOwnedHistory(test.Context(), &historyEvidence{}); err == nil {
			test.Fatal("unowned source passed the preflight before opening a connection")
		}
	}
}
