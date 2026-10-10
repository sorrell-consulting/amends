package showcase

import (
	"strings"
	"testing"
)

func TestZombieFenceEvidence(test *testing.T) {
	attempt := `{"event":"processing_attempt","namespace":"owned","partition":0,"offset":6,"epoch":3,"outcome":"fenced","storage_call_elapsed_ns":25}` + "\n"
	terminal := `{"event":"processing_stopped","namespace":"owned","partition":0,"offset":6,"epoch":3,"outcome":"fenced"}` + "\n"
	for _, scenario := range []struct {
		name, log string
		valid     bool
	}{
		{"terminal fence", attempt + terminal, true},
		{"reordered fields and changed prose", `{"msg":"different wording","outcome":"fenced","epoch":3,"offset":6,"partition":0,"namespace":"owned","event":"processing_attempt","storage_call_elapsed_ns":25}` + "\n" + terminal, true},
		{"unrelated event", `{ "event": "partition_assigned" }` + "\n" + attempt + terminal, true},
		{"missing attempt", terminal, false},
		{"missing partition", strings.ReplaceAll(attempt, `"partition":0,`, "") + terminal, false},
		{"missing offset", strings.ReplaceAll(attempt, `"offset":6,`, "") + terminal, false},
		{"missing timing", strings.ReplaceAll(attempt, `,"storage_call_elapsed_ns":25`, "") + terminal, false},
		{"malformed JSON", "not JSON\n" + attempt + terminal, false},
		{"duplicate terminal", attempt + terminal + terminal, false},
		{"wrong terminal outcome", attempt + strings.ReplaceAll(terminal, `"fenced"`, `"error"`), false},
		{"wrong terminal epoch", attempt + strings.ReplaceAll(terminal, `"epoch":3`, `"epoch":4`), false},
		{"missing terminal", attempt, false},
		{"retry after fence", attempt + attempt + terminal, false},
		{"success", strings.ReplaceAll(attempt, `"outcome":"fenced"`, `"outcome":"acknowledged"`) + terminal, false},
		{"other error", strings.ReplaceAll(attempt, `"outcome":"fenced"`, `"outcome":"error"`) + terminal, false},
		{"different epoch", strings.ReplaceAll(attempt, `"epoch":3`, `"epoch":4`) + terminal, false},
		{"different namespace", strings.ReplaceAll(attempt, `"namespace":"owned"`, `"namespace":"other"`) + terminal, false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			count, err := zombieFenceAttempts([]byte(scenario.log), "owned", 3)
			if (err == nil) != scenario.valid || (scenario.valid && count != 1) {
				test.Fatalf("incorrect fence evidence acceptance: count=%d error=%v", count, err)
			}
		})
	}
}
