# Named guard mutation campaign

The campaign checks whether a selected semantic assertion detects removal of its corresponding guard. It defines twelve non-equivalent mutations and one separately classified equal-time ordering change. Results apply to those operators and schedules, not exhaustive correctness or a general mutation score.

## Running it

Use the pinned Go toolchain from `go.mod` and the local PostgreSQL endpoint used by `make test-postgres`. PostgreSQL must already be running; `make up` starts the repository's local dependencies if needed. The campaign itself needs PostgreSQL but does not need Redpanda.

```sh
make test-mutations
# Same campaign and evidence collection:
make evidence-mutations
```

Each invocation creates `artifacts/mutations/run-<unique-suffix>/`. The runner prints that directory and a result for each mutation. To investigate one operator:

```sh
AMENDS_TEST_DATABASE_URL='postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable' \
  go run -mod=readonly ./tools/mutations -only M09
```

Only use a disposable local test database. The existing SQL test helpers migrate it and create unique synthetic namespaces, then remove their own rows. They neither delete volumes nor access broker or cloud services. SQL setup errors are campaign errors.

The [manual workflow](https://github.com/sorrell-consulting/amends/blob/main/.github/workflows/mutations.yml) runs the ordinary checks and real SQL suite before the isolated campaign, using PostgreSQL 18.6 and the toolchain in `go.mod`. It uploads evidence even on failure. It has only a `workflow_dispatch` trigger. `make check`, `make test`, `make evidence`, and push/PR CI never execute mutated application code. Ordinary CI checks the runner, manifest anchors, and the correct implementation.

## Operators and the assertions they exercise

The [manifest](https://github.com/sorrell-consulting/amends/blob/main/tools/mutations/campaign.json) contains token substitutions or narrowly scoped comparison edits, full test names, and stable assertion markers such as `[amends:epoch-fence]`. Human diagnostic text after a marker can change without changing classification.

| ID | Changed rule in the private copy | Selected observation |
| --- | --- | --- |
| M01 | Allow lower arriving revisions to replace authority | SC06 stale-last projection differs from the independent oracle |
| M02 | Discard cancellation input without retaining authority | SC04 cancel-before-new becomes active despite the higher cancellation |
| M03 | Omit balance withdrawals in the diff | SC14 materialized days differ from the hand-stated sparse-day projection |
| M04 | Delete a view row instead of keeping its deletion version | SC05/SC16 older output resurrects a withdrawn balance |
| M05 | Install lower output versions | SC16 delayed balance changes the expected value/version or progress |
| M06 | Accept equal-version conflicting content | SC17 fails its explicit protocol-rejection assertion |
| M07 | Commit source progress before the state transaction | SC18 kills the actual processing connection; the durable snapshot has partial progress |
| M08 | Omit the transactional outbox insert | SC18/SC20 loses a successful COMMIT reply; recovery finds missing publication intent |
| M09 | Remove the database epoch comparison | SC19 installs a replacement epoch while the old transaction waits on the real row lock; the old owner then commits |
| M10 | Commit a view row before committing its progress | SC21 loses the first successful COMMIT reply; the row and frontier disagree |
| M11 | Retain only the first candidate at a revision | SC08 worker trust differs from the oracle's unresolved conflict |
| M12 | Omit the memory verifier's output-frontier comparison | SC25 falsely certifies an unapplied READY output because business values already agree |
| E01 | Reverse the equal-time TxnID tiebreak | All hand-derived scenario cases still pass; declared equivalent for additive daily closes |

M07–M10 use real PostgreSQL. M07 advances the frontier in an early transaction, opens another transaction for the state decision, then encounters the test's actual connection termination. M08 exercises the lost-output side of missing committed intent; it does not simulate an uncommitted broker publication. M10 splits the two commits, so the existing controlled reply loss exposes the intermediate durable state. These operators do not replace database failure injection with a mock or count an infrastructure failure as a detected guard violation.

M01–M06 and M11 exercise pure code through selected scenario/model tests. M12 specifically mutates `sim.Model.Check`; it does not mutate the running broker verifier. The ordinary real pipeline verification suite remains separate evidence for that adapter. The campaign does not mutate every retry path or every SQL constraint.

E01's classification follows the [daily-close semantics](semantics.md#s5-daily-balances-and-crossing-alerts): equal-time transactions contribute to the same exact integer daily total, and alerts compare consecutive daily closes. Reversing their traversal changes neither the activity days nor those totals. Its passing scenarios support that classification; passing tests alone do not establish arbitrary program equivalence. Diagnostic order and intraday business semantics are outside this equivalence claim.

## Isolation and evidence

[`tools/mutations`](https://github.com/sorrell-consulting/amends/tree/main/tools/mutations) is a standalone development command using the Go standard library. It copies an explicit set of repository build inputs into a private temporary directory: module files, Makefile, commands, internal packages, fixtures, migrations, simulators, tests, and tools. It rejects symlinks and special files, disables parent workspaces and per-user Go configuration, clears inherited `GOFLAGS`, and selects the pinned toolchain. Dependencies still come from the ordinary published Go modules and cache.

The runner first executes the exact named test against the unchanged snapshot. Only a passing baseline permits mutation. Each operator receives a fresh copy of that same snapshot. An anchor must match exactly once; a missing, duplicated, or unchanged substitution is an error. Token substitutions ignore formatting and comments but retain identifiers and literals. M01 and E01 select a comparison by function, field names, and operator, allowing local variable renames. More involved transaction edits remain explicit in the manifest. The runner refuses test-file and oracle-file mutation. Changes exist only in temporary copies; there are no broken runtime modes in application code. The runner removes its temporary tree on ordinary exit.

Both executions use `go test -mod=readonly -buildvcs=false -race -count=1 -timeout=90s -json`; SQL cases add `-tags=integration`. Every subtest name is anchored separately, so an accidentally broadened or missing selector cannot count as a passing baseline. A command has a three-minute outer deadline. The JSON classifier requires the named test to run and finish and the package to finish. A caught mutation must fail with the declared source-located assertion marker in that test. Unrelated failed tests/diagnostics, skipped tests, panics, race reports, timeouts, build errors, and missing events produce `ERROR`.

| Saved artifact | Purpose |
| --- | --- |
| `campaign.json` | Exact operators and assertion markers selected for this invocation |
| `source.zip` | Unmodified standalone build inputs, including dirty/untracked source changes within the selected directories |
| `report.json` | Toolchain, Git revision/worktree observation, SHA-256 per input file, exact commands, assertions, statuses, duration, planned count, completion and success flags |
| `<ID>/baseline.jsonl` | Passing named test on unchanged input, or the baseline error |
| `<ID>/mutant.jsonl` | Exact test output after substitution, when mutation was reached |
| `<ID>/edit-<N>.before.txt` and `.after.txt` | Complete original and substituted source files, stored as text evidence |
| `<ID>/failures/` | Additional artifacts retained by a selected test, if it produces any |

The report is updated after each case and remains `complete: false` until all selected cases finish. An early setup failure may leave only the manifest or source archive; absent results are never success. A full run must have `planned: 13`, thirteen results, `complete: true`, and `passed: true`. A `-only` run is explicitly a smaller selection. The runner does not record environment variables or database connection strings in its metadata; test error logs should still be reviewed before sharing.

`CAUGHT` means the declared assertion failed; `SURVIVED` means a non-equivalent operator passed its named test. `EQUIVALENT` requires the declared equivalent operator to pass. `ERROR` means the campaign did not establish either result. Any `SURVIVED` or `ERROR` makes the command fail. Equivalent cases are excluded from the caught count, and a surviving operator is never automatically relabeled equivalent.

For reproduction, extract `source.zip` into an empty directory, supply the same local PostgreSQL prerequisite, and run the command above there. The archive includes the runner and manifest. Review the stored before/after files and assertion output when an operator fails; update a stale anchor when the implementation changes, without weakening the contract assertion merely to restore a passing campaign.
