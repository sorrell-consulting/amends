# Evaluate amends

Start with the in-memory correction. Add real storage and a crash when you want to examine recovery. Run these commands from the repository root.

## The story to look for

Opening inventory is **100 units**, with a **reorder threshold of 60 units** marking the low-stock boundary. An issue removes stock and a receipt adds it. Issues of 50 and 20 units on Days 1 and 2, then a receipt of 50 on Day 3, produce **end-of-day balances of 50, 30, and 80 units**.

On Day 1, inventory falls from 100 to 50, crossing below 60. This **downward crossing** generates a low-inventory alert: the preceding daily close (or opening balance) must be at or above the threshold and the next close below it. Remaining below the threshold on Day 2 creates no new alert; recovering above it on Day 3 does not erase the historical Day 1 alert.

A later, higher revision replaces the first issue of 50 units with an issue of 30. The revised closes are **70, 50, and 100 units**. Day 1 no longer crosses below 60; Day 2 now falls from 70 to 50. The system must withdraw the Day 1 alert and publish a Day 2 alert. The [fixture guide](https://github.com/sorrell-consulting/amends/blob/main/fixtures/README.md) states the expected values, and the [visual comparison](index.md#see-the-correction) shows both histories.

The crash path interrupts the correction inside an observed SQL transaction and confirms that no partial decision became durable. Recovery installs a newer database ownership generation, called an **epoch**, before processing resumes. A separately implemented full-history calculation (the **independent oracle**) reconstructs the same retained source history and checks the final stored results, or **materialized view**. It does not reuse the incremental calculation, though both paths share decoding and data types.

## Path 1: correction in memory

**Prerequisites:** GNU Make and Go **1.27.2**, or a Go launcher with network access to download it and the pinned modules. Docker and a C compiler are not needed for this example. A C compiler is needed for the optional race-enabled tests.

```sh
make core-demo
```

Expect three `CORE_AGREEMENT` stages: initial daily closes of 50/30/80 units, corrected closes of 70/50/100 units, and cancellation leaving Day 1 and Day 2 closes of 70/50 units. Cancellation removes the last active Day 3 movement. The example stores balance rows only for days with active transactions, so Day 3's row disappears; the previous balance carries forward implicitly, rather than becoming zero. Its deletion version (a **tombstone**) remains to prevent an older delivery from restoring the row. The Day 2 alert survives.

The command checks both the independent oracle and incremental/view path against embedded hand-derived expectations. It builds `bin/amendsctl` and `bin/amends`, prints a transcript, and creates no durable pipeline or per-run evidence directory. An [example transcript](media/core-demo.txt) is checked in. **Shutdown:** none.

## Prerequisites for the durable paths

Paths 2 and 3 also need a working Linux Docker engine with Compose, image-registry access, and available loopback ports **15432** and **19092**. `docker version` must report a server. On Windows, enable Docker Desktop integration for the WSL distribution holding the checkout. Allow at least 4 GiB of Docker memory and space for images and retained history; this is a starting allocation, not a measured minimum.

The wrappers build the applications, start the pinned PostgreSQL 18.6 and Redpanda v26.2.3 services, and check readiness. Use the default `amends-local` Compose project from only one checkout at a time. See the [runbook](runbook.md#prerequisites-and-connections) for credentials, resource settings, and connection details.

## Path 2: durable correction

```sh
make demo MODE=correction-only
```

The runner creates fresh source/output topics and an isolated identity for this run, its **namespace**. Two worker processes handle input; a relay publishes committed changes, and a view service stores the latest results. The runner loads and verifies the original history, then the amendment. Before each comparison it finishes publication, stops its relay, and waits for that process to exit (joins it), establishing a stable comparison boundary.

Expect exit zero and two strict **PASS** verifier stages:

| Stage | Source end H | Output end O | Business state |
| --- | --- | --- | --- |
| Before correction | `[3,0]` | `[5,0]` | Closes 50/30/80; Day 1 crossing |
| After correction | `[4,0]` | `[10,0]` | Closes 70/50/100; Day 2 crossing |

H is the source end and O the output end, each listed by partition. They are **exclusive offset coordinates**: positions immediately after the compared history. A durable processing position, or **frontier**, is the next offset to process; workers track source input while the view tracks published output. Offsets can have gaps, so differences are not general business-record counts. The second partition is assigned but has no fixture input. The runner compares the view with fixed expected values as well as the independent verifier.

**Artifacts:** use the exact `artifacts/showcase/run-*/` directory printed by this command. Check `run.json`, `verify-before.log`, `verify-after.log`, and `manifest.json` as described below. **Shutdown:** the runner joins its application children; dependencies remain running. Use the shared shutdown procedure after your last durable path.

## Path 3: crash during the correction

```sh
make drill DRILL=crash-mid-correction
```

This creates another fresh pipeline. It holds a specific output row lock, observes the worker's correction transaction blocked behind it, kills that owned worker, and checks the durable rollback snapshot before allowing recovery. No timing guess selects the fault boundary. The [showcase guide](showcase.md#worker-crash-barrier) explains the transaction assertions.

Expect the same two strict PASS stages and H/O boundaries as Path 2, plus unchanged durable state across the killed transaction and a higher recovered ownership epoch. An overall failure, including failed child cleanup, remains a failure even if an earlier stage printed PASS.

**Artifacts:** the printed showcase directory additionally contains `rollback-before.json` and `rollback-after.json`; `run.json.crash` records the observed barrier and recovery. **Shutdown:** application children are joined; stop dependencies below when finished.

The default `make demo` runs Paths 2 and 3 in separate namespaces. You do not need to run it as well. The Make crash command dispatches `amendsctl demo -mode crash-mid-correction`; other catalog drills use `amendsctl drill -name NAME`.

## Inspect retained evidence and stop

| Artifact | What to check |
| --- | --- |
| `run.json` | Overall `status=PASS`, stage statuses/boundaries, assigned members, and every owned child joined |
| `verify-before.log`, `verify-after.log` | Original verifier reports, complete boundaries, expected projections, no differences |
| `manifest.json` | Exact retained namespace, source/output identities, and configuration |
| `dashboard-after.html` | Progress, projections, committed aggregates/history; always **NOT VERIFIED** |
| `rollback-before.json`, `rollback-after.json` | For the crash path, unchanged durable state at the selected fault boundary |

Zero lag and READY describe observations. They do not establish independent agreement. Other drills retain `INCOMPLETE`, `BLOCKED`, or qualified verification results; a successful assertion of one of those outcomes is not verifier PASS. The [verification guide](verification.md) defines all statuses.

Each durable run prints an exact command to reverify its retained namespace. Use the same database endpoint and saved manifest. For a manually operated pipeline, stop source loaders, drain the outbox, and stop/join the relay before `verify --quiesce`; the verifier does not stop processes or publish pending intent for you.

When no other work is using this stack:

```sh
make down
```

This stops dependencies and retains containers, volumes, namespaces, and topics. Resume with `make up` and `make ready`. Do not delete volumes to resolve an ordinary startup or verification problem.

## Troubleshooting and optional checks

Check the failed command's output, Go availability, Docker client/server access, registry access, and occupied ports. `make status` and the [bounded dependency logs](runbook.md#start-inspect-and-stop) help separate setup problems from processing failures.

A restricted network may deny the Go proxy's `storage.googleapis.com` ZIP redirects. If these module downloads fail for that reason, try downloading directly from their repositories:

```sh
GOTOOLCHAIN=go1.27.2 GOWORK=off GOPROXY=direct \
  go mod download github.com/klauspost/compress github.com/pierrec/lz4/v4
GOTOOLCHAIN=go1.27.2 GOWORK=off go mod verify
```

This requires access to the module repositories and checksum service. It preserves pins, TLS, and checksum verification; an unresolved denial remains a setup failure.

For contributor validation, run `make check` with a C compiler available. It checks formatting, vets, runs fresh race-enabled tests, and builds. It is optional for this walkthrough. See [testing](testing.md#commands-by-purpose) for service-backed tests and reproducible evidence.

After the walkthrough, the important questions are what authorizes a correction, which results change, what the crash demonstrates, and why zero lag is insufficient. Automated command results establish execution evidence; understanding the design requires those explanations too.
