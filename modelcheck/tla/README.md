# TLA+ Model Checking

Status: **done for v0.1 core invariants** (2026-08-16).

`Fenced.tla` models the kernel invariants the tech baseline
§18.4 requires verifying before they are trusted in production:

- **I1** Legal state machine: phases advance only along the allowed tables
  (`internal/kernel/domain/phase.go`, copied 1:1).
- **I2** Single active attempt per run and per task; retries strictly
  sequential; leases held by exactly one runtime; fencing tokens strictly
  increase on handover; a stale runtime (diverged knownToken) can never act
  again.
- **I3** Budget: settlements idempotent per (task,key); cumulative
  consumption never exceeds the reservation unless the ledger is marked
  exhausted; once exhausted, no further settlement is recorded.
- **I4** Outbox/inbox: every outbox event is eventually delivered
  (at-least-once); each (consumer,key) inbox receipt is processed exactly
  once.
- **I5** Completion ordering: a task becomes SUCCEEDED only after its result
  was recorded (result-before-success).
- **I6** Cancellation convergence (liveness): cancel_requested eventually
  reaches a terminal phase.

## Running

```powershell
# one-time: download TLC (Java 17 required)
Invoke-WebRequest https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar `
    -OutFile ../../tmp/tools/tla/tla2tools.jar

# model check (from this directory)
java -XX:+UseParallelGC -jar ../../tmp/tools/tla/tla2tools.jar -config Fenced.cfg Fenced.tla
```

## Verified results (2026-08-16, TLC 2026.08.11)

Model scope: 1 task, 2 attempts (max 2 runs), 2 runtimes, 1 outbox event,
1 budget key, 1 consumer, usage amounts {40, 80}, reservation 100 tokens.

```text
16,879 states generated, 3,222 distinct states found
depth 15; all 13 invariants + 3 liveness properties hold
Model checking completed. No error has been found.
```

## Fidelity bugs the checker caught (fixed in the model, not the system)

The first runs failed because the *model* diverged from the implemented
kernel — exactly what the README warns about ("a model that diverges from
the code is a bug report"):

1. `CancelRequestTask` left `attemptPhase` unassigned on the no-attempt
   branch (TLC reports `null`).
2. `Schedule` allowed a second PENDING attempt while the first was still
   unclaimed, and `Claim` did not move PENDING → PLACED; the checker found
   one runtime holding two leases (`InvLease`).
3. Terminal transitions (`Complete`/`FailAttempt`/`AckCancel`/`TimeOutTask`)
   did not release the lease; the real store releases it on terminal
   transitions (`runtime_leases.released_at`, see
   `internal/kernel/store/postgres/runtime.go`).

Models must match the state machines implemented in
`internal/kernel/domain` and the database constraints in `db/migrations`;
a model that diverges from the code is a bug report, not a document.
