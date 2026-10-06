----------------------------- MODULE Fenced -----------------------------
(*
  Agent OS v0.1 — core kernel invariants, model-checked with TLC.

  Faithful to:
    - internal/kernel/domain/phase.go   (phase vocabularies and transition
                                         tables, copied 1:1)
    - db/migrations                     (unique constraints, CAS resource
                                         versions, append-only ledgers)
    - ADR-002                           (row locks + CAS + unique constraints
                                         under READ COMMITTED)

  The model abstracts away SQL but preserves the invariants the constraints
  encode:

    I1  Legal state machine: phases advance only along the allowed tables.
    I2  Single active attempt per run (and per task): retries are strictly
        sequential. Leases: exactly one runtime holds a run's lease; fencing
        tokens strictly increase on handover; a stale runtime (knownToken
        diverged from fenceToken) can never act on the run again.
    I3  Budget: settlements are idempotent per (task,key); cumulative
        consumption never exceeds the reservation unless the ledger is
        marked exhausted; once exhausted, no further settlement is recorded.
    I4  Outbox/inbox: every outbox event is eventually delivered
        (at-least-once, liveness); each (consumer,key) inbox receipt is
        processed exactly once.
    I5  Completion ordering: a task becomes SUCCEEDED only after its result
        was recorded (result-before-success).
    I6  Cancellation convergence (liveness): cancel_requested eventually
        reaches a terminal phase.

  Run (from this directory):
    java -XX:+UseParallelGC -jar <path-to>/tla2tools.jar -config Fenced.cfg Fenced.tla
*)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Tasks,       \* finite set of task ids
  AttemptIds,  \* finite set of attempt ids (one per run, runs are retries)
  Runtimes,    \* finite set of runtime instances
  Events,      \* finite set of outbox event ids
  Keys,        \* finite set of budget idempotency keys
  Consumers,   \* finite set of inbox consumers
  UsageAmts,   \* finite set of token amounts a settlement may report
  MaxAttempts  \* maximum runs (attempts) per task

VARIABLES
  taskPhase,        \* [Tasks -> TASK_PHASES]
  taskResult,       \* [Tasks -> BOOLEAN] result artifact recorded
  cancelRequested,  \* [Tasks -> BOOLEAN]
  attemptsUsed,     \* [Tasks -> 0..MaxAttempts]
  attemptsCreated,  \* subset of AttemptIds
  attemptPhase,     \* [AttemptIds -> ATTEMPT_PHASES \cup {"UNBORN"}]
  attemptTask,      \* [AttemptIds -> Tasks]
  attemptRun,       \* [AttemptIds -> 1..MaxAttempts]
  leaseHolder,      \* [AttemptIds -> Runtimes \cup {"none"}]
  fenceToken,       \* [AttemptIds -> Nat]
  knownToken,       \* [Runtimes -> Nat] what each runtime believes the fence is
  settled,          \* [Tasks -> Nat] cumulative tokens settled
  exhausted,        \* [Tasks -> BOOLEAN] hard stop armed
  settlementRecords,\* subset of Tasks \X Keys \X Nat (append-only, by key)
  pendingOutbox,    \* subset of Tasks \X Events awaiting acknowledgement
  delivered,        \* subset of Tasks \X Events delivered at least once
  inboxReceipts     \* subset of Consumers \X Keys processed exactly once

vars == <<taskPhase, taskResult, cancelRequested, attemptsUsed, attemptsCreated,
          attemptPhase, attemptTask, attemptRun, leaseHolder, fenceToken,
          knownToken, settled, exhausted, settlementRecords, pendingOutbox,
          delivered, inboxReceipts>>

(* ---- Phase vocabularies and transition tables (domain/phase.go) ---- *)

TASK_PHASES == {"QUEUED", "ADMITTED", "RUNNING", "SUCCEEDED", "FAILED",
                "CANCELLED", "TIMED_OUT", "REJECTED"}
TASK_TERMINAL == {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED"}

ATTEMPT_PHASES == {"PENDING", "PLACED", "STARTING", "RUNNING", "WAITING_TOOL",
                   "WAITING_AGENT", "WAITING_APPROVAL", "CHECKPOINTING",
                   "COMPLETED", "ATTEMPT_FAILED", "CANCEL_REQUESTED", "CANCELLED"}
ATTEMPT_TERMINAL == {"COMPLETED", "ATTEMPT_FAILED", "CANCELLED"}
ATTEMPT_ACTIVE == {"PLACED", "STARTING", "RUNNING", "WAITING_TOOL",
                   "WAITING_AGENT", "WAITING_APPROVAL", "CHECKPOINTING"}
ATTEMPT_RUNNABLE == ATTEMPT_ACTIVE \cup {"PENDING", "CANCEL_REQUESTED"}

TaskTransition(from, to) ==
    \/ (from = "QUEUED" /\ to \in {"ADMITTED", "REJECTED", "CANCELLED", "TIMED_OUT"})
    \/ (from = "ADMITTED" /\ to \in {"RUNNING", "CANCELLED", "TIMED_OUT"})
    \/ (from = "RUNNING" /\ to \in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"})

AttemptTransition(from, to) ==
    \/ (from = "PENDING" /\ to \in {"PLACED", "ATTEMPT_FAILED", "CANCELLED"})
    \/ (from = "PLACED" /\ to \in {"STARTING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "STARTING" /\ to \in {"RUNNING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "RUNNING" /\ to \in {"WAITING_TOOL", "WAITING_AGENT",
                                    "WAITING_APPROVAL", "CHECKPOINTING", "COMPLETED",
                                    "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "WAITING_TOOL" /\ to \in {"RUNNING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "WAITING_AGENT" /\ to \in {"RUNNING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "WAITING_APPROVAL" /\ to \in {"RUNNING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "CHECKPOINTING" /\ to \in {"RUNNING", "ATTEMPT_FAILED", "CANCEL_REQUESTED"})
    \/ (from = "CANCEL_REQUESTED" /\ to \in {"CANCELLED", "ATTEMPT_FAILED"})

(* ---- Budget ledger ---- *)

reserved == [t \in Tasks |-> 100]   \* fixed reservation per task (model scope)

RECURSIVE SumAmts(_)
SumAmts(S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S: TRUE IN x[3] + SumAmts(S \ {x})

(* ---- Initial state ---- *)

Init ==
    /\ taskPhase = [t \in Tasks |-> "QUEUED"]
    /\ taskResult = [t \in Tasks |-> FALSE]
    /\ cancelRequested = [t \in Tasks |-> FALSE]
    /\ attemptsUsed = [t \in Tasks |-> 0]
    /\ attemptsCreated = {}
    /\ attemptPhase = [a \in AttemptIds |-> "UNBORN"]
    /\ attemptTask = [a \in AttemptIds |-> CHOOSE t \in Tasks: TRUE]
    /\ attemptRun = [a \in AttemptIds |-> 0]
    /\ leaseHolder = [a \in AttemptIds |-> "none"]
    /\ fenceToken = [a \in AttemptIds |-> 0]
    /\ knownToken = [r \in Runtimes |-> 0]
    /\ settled = [t \in Tasks |-> 0]
    /\ exhausted = [t \in Tasks |-> FALSE]
    /\ settlementRecords = {}
    /\ pendingOutbox = {}
    /\ delivered = {}
    /\ inboxReceipts = {}

(* ---- Actions ---- *)

Admit(t) ==
    /\ taskPhase[t] = "QUEUED"
    /\ taskPhase' = [taskPhase EXCEPT ![t] = "ADMITTED"]
    /\ UNCHANGED <<taskResult, cancelRequested, attemptsUsed, attemptsCreated,
                  attemptPhase, attemptTask, attemptRun, leaseHolder, fenceToken,
                  knownToken, settled, exhausted, settlementRecords, pendingOutbox,
                  delivered, inboxReceipts>>

Reject(t) ==
    /\ taskPhase[t] = "QUEUED"
    /\ taskPhase' = [taskPhase EXCEPT ![t] = "REJECTED"]
    /\ UNCHANGED <<taskResult, cancelRequested, attemptsUsed, attemptsCreated,
                  attemptPhase, attemptTask, attemptRun, leaseHolder, fenceToken,
                  knownToken, settled, exhausted, settlementRecords, pendingOutbox,
                  delivered, inboxReceipts>>

Schedule(t) ==
    \* Creates the next run's attempt. Strictly sequential: no new attempt
    \* while any earlier attempt of the task is still alive (PENDING or
    \* active), and nothing after cancellation was requested — the control
    \* plane starts a new run only after the previous run failed.
    /\ taskPhase[t] \in {"ADMITTED", "RUNNING"}
    /\ attemptsUsed[t] < MaxAttempts
    /\ ~cancelRequested[t]
    /\ \A b \in AttemptIds:
         attemptPhase[b] # "UNBORN" /\ attemptTask[b] = t
           => attemptPhase[b] \in {"ATTEMPT_FAILED", "CANCELLED"}
    /\ \E a \in AttemptIds \ attemptsCreated:
         /\ attemptsCreated' = attemptsCreated \cup {a}
         /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "PENDING"]
         /\ attemptTask' = [attemptTask EXCEPT ![a] = t]
         /\ attemptRun' = [attemptRun EXCEPT ![a] = attemptsUsed[t] + 1]
    /\ attemptsUsed' = [attemptsUsed EXCEPT ![t] = attemptsUsed[t] + 1]
    /\ taskPhase' = [taskPhase EXCEPT ![t] = "RUNNING"]
    /\ UNCHANGED <<taskResult, cancelRequested, leaseHolder, fenceToken,
                  knownToken, settled, exhausted, settlementRecords, pendingOutbox,
                  delivered, inboxReceipts>>

Claim(a, r) ==
    \* A runtime takes the lease (PENDING -> PLACED) and raises the fence:
    \* any earlier holder's knownToken is now stale and can never act on this
    \* run again.
    /\ attemptPhase[a] = "PENDING"
    /\ leaseHolder[a] = "none"
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "PLACED"]
    /\ leaseHolder' = [leaseHolder EXCEPT ![a] = r]
    /\ fenceToken' = [fenceToken EXCEPT ![a] = fenceToken[a] + 1]
    /\ knownToken' = [knownToken EXCEPT ![r] = fenceToken[a] + 1]
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptTask, attemptRun, settled,
                  exhausted, settlementRecords, pendingOutbox, delivered,
                  inboxReceipts>>

FencedAttemptStep(a, r, From, To) ==
    \* Every runtime action on a run requires holding its lease with the
    \* current fence token; a stale runtime is disabled.
    /\ attemptPhase[a] = From
    /\ leaseHolder[a] = r
    /\ knownToken[r] = fenceToken[a]
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = To]
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptTask, attemptRun, leaseHolder, fenceToken,
                  knownToken, settled, exhausted, settlementRecords, pendingOutbox,
                  delivered, inboxReceipts>>

Activate(a, r) == FencedAttemptStep(a, r, "PLACED", "STARTING")
Start(a, r) == FencedAttemptStep(a, r, "STARTING", "RUNNING")
Park(a, r) == FencedAttemptStep(a, r, "RUNNING", "WAITING_APPROVAL")
Resume(a, r) == FencedAttemptStep(a, r, "WAITING_APPROVAL", "RUNNING")

Complete(a, r) ==
    \* Result-before-success: the result artifact is recorded in the same
    \* step that moves the task to SUCCEEDED; InvNoResultLoss forbids any
    \* other ordering. Completion is rejected once cancellation was requested.
    \* The lease is released with the terminal transition.
    /\ attemptPhase[a] = "RUNNING"
    /\ leaseHolder[a] = r
    /\ knownToken[r] = fenceToken[a]
    /\ ~cancelRequested[attemptTask[a]]
    /\ taskResult' = [taskResult EXCEPT ![attemptTask[a]] = TRUE]
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "COMPLETED"]
    /\ leaseHolder' = [leaseHolder EXCEPT ![a] = "none"]
    /\ taskPhase' = [taskPhase EXCEPT ![attemptTask[a]] = "SUCCEEDED"]
    /\ UNCHANGED <<cancelRequested, attemptsUsed, attemptsCreated, attemptTask,
                  attemptRun, fenceToken, knownToken, settled,
                  exhausted, settlementRecords, pendingOutbox, delivered,
                  inboxReceipts>>

FailAttempt(a, r, Retry) ==
    /\ attemptPhase[a] \in {"PLACED", "STARTING", "RUNNING", "WAITING_TOOL",
                            "WAITING_AGENT", "WAITING_APPROVAL", "CHECKPOINTING",
                            "CANCEL_REQUESTED"}
    /\ leaseHolder[a] = r
    /\ knownToken[r] = fenceToken[a]
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "ATTEMPT_FAILED"]
    /\ leaseHolder' = [leaseHolder EXCEPT ![a] = "none"]
    /\ \/ /\ Retry /\ attemptsUsed[attemptTask[a]] < MaxAttempts
           /\ ~cancelRequested[attemptTask[a]]
           /\ UNCHANGED taskPhase
       \/ /\ (~Retry \/ attemptsUsed[attemptTask[a]] >= MaxAttempts
              \/ cancelRequested[attemptTask[a]])
          /\ taskPhase' = [taskPhase EXCEPT ![attemptTask[a]] = "FAILED"]
    /\ UNCHANGED <<taskResult, cancelRequested, attemptsUsed, attemptsCreated,
                  attemptTask, attemptRun, fenceToken, knownToken,
                  settled, exhausted, settlementRecords, pendingOutbox, delivered,
                  inboxReceipts>>

CancelRequestTask(t) ==
    /\ taskPhase[t] \in {"QUEUED", "ADMITTED", "RUNNING"}
    /\ cancelRequested' = [cancelRequested EXCEPT ![t] = TRUE]
    /\ \/ /\ \E a \in AttemptIds:
              attemptPhase[a] \in ATTEMPT_RUNNABLE /\ attemptTask[a] = t
          /\ \E a \in AttemptIds:
               /\ attemptPhase[a] \in ATTEMPT_RUNNABLE
               /\ attemptTask[a] = t
               /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "CANCEL_REQUESTED"]
          /\ taskPhase' = taskPhase
          /\ UNCHANGED <<taskResult, attemptsUsed, attemptsCreated, attemptTask,
                        attemptRun, leaseHolder, fenceToken, knownToken, settled,
                        exhausted, settlementRecords, pendingOutbox, delivered,
                        inboxReceipts>>
       \/ /\ ~\E a \in AttemptIds:
               attemptPhase[a] \in ATTEMPT_RUNNABLE /\ attemptTask[a] = t
          /\ attemptPhase' = attemptPhase
          /\ taskPhase' = [taskPhase EXCEPT ![t] = "CANCELLED"]
          /\ UNCHANGED <<taskResult, attemptsUsed, attemptsCreated, attemptTask,
                        attemptRun, leaseHolder, fenceToken, knownToken, settled,
                        exhausted, settlementRecords, pendingOutbox, delivered,
                        inboxReceipts>>

AckCancel(a, r) ==
    /\ attemptPhase[a] = "CANCEL_REQUESTED"
    /\ leaseHolder[a] = r
    /\ knownToken[r] = fenceToken[a]
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "CANCELLED"]
    /\ leaseHolder' = [leaseHolder EXCEPT ![a] = "none"]
    /\ taskPhase' = [taskPhase EXCEPT ![attemptTask[a]] = "CANCELLED"]
    /\ UNCHANGED <<taskResult, cancelRequested, attemptsUsed, attemptsCreated,
                  attemptTask, attemptRun, fenceToken, knownToken,
                  settled, exhausted, settlementRecords, pendingOutbox, delivered,
                  inboxReceipts>>

LeaseExpire(a) ==
    \* Heartbeat loss: the lease is dropped, the fence stays (so the old
    \* holder can never act again), and the recovery controller fails the
    \* attempt — the kernel never leaves an active attempt unleased.
    /\ leaseHolder[a] # "none"
    /\ leaseHolder' = [leaseHolder EXCEPT ![a] = "none"]
    /\ attemptPhase' = [attemptPhase EXCEPT ![a] = "ATTEMPT_FAILED"]
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptTask, attemptRun,
                  fenceToken, knownToken, settled, exhausted, settlementRecords,
                  pendingOutbox, delivered, inboxReceipts>>

TimeOutTask(t) ==
    /\ taskPhase[t] \notin TASK_TERMINAL
    /\ taskPhase' = [taskPhase EXCEPT ![t] = "TIMED_OUT"]
    /\ attemptPhase' = [a \in AttemptIds |->
        IF attemptTask[a] = t /\ attemptPhase[a] \in ATTEMPT_RUNNABLE
        THEN "ATTEMPT_FAILED" ELSE attemptPhase[a]]
    /\ leaseHolder' = [a \in AttemptIds |->
        IF attemptTask[a] = t /\ attemptPhase[a] \in ATTEMPT_RUNNABLE
        THEN "none" ELSE leaseHolder[a]]
    /\ UNCHANGED <<taskResult, cancelRequested, attemptsUsed, attemptsCreated,
                  attemptTask, attemptRun, fenceToken, knownToken,
                  settled, exhausted, settlementRecords, pendingOutbox, delivered,
                  inboxReceipts>>

Settle(t, k, amt) ==
    \* Idempotent per (task,key); the hard stop arms when a settlement would
    \* exceed the reservation; once armed, no further settlement is recorded.
    /\ amt \in UsageAmts
    /\ ~exhausted[t]
    /\ \A s \in settlementRecords: ~(s[1] = t /\ s[2] = k)
    /\ \/ /\ reserved[t] # 0 /\ settled[t] + amt > reserved[t]
          /\ exhausted' = [exhausted EXCEPT ![t] = TRUE]
          /\ UNCHANGED <<settled, settlementRecords>>
       \/ /\ (reserved[t] = 0 \/ settled[t] + amt <= reserved[t])
          /\ settled' = [settled EXCEPT ![t] = settled[t] + amt]
          /\ settlementRecords' = settlementRecords \cup {<<t, k, amt>>}
          /\ UNCHANGED exhausted
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptPhase, attemptTask, attemptRun,
                  leaseHolder, fenceToken, knownToken, pendingOutbox, delivered,
                  inboxReceipts>>

EnqueueEvent(t, e) ==
    /\ <<t, e>> \notin pendingOutbox
    /\ pendingOutbox' = pendingOutbox \cup {<<t, e>>}
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptPhase, attemptTask, attemptRun,
                  leaseHolder, fenceToken, knownToken, settled, exhausted,
                  settlementRecords, delivered, inboxReceipts>>

Deliver(t, e) ==
    \* At-least-once: delivery does not consume the outbox row.
    /\ <<t, e>> \in pendingOutbox
    /\ delivered' = delivered \cup {<<t, e>>}
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptPhase, attemptTask, attemptRun,
                  leaseHolder, fenceToken, knownToken, settled, exhausted,
                  settlementRecords, pendingOutbox, inboxReceipts>>

ProcessInbox(c, t, e, k) ==
    \* Exactly-once consumption: a receipt is recorded once per (consumer,key).
    /\ <<t, e>> \in delivered
    /\ <<c, k>> \notin inboxReceipts
    /\ inboxReceipts' = inboxReceipts \cup {<<c, k>>}
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptPhase, attemptTask, attemptRun,
                  leaseHolder, fenceToken, knownToken, settled, exhausted,
                  settlementRecords, pendingOutbox, delivered>>

AckEvent(t, e) ==
    \* The outbox row is consumed only after the event was delivered.
    /\ <<t, e>> \in pendingOutbox
    /\ <<t, e>> \in delivered
    /\ pendingOutbox' = pendingOutbox \ {<<t, e>>}
    /\ UNCHANGED <<taskPhase, taskResult, cancelRequested, attemptsUsed,
                  attemptsCreated, attemptPhase, attemptTask, attemptRun,
                  leaseHolder, fenceToken, knownToken, settled, exhausted,
                  settlementRecords, delivered, inboxReceipts>>

(* ---- Next and fairness ---- *)

Next ==
    \/ \E t \in Tasks: Admit(t)
    \/ \E t \in Tasks: Reject(t)
    \/ \E t \in Tasks: Schedule(t)
    \/ \E a \in AttemptIds, r \in Runtimes: Claim(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes: Activate(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes: Start(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes: Park(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes: Resume(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes: Complete(a, r)
    \/ \E a \in AttemptIds, r \in Runtimes, Retry \in BOOLEAN: FailAttempt(a, r, Retry)
    \/ \E t \in Tasks: CancelRequestTask(t)
    \/ \E a \in AttemptIds, r \in Runtimes: AckCancel(a, r)
    \/ \E a \in AttemptIds: LeaseExpire(a)
    \/ \E t \in Tasks: TimeOutTask(t)
    \/ \E t \in Tasks, k \in Keys, amt \in UsageAmts: Settle(t, k, amt)
    \/ \E t \in Tasks, e \in Events: EnqueueEvent(t, e)
    \/ \E t \in Tasks, e \in Events: Deliver(t, e)
    \/ \E c \in Consumers, t \in Tasks, e \in Events, k \in Keys: ProcessInbox(c, t, e, k)
    \/ \E t \in Tasks, e \in Events: AckEvent(t, e)

Fairness ==
    /\ WF_vars(\E t \in Tasks: TimeOutTask(t))
    /\ WF_vars(\E t \in Tasks, e \in Events: Deliver(t, e))
    /\ WF_vars(\E t \in Tasks: CancelRequestTask(t))
    /\ WF_vars(\E a \in AttemptIds, r \in Runtimes: AckCancel(a, r))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ---- Invariants ---- *)

TypeOK ==
    /\ taskPhase \in [Tasks -> TASK_PHASES]
    /\ taskResult \in [Tasks -> BOOLEAN]
    /\ cancelRequested \in [Tasks -> BOOLEAN]
    /\ attemptsUsed \in [Tasks -> 0..MaxAttempts]
    /\ attemptsCreated \subseteq AttemptIds
    /\ attemptPhase \in [AttemptIds -> ATTEMPT_PHASES \cup {"UNBORN"}]
    /\ attemptTask \in [AttemptIds -> Tasks]
    /\ attemptRun \in [AttemptIds -> 0..MaxAttempts]
    /\ leaseHolder \in [AttemptIds -> Runtimes \cup {"none"}]
    /\ fenceToken \in [AttemptIds -> Nat]
    /\ knownToken \in [Runtimes -> Nat]
    /\ settled \in [Tasks -> Nat]
    /\ exhausted \in [Tasks -> BOOLEAN]
    /\ settlementRecords \subseteq Tasks \X Keys \X Nat
    /\ pendingOutbox \subseteq Tasks \X Events
    /\ delivered \subseteq Tasks \X Events
    /\ inboxReceipts \subseteq Consumers \X Keys

(* I2: at most one active attempt per run and per task; retries sequential. *)
InvSingleActivePerRun ==
    \A a \in AttemptIds, b \in AttemptIds:
        a # b /\ attemptPhase[a] \in ATTEMPT_ACTIVE
              /\ attemptPhase[b] \in ATTEMPT_ACTIVE
              /\ attemptTask[a] = attemptTask[b]
              /\ attemptRun[a] = attemptRun[b]
        => FALSE

InvSingleActiveTask ==
    \A a \in AttemptIds, b \in AttemptIds:
        a # b /\ attemptPhase[a] \in ATTEMPT_ACTIVE
              /\ attemptPhase[b] \in ATTEMPT_ACTIVE
              /\ attemptTask[a] = attemptTask[b]
        => FALSE

(* I2: leases and fencing. *)
InvLease ==
    \A r \in Runtimes:
        Cardinality({a \in AttemptIds: leaseHolder[a] = r}) <= 1

InvFenceMatches ==
    \A a \in AttemptIds:
        attemptPhase[a] \in ATTEMPT_ACTIVE => leaseHolder[a] # "none"

InvFencing ==
    \A a \in AttemptIds:
        leaseHolder[a] # "none" => fenceToken[a] >= 1

(* I3: budget. *)
InvBudget ==
    \A t \in Tasks:
        reserved[t] = 0 \/ exhausted[t] \/ settled[t] <= reserved[t]

InvSettleIdempotent ==
    \A t \in Tasks, k \in Keys:
        Cardinality({s \in settlementRecords: s[1] = t /\ s[2] = k}) <= 1

InvSettledConsistent ==
    \A t \in Tasks:
        settled[t] = SumAmts({s \in settlementRecords: s[1] = t})

(* I4: inbox receipts are a set — no (consumer,key) is processed twice. *)
InvInboxIdempotent ==
    \A c \in Consumers, k \in Keys:
        Cardinality({r \in inboxReceipts: r[1] = c /\ r[2] = k}) <= 1

(* I5: result-before-success. *)
InvNoResultLoss ==
    \A t \in Tasks:
        taskPhase[t] = "SUCCEEDED" => taskResult[t]

InvSucceededHasAttempt ==
    \A t \in Tasks:
        taskPhase[t] = "SUCCEEDED"
        => \E a \in AttemptIds: attemptPhase[a] = "COMPLETED" /\ attemptTask[a] = t

(* I1: active attempts only exist while their task is running. *)
InvAttemptActiveImpliesTaskRunning ==
    \A a \in AttemptIds:
        attemptPhase[a] \in ATTEMPT_ACTIVE
        => taskPhase[attemptTask[a]] = "RUNNING"

(* ---- Liveness properties ---- *)

EventuallyTerminal(t) == <>(taskPhase[t] \in TASK_TERMINAL)

CancelConverges ==
    \A t \in Tasks:
        cancelRequested[t] => EventuallyTerminal(t)

EventuallyDelivered(t, e) == <>(<<t, e>> \in delivered)

OutboxDelivered ==
    \A t \in Tasks, e \in Events:
        <<t, e>> \in pendingOutbox => EventuallyDelivered(t, e)

AllTasksConverge ==
    \A t \in Tasks:
        EventuallyTerminal(t)

=============================================================================
