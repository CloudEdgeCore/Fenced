-- Workflow budget reservations close the spawn-time commitment loop.
-- Creating (declaring or dynamically spawning) a step now reserves the
-- future Task's token/cost ceiling and its task-count slot on the workflow
-- usage ledger in the same transaction, so concurrent spawns can never
-- collectively promise more than the workflow budget before any Task
-- budget ledger exists. The reservation transfers to the task's own budget
-- ledger at admission and is released when an undispatched step reaches a
-- terminal state or a workflow task terminates without establishing a
-- ledger (rejected or unbudgeted tasks).

ALTER TABLE workflow_steps
    ADD COLUMN reserved_tasks bigint NOT NULL DEFAULT 0 CHECK (reserved_tasks >= 0),
    ADD COLUMN reserved_tokens bigint NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    ADD COLUMN reserved_cost_micro_usd bigint NOT NULL DEFAULT 0 CHECK (reserved_cost_micro_usd >= 0);

ALTER TABLE workflow_usage_ledgers
    ADD COLUMN step_reserved_tasks bigint NOT NULL DEFAULT 0 CHECK (step_reserved_tasks >= 0),
    ADD COLUMN step_reserved_tokens bigint NOT NULL DEFAULT 0 CHECK (step_reserved_tokens >= 0),
    ADD COLUMN step_reserved_cost_micro_usd bigint NOT NULL DEFAULT 0 CHECK (step_reserved_cost_micro_usd >= 0);

-- Upgrade backfill: steps that were declared or spawned but never
-- dispatched still hold one future task each. Token and cost ceilings
-- cannot be reconstructed without re-deriving merged task specs, so those
-- dimensions stay zero (a conservative under-commitment that clears as the
-- pre-upgrade backlog drains).
UPDATE workflow_steps SET reserved_tasks = 1
    WHERE status IN ('PENDING', 'WAITING_APPROVAL');
UPDATE workflow_usage_ledgers l SET step_reserved_tasks = s.held, updated_at = now()
FROM (
    SELECT tenant_id, workflow_id, COUNT(*) AS held FROM workflow_steps
    WHERE status IN ('PENDING', 'WAITING_APPROVAL')
    GROUP BY tenant_id, workflow_id
) s
WHERE l.tenant_id = s.tenant_id AND l.workflow_id = s.workflow_id;

-- A terminal workflow task can no longer establish its budget ledger, so
-- whatever step reservation still covers it is released (this covers
-- rejected and unbudgeted tasks; dispatched-and-admitted tasks already
-- transferred their reservation through the budget-insert trigger). The
-- step row is locked before the ledger row so every reservation path
-- (spawn, step transition, admission transfer, terminal release) acquires
-- workflow_steps before workflow_usage_ledgers and cannot deadlock.
CREATE OR REPLACE FUNCTION fenced_workflow_usage_task_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE budget task_budget_ledgers%ROWTYPE; hold_tasks bigint; hold_tokens bigint; hold_cost bigint;
BEGIN
  IF NEW.workflow_id IS NOT NULL AND OLD.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','REJECTED')
     AND NEW.phase IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','REJECTED') THEN
    SELECT * INTO budget FROM task_budget_ledgers WHERE tenant_id=NEW.tenant_id AND task_id=NEW.id;
    SELECT reserved_tasks, reserved_tokens, reserved_cost_micro_usd INTO hold_tasks, hold_tokens, hold_cost
      FROM workflow_steps WHERE tenant_id=NEW.tenant_id AND id=NEW.workflow_step_id FOR UPDATE;
    UPDATE workflow_usage_ledgers SET pending_tasks=GREATEST(0,pending_tasks-1),
      reserved_tokens=GREATEST(0,reserved_tokens-GREATEST(COALESCE(budget.reserved_tokens,0)-COALESCE(budget.consumed_tokens,0),0)),
      reserved_cost_micro_usd=GREATEST(0,reserved_cost_micro_usd-GREATEST(COALESCE(budget.reserved_cost_micro_usd,0)-COALESCE(budget.consumed_cost_micro_usd,0),0)),
      step_reserved_tasks=GREATEST(0,step_reserved_tasks-COALESCE(hold_tasks,0)),
      step_reserved_tokens=GREATEST(0,step_reserved_tokens-COALESCE(hold_tokens,0)),
      step_reserved_cost_micro_usd=GREATEST(0,step_reserved_cost_micro_usd-COALESCE(hold_cost,0)),
      updated_at=now()
    WHERE tenant_id=NEW.tenant_id AND workflow_id=NEW.workflow_id;
    UPDATE workflow_steps SET reserved_tasks=0, reserved_tokens=0, reserved_cost_micro_usd=0
    WHERE tenant_id=NEW.tenant_id AND id=NEW.workflow_step_id;
  END IF;
  RETURN NEW;
END $$;

-- The task's own budget ledger now carries the ceiling: transfer the
-- originating step's outstanding token/cost reservation so the commitment
-- is counted exactly once across the spawn and admission transactions. The
-- step row locks before the ledger row (see task-terminal note above).
CREATE OR REPLACE FUNCTION fenced_workflow_usage_budget_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE workflow uuid; lineage_step uuid; terminal boolean; hold_tokens bigint; hold_cost bigint;
BEGIN
  SELECT workflow_id, workflow_step_id, phase IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','REJECTED')
    INTO workflow, lineage_step, terminal
    FROM tasks WHERE tenant_id=NEW.tenant_id AND id=NEW.task_id;
  IF workflow IS NOT NULL AND NOT terminal AND lineage_step IS NOT NULL THEN
    SELECT reserved_tokens, reserved_cost_micro_usd INTO hold_tokens, hold_cost
      FROM workflow_steps WHERE tenant_id=NEW.tenant_id AND id=lineage_step FOR UPDATE;
    UPDATE workflow_usage_ledgers SET reserved_tokens=reserved_tokens+NEW.reserved_tokens,
      reserved_cost_micro_usd=reserved_cost_micro_usd+NEW.reserved_cost_micro_usd,
      step_reserved_tokens=GREATEST(0,step_reserved_tokens-COALESCE(hold_tokens,0)),
      step_reserved_cost_micro_usd=GREATEST(0,step_reserved_cost_micro_usd-COALESCE(hold_cost,0)),
      updated_at=now()
    WHERE tenant_id=NEW.tenant_id AND workflow_id=workflow;
    UPDATE workflow_steps SET reserved_tokens=0, reserved_cost_micro_usd=0
    WHERE tenant_id=NEW.tenant_id AND id=lineage_step;
  END IF;
  RETURN NEW;
END $$;

-- The step insert itself commits the reservation: the AFTER INSERT trigger
-- moves the new row's reserved_* onto the usage ledger inside the same
-- statement, keeping the hot spawn path at one round trip fewer than a
-- separate ledger UPDATE.
CREATE FUNCTION fenced_workflow_usage_step_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE workflow_usage_ledgers SET
    step_reserved_tasks = step_reserved_tasks + NEW.reserved_tasks,
    step_reserved_tokens = step_reserved_tokens + NEW.reserved_tokens,
    step_reserved_cost_micro_usd = step_reserved_cost_micro_usd + NEW.reserved_cost_micro_usd,
    updated_at = now()
  WHERE tenant_id = NEW.tenant_id AND workflow_id = NEW.workflow_id;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_step_insert AFTER INSERT ON workflow_steps
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_step_insert();
