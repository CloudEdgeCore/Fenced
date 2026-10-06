-- Pre-aggregated workflow usage removes COUNT/SUM scans from the dynamic
-- spawn serialization path. Trigger updates share the originating task or
-- settlement transaction, so the ledger cannot lag a committed guard input.
CREATE TABLE workflow_usage_ledgers (
    tenant_id text NOT NULL,
    workflow_id uuid NOT NULL,
    task_count bigint NOT NULL DEFAULT 0 CHECK (task_count >= 0),
    settled_tokens bigint NOT NULL DEFAULT 0 CHECK (settled_tokens >= 0),
    settled_cost_micro_usd bigint NOT NULL DEFAULT 0 CHECK (settled_cost_micro_usd >= 0),
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    reserved_cost_micro_usd bigint NOT NULL DEFAULT 0 CHECK (reserved_cost_micro_usd >= 0),
    pending_tasks bigint NOT NULL DEFAULT 0 CHECK (pending_tasks >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, workflow_id),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE CASCADE
);

CREATE FUNCTION fenced_workflow_usage_workflow_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO workflow_usage_ledgers(tenant_id,workflow_id,updated_at)
    VALUES(NEW.tenant_id,NEW.id,now()) ON CONFLICT DO NOTHING;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_workflow_insert AFTER INSERT ON workflows
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_workflow_insert();

INSERT INTO workflow_usage_ledgers(tenant_id,workflow_id,task_count,settled_tokens,
    settled_cost_micro_usd,reserved_tokens,reserved_cost_micro_usd,pending_tasks,updated_at)
SELECT w.tenant_id,w.id,COUNT(t.id),COALESCE(SUM(s.tokens),0),COALESCE(SUM(s.cost_micro_usd),0),
    COALESCE(SUM(CASE WHEN t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED') THEN
      GREATEST(COALESCE(l.reserved_tokens,0)-COALESCE(l.consumed_tokens,0),0) ELSE 0 END),0),
    COALESCE(SUM(CASE WHEN t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED') THEN
      GREATEST(COALESCE(l.reserved_cost_micro_usd,0)-COALESCE(l.consumed_cost_micro_usd,0),0) ELSE 0 END),0),
    COUNT(t.id) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),now()
FROM workflows w LEFT JOIN tasks t ON t.tenant_id=w.tenant_id AND t.workflow_id=w.id
LEFT JOIN task_budget_ledgers l ON l.tenant_id=t.tenant_id AND l.task_id=t.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(x.tokens),0) tokens,COALESCE(SUM(x.cost_micro_usd),0) cost_micro_usd
    FROM task_budget_settlements x WHERE x.tenant_id=t.tenant_id AND x.task_id=t.id
) s ON true
GROUP BY w.tenant_id,w.id;

CREATE FUNCTION fenced_workflow_usage_task_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.workflow_id IS NOT NULL THEN
    INSERT INTO workflow_usage_ledgers(tenant_id,workflow_id,task_count,pending_tasks,updated_at)
      VALUES(NEW.tenant_id,NEW.workflow_id,1,CASE WHEN NEW.phase IN ('SUCCEEDED','FAILED','CANCELLED') THEN 0 ELSE 1 END,now())
    ON CONFLICT(tenant_id,workflow_id) DO UPDATE SET
      task_count=workflow_usage_ledgers.task_count+1,
      pending_tasks=workflow_usage_ledgers.pending_tasks+EXCLUDED.pending_tasks,updated_at=now();
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_task_insert AFTER INSERT ON tasks
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_task_insert();

CREATE FUNCTION fenced_workflow_usage_task_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE budget task_budget_ledgers%ROWTYPE;
BEGIN
  IF NEW.workflow_id IS NOT NULL AND OLD.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')
     AND NEW.phase IN ('SUCCEEDED','FAILED','CANCELLED') THEN
    SELECT * INTO budget FROM task_budget_ledgers WHERE tenant_id=NEW.tenant_id AND task_id=NEW.id;
    UPDATE workflow_usage_ledgers SET pending_tasks=GREATEST(0,pending_tasks-1),
      reserved_tokens=GREATEST(0,reserved_tokens-GREATEST(COALESCE(budget.reserved_tokens,0)-COALESCE(budget.consumed_tokens,0),0)),
      reserved_cost_micro_usd=GREATEST(0,reserved_cost_micro_usd-GREATEST(COALESCE(budget.reserved_cost_micro_usd,0)-COALESCE(budget.consumed_cost_micro_usd,0),0)),updated_at=now()
    WHERE tenant_id=NEW.tenant_id AND workflow_id=NEW.workflow_id;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_task_terminal AFTER UPDATE OF phase ON tasks
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_task_terminal();

CREATE FUNCTION fenced_workflow_usage_budget_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE workflow uuid; terminal boolean;
BEGIN
  SELECT workflow_id,phase IN ('SUCCEEDED','FAILED','CANCELLED') INTO workflow,terminal
    FROM tasks WHERE tenant_id=NEW.tenant_id AND id=NEW.task_id;
  IF workflow IS NOT NULL AND NOT terminal THEN
    UPDATE workflow_usage_ledgers SET reserved_tokens=reserved_tokens+NEW.reserved_tokens,
      reserved_cost_micro_usd=reserved_cost_micro_usd+NEW.reserved_cost_micro_usd,updated_at=now()
    WHERE tenant_id=NEW.tenant_id AND workflow_id=workflow;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_budget_insert AFTER INSERT ON task_budget_ledgers
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_budget_insert();

CREATE FUNCTION fenced_workflow_usage_settlement_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE workflow uuid; terminal boolean;
BEGIN
  SELECT workflow_id,phase IN ('SUCCEEDED','FAILED','CANCELLED') INTO workflow,terminal
    FROM tasks WHERE tenant_id=NEW.tenant_id AND id=NEW.task_id;
  IF workflow IS NOT NULL THEN
    UPDATE workflow_usage_ledgers SET settled_tokens=settled_tokens+NEW.tokens,
      settled_cost_micro_usd=settled_cost_micro_usd+NEW.cost_micro_usd,
      reserved_tokens=CASE WHEN terminal THEN reserved_tokens ELSE GREATEST(0,reserved_tokens-NEW.tokens) END,
      reserved_cost_micro_usd=CASE WHEN terminal THEN reserved_cost_micro_usd ELSE GREATEST(0,reserved_cost_micro_usd-NEW.cost_micro_usd) END,
      updated_at=now() WHERE tenant_id=NEW.tenant_id AND workflow_id=workflow;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER workflow_usage_settlement_insert AFTER INSERT ON task_budget_settlements
  FOR EACH ROW EXECUTE FUNCTION fenced_workflow_usage_settlement_insert();
