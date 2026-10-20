-- A rollout replaces a serving instance only after its replacement has proven
-- itself. The proof is the moment the instance was first observed healthy in
-- its current generation; it has to survive a controller restart, so it is
-- stored with the instance instead of being remembered in memory.
ALTER TABLE agent_service_instances
    ADD COLUMN ready_at TIMESTAMPTZ;

-- A legacy instance that was already running has no recorded proof. It must
-- prove itself again before a rollout relies on it.
UPDATE agent_service_instances
SET ready_at = NULL
WHERE phase NOT IN ('Running');
