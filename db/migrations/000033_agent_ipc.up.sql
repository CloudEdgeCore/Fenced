-- P0-IPC-01: durable agent-to-agent mailboxes.
--
-- A mailbox is the durable record a receiving agent drains by cursor. It is
-- deliberately not a second outbox: outbox_events (000001) is the *dispatch*
-- record that the projector publishes to NATS and stops considering once
-- published. A message must survive both the sending attempt and the receiving
-- attempt, so it cannot live in the dispatch record.
--
-- Ordering is one global identity column rather than a per-mailbox counter. A
-- per-mailbox counter would make every concurrent send to the same recipient
-- serialize behind a single row lock, and the mailbox path is the hot path. A
-- global sequence is still strictly increasing inside one mailbox, so
-- "sequence > cursor" remains a correct cursor and the gaps between mailboxes
-- are harmless.
--
-- Acknowledgement reuses inbox_receipts (000001) rather than adding a table.
-- The receipt key is (tenant_id, consumer_name, event_id), so scoping
-- consumer_name to the receiving run makes redelivery idempotent at the
-- receiving side. That table has existed since the foundation migration with no
-- production reader and no production writer; IPC is its first real user.
--
-- Delivery is at-least-once, not exactly-once. The receipt suppresses a
-- duplicate *observation* inside Fenced; it cannot suppress a duplicate
-- external side effect the receiving agent performs after it has read a
-- message, because that call leaves the fencing and audit boundary.
--
-- The outbox subject grammar constrains PR-2: eventSubject() renders
-- "fenced.events.<aggregate_type lowercased>.<event_type lowercased>" and both
-- tokens must match ^[A-Za-z][A-Za-z0-9_]{0,127}$, so the aggregate type for a
-- message event is pinned to "ipc" here and cannot carry '.' or '-'.

CREATE TABLE ipc_messages (
    sequence               bigint GENERATED ALWAYS AS IDENTITY,
    id                     uuid NOT NULL,
    tenant_id              text NOT NULL CHECK (tenant_id <> ''),
    from_agent_version_ref text NOT NULL CHECK (from_agent_version_ref <> ''),
    to_address             text NOT NULL CHECK (to_address <> ''),
    kind                   text NOT NULL CHECK (kind <> ''),
    payload                jsonb NOT NULL,
    attachments            jsonb NOT NULL DEFAULT '[]'::jsonb,
    correlation_id         text NOT NULL DEFAULT '',
    reply_to_message_id    uuid,
    idempotency_key        text NOT NULL CHECK (idempotency_key <> ''),
    trace_id               text NOT NULL DEFAULT '',
    sent_at                timestamptz NOT NULL,
    deadline               timestamptz,
    PRIMARY KEY (tenant_id, sequence)
);

-- The public identity of a message is (tenant_id, id). The caller supplies the
-- id, so a send retried after a connection failure resolves to the message the
-- first attempt created instead of creating a second one.
CREATE UNIQUE INDEX ipc_messages_identity_idx
    ON ipc_messages (tenant_id, id);

-- A sender replaying one logical send reuses its idempotency key. Scoping the
-- key to the sender lets two different agents pick the same key without
-- colliding, and a replay from a different sender is a conflict rather than a
-- silent deduplication. This mirrors tasks (tenant_id, namespace,
-- idempotency_key).
CREATE UNIQUE INDEX ipc_messages_idempotency_idx
    ON ipc_messages (tenant_id, from_agent_version_ref, idempotency_key);

-- The mailbox drain and the receipt LEFT JOIN both walk this order; a partial
-- index cannot serve them because neither query fixes a kind.
CREATE INDEX ipc_messages_mailbox_idx
    ON ipc_messages (tenant_id, to_address, sequence);

-- Only a minority of messages carry a deadline; a partial index keeps an
-- expiry sweep proportional to those instead of to the whole mailbox table.
CREATE INDEX ipc_messages_deadline_idx
    ON ipc_messages (tenant_id, deadline)
    WHERE deadline IS NOT NULL;
