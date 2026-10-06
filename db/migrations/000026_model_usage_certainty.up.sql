-- A failed or interrupted provider request may have incurred external usage
-- even when no final usage frame reached Fenced. Never equate that unknown
-- outcome with known zero usage.
ALTER TABLE model_calls
    ADD COLUMN usage_certainty text NOT NULL DEFAULT 'UNKNOWN_USAGE'
    CHECK (usage_certainty IN ('KNOWN_ZERO_USAGE', 'KNOWN_USAGE', 'UNKNOWN_USAGE'));

UPDATE model_calls
SET usage_certainty = CASE
    WHEN status IN ('COMPLETED', 'STOPPED') AND input_tokens + output_tokens > 0 THEN 'KNOWN_USAGE'
    WHEN status IN ('COMPLETED', 'STOPPED') THEN 'KNOWN_ZERO_USAGE'
    ELSE 'UNKNOWN_USAGE'
END;

CREATE INDEX model_calls_unknown_usage_idx
    ON model_calls (updated_at, tenant_id, id)
    WHERE usage_certainty = 'UNKNOWN_USAGE';
