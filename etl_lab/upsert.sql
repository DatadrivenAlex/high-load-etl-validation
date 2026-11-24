INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (source, event_id) DO UPDATE SET
    revision = excluded.revision,
    occurred_at = excluded.occurred_at,
    currency = excluded.currency,
    amount_minor = excluded.amount_minor
WHERE excluded.revision > events.revision;
