CREATE TABLE IF NOT EXISTS events (
    source TEXT NOT NULL, event_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    occurred_at TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency IN ('EUR', 'USD', 'GBP')),
    amount_minor INTEGER NOT NULL CHECK (amount_minor >= 0),
    PRIMARY KEY (source, event_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS rejects (
    source TEXT NOT NULL, record_number INTEGER NOT NULL, reason TEXT NOT NULL,
    PRIMARY KEY (source, record_number)
) WITHOUT ROWID;
CREATE VIEW IF NOT EXISTS daily_totals AS
    SELECT substr(occurred_at, 1, 10) AS utc_day, currency,
           count(*) AS events, sum(amount_minor) AS amount_minor
    FROM events GROUP BY utc_day, currency;
