-- Budget checks and status refreshes aggregate one day and mode in one space.
-- Historical ledgers must not turn a missing current-day guard into a full scan.
DEFINE INDEX IF NOT EXISTS cost_ledger_daily_space_date_mode ON TABLE cost_ledger_daily FIELDS space_id, date, mode;
