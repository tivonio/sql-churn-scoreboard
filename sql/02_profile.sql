/*
02_profile.sql

PURPOSE:
    - Run quick checks after loading CSVs.
    - Confirm load worked.
    - Validate key assumptions.
    - Catch common data issues such as NULLS.
    - Detect join risks (orphan rows).

Dependencies
    - Requires: 00_create_tables.sql and 01_load_data.sql
*/

SET search_path = ravenstack;

-- Row counts: confirms the COPY loaded data into each table.
SELECT
    'accounts' AS table_name,
    COUNT(*) AS row_count
FROM accounts
UNION ALL
SELECT
    'subscriptions',
    COUNT(*)
FROM subscriptions
UNION ALL
SELECT
    'feature_usage_raw',
    COUNT(*)
FROM feature_usage_raw
UNION ALL
SELECT
    'support_tickets',
    COUNT(*)
FROM support_tickets
UNION ALL
SELECT
    'churn_events',
    COUNT(*)
FROM churn_events;

-- Key uniqueness checks: confirms that unique identifiers (keys) are in fact unique.
SELECT
    'accounts.account_id duplicates' AS check_name,
    COUNT(*) AS dupes
FROM (
    SELECT account_id
    FROM accounts
    GROUP BY 1
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
    'subscriptions.subscription_id duplicates' AS check_name,
    COUNT(*) AS dupes
FROM (
    SELECT subscription_id
    FROM subscriptions
    GROUP BY 1
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
    'feature_usage_raw.row_id duplicates',
    COUNT(*) AS dupes
FROM (
    SELECT row_id
    FROM feature_usage_raw
    GROUP BY 1
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
    'support_tickets.ticket_id duplicates',
    COUNT(*) AS dupes
FROM (
    SELECT ticket_id
    FROM support_tickets
    GROUP BY 1
    HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
    'churn_events.churn_event_id duplicates',
    COUNT(*) AS dupes
FROM (
    SELECT churn_event_id
    FROM churn_events
    GROUP BY 1
    HAVING COUNT(*) > 1
) d;

-- NULL checks: checks for missing critical fields that will break time logic and joins.
-- Example: subscriptions.start_date should not be null.
SELECT
    'subscriptions' AS table_name,
    SUM(CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) AS null_start_date,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS null_account_id
FROM subscriptions;

SELECT
    'feature_usage_raw' AS table_name,
    SUM(CASE WHEN usage_date IS NULL THEN 1 ELSE 0 END) AS null_usage_date,
    SUM(CASE WHEN subscription_id IS NULL THEN 1 ELSE 0 END) AS null_subscription_id
FROM feature_usage_raw;

SELECT
    'churn_events' AS table_name,
    SUM(CASE WHEN churn_date IS NULL THEN 1 ELSE 0 END) AS null_churn_date,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS null_account_id
FROM churn_events;

SELECT
    'support_tickets' AS table_name,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS null_account_id
FROM support_tickets;

-- Date logic checks: look for impossible timelines such as end date occurring before start date.
SELECT
    COUNT(*) AS bad_end_before_start
FROM subscriptions
WHERE end_date IS NOT NULL
    AND end_date < start_date;

-- Check min/max dates on subscriptions and feature_usage_raw
SELECT
  MIN(start_date) AS min_start_date,
  MAX(COALESCE(end_date, start_date)) AS max_end_date
FROM subscriptions;

SELECT
  MIN(usage_date) AS min_usage_date,
  MAX(usage_date) AS max_usage_date
FROM feature_usage_raw;

-- FK orphan checks: look for rows that fail expected relationships.
SELECT
    COUNT(*) AS subscriptions_missing_account
FROM subscriptions s
LEFT JOIN accounts a ON a.account_id = s.account_id
WHERE a.account_id IS NULL;

SELECT
    COUNT(*) AS usage_missing_subscription
FROM feature_usage_raw u
LEFT JOIN subscriptions s ON s.subscription_id = u.subscription_id
WHERE s.subscription_id IS NULL;

SELECT
    COUNT(*) AS churn_missing_account
FROM churn_events c
LEFT JOIN accounts a ON a.account_id = c.account_id
WHERE a.account_id IS NULL;

SELECT
    COUNT(*) AS support_missing_account
FROM support_tickets s
LEFT JOIN accounts a ON a.account_id = s.account_id
WHERE a.account_id IS NULL;

-- Usage duplicates: usage_id is not guaranteed to be unique in the raw file.
-- Usage is stored in feature_usage_raw as a landing table and aggregate for analysis.
-- This query tells how much duplication exists.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT usage_id) AS distinct_usage_id,
    (COUNT(*) - COUNT(DISTINCT usage_id)) AS usage_id_duplicate_rows
FROM feature_usage_raw;
