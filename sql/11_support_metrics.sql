/*
11_support_metrics.sql

PURPOSE:
    -Create account-month support metrics that can be aligned to churn timing.

Significance
    - Explains churn risk by testing signals that appear before churn.
    - Support burden is a common leading indicator and is easy to quantify.

Dependencies
    - Requires: support_tickets
        Key columns used:
            - submitted_at
            - closed_at
            - resolution_time_hours
            - priority
            -escalation_flag

Output
    - v_support_account_month (one row per account per month)

Notes
    - tickets_closed = closed_at IS NOT NULL.
    - avg_resolution_hours uses resolution_time_hours.
    - high_priority_tickets counts priority in ('high', 'urgent')
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_support_account_month AS
WITH base AS (
    SELECT
        DATE_TRUNC('month', submitted_at::TIMESTAMP)::DATE AS month_start,
        account_id,
        ticket_id,
        submitted_at::TIMESTAMP AS submitted_at_ts,
        closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        escalation_flag
    FROM support_tickets
),
metrics as (
    SELECT
        month_start,
        account_id,
        COUNT(*) AS tickets_opened,
        COUNT(*) FILTER (
            WHERE closed_at IS NOT NULL
        ) AS tickets_closed,
        AVG(resolution_time_hours) FILTER (
            WHERE closed_at IS NOT NULL
        ) AS avg_resolution_hours,
        AVG(first_response_time_minutes) AS avg_first_response_minutes,
        AVG(satisfaction_score) FILTER (
            WHERE satisfaction_score IS NOT NULL
        ) AS avg_satisfaction_score,
        COUNT(*) FILTER (
            WHERE lower(trim(priority)) in ('high', 'urgent')
        ) AS high_priority_tickets,
        COUNT(*) FILTER (
            WHERE escalation_flag = true
        ) as escalated_tickets
    FROM base
    GROUP BY 1, 2
)
SELECT *
FROM metrics;

-- QC queries
-- 1) Grain check: should be zero
SELECT
    count(*) AS bad_rows
FROM (
    SELECT
        month_start,
        account_id,
        COUNT(*) AS n
    FROM v_support_account_month
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
) t;

-- 2) Date range check
SELECT
    MIN(month_start) AS min_month,
    MAX(month_start) AS max_month,
    COUNT(*) AS rows
FROM v_support_account_month;

-- 3) Basic distribution check
SELECT
    AVG(tickets_opened) AS avg_tickets_opened,
    MAX(tickets_opened) AS max_tickets_opened,
    AVG(high_priority_tickets) AS avg_high_priority,
    AVG(escalated_tickets) AS avg_escalated
FROM v_support_account_month;