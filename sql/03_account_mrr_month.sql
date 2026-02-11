/*
03_account_mrr_month.sql

PURPOSE:
    - Create a month-end snapshot of each account's subscription status.
    - Produce output of one row per (account_id, month_start).
    - This is the base table for churn, retention, and revenue movement, and tier mix.

Key modeling decision (EOM snapshot)
    - A subscription is considered active for a month only if it is active *as of the
      final day of that month* (month_end).
    - If an account has overlapping subscriptions at month-end, select the subscription
      with the latest start_date; break ties by higher mrr_amount, then subscription_id.

Significance
    - Stakeholders and Finance typically interpret "last month" as a month-close state.
    - Using EOM avoids timing drift where mid-month cancellations still appear as "active"
      in that month under an "active-anytime" rule.

Dependencies
    - Requires: subscriptions (and a month generator).
    - Assumes: end_date NULL mean "still active".
    - Assumes: end_date is inclusive (active through end_date)

Output
    - v_account_mrr_month
*/
SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_account_mrr_month AS
WITH bounds AS (
    SELECT
        DATE_TRUNC('month', MIN(start_date))::date AS min_month,
        DATE_TRUNC('month', MAX(COALESCE(end_date, start_date)))::date AS max_month
    FROM subscriptions
),
months AS (
    SELECT
        GENERATE_SERIES(min_month, max_month, interval '1 month')::date AS month_start,
        (GENERATE_SERIES(min_month, max_month, interval '1 month')
            + interval '1 month' - interval '1 day')::date AS month_end
    FROM bounds
),
-- subs_at_eom: subscriptions active as-of the final day of the month (month_end).
subs_at_eom AS (
    SELECT
        m.month_start,
        m.month_end,
        s.account_id,
        s.subscription_id,
        s.plan_tier,
        s.seats,
        COALESCE(s.mrr_amount, 0)::numeric(12,2) AS mrr_amount,
        s.start_date,
        COALESCE(s.end_date, date '9999-12-31') AS end_date
    FROM months m
    JOIN subscriptions s
      ON s.start_date <= m.month_end
     AND COALESCE(s.end_date, date '9999-12-31') >= m.month_end
),
-- ranked: enforces "one subscription per account per month" at month-end.
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY month_start, account_id
            ORDER BY start_date DESC, mrr_amount DESC, subscription_id DESC
        ) AS rn
    FROM subs_at_eom
)
SELECT
    month_start,
    account_id,
    subscription_id,
    plan_tier,
    seats,
    mrr_amount
FROM ranked
WHERE rn = 1;

-- Optional: preview (uncomment)
SELECT *
FROM v_account_mrr_month
ORDER BY month_start, account_id
LIMIT 10;