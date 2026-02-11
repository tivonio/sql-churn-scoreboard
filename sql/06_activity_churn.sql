/*
06_activity_churn.sql

PURPOSE:
    - Identify which paying customers are at risk of disengaging
      based on trends and patterns in their product usage.

Definition
    - active: total_usage_count > 0 in a month
    - inactive_but_paying: mrr_amount > 0 AND total_usage_count = 0

Activity churn (monthly)
    - activity_churned_account:
      was paying AND was active last month,
      is paying AND is inactive this month

Significance
    - Revenue can look stable while usage declines.
    - Activity churn is a leading indicator of future churn.

Dependencies
    - Requires: feature_usage_raw, subscriptions, and v_account_mrr_month (03_account_mrr_month.sql).

Output
    - v_activity_churn_month
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_activity_churn_month AS
-- feature_usage_raw is a landing table and may contain duplicates by usage_id.
-- usage is aggregated by (account_id, month) so duplicates only matter if they inflate usage_count.
WITH bounds AS (
    SELECT
        date_trunc('month', MIN(start_date))::date AS min_month,
        date_trunc('month', MAX(COALESCE(end_date, start_date)))::date AS max_month
    FROM subscriptions
),
months AS (
    SELECT
        generate_series(min_month, max_month, interval '1 month')::date AS month_start
    FROM bounds
),
accounts_in_scope AS (
    SELECT DISTINCT account_id
    FROM subscriptions
),
account_month_spine AS (
    SELECT
        a.account_id,
        m.month_start
    FROM accounts_in_scope a
    CROSS JOIN months m
),
usage_by_account_month AS (
    SELECT
        s.account_id,
        date_trunc('month', u.usage_date)::date AS month_start,
        SUM(COALESCE(u.usage_count, 0)) AS total_usage_count
    FROM feature_usage_raw u
    JOIN subscriptions s
        ON s.subscription_id = u.subscription_id
    GROUP BY 1, 2
),
joined AS (
    SELECT
        sp.month_start,
        sp.account_id,
        COALESCE(mrr.mrr_amount, 0)::numeric(12,2) AS mrr_amount,
        COALESCE(u.total_usage_count, 0) AS total_usage_count,
        CASE
            WHEN COALESCE(mrr.mrr_amount, 0) > 0 THEN 1
            ELSE 0
        END AS is_paying,
        CASE
            WHEN COALESCE(u.total_usage_count, 0) > 0 THEN 1
            ELSE 0
        END AS is_active
    FROM account_month_spine sp
    LEFT JOIN v_account_mrr_month mrr
        ON mrr.account_id = sp.account_id
       AND mrr.month_start = sp.month_start
    LEFT JOIN usage_by_account_month u
        ON u.account_id = sp.account_id
       AND u.month_start = sp.month_start
),
with_prev AS (
    SELECT
        *,
        LAG(is_active, 1, 0) OVER (
            PARTITION BY account_id
            ORDER BY month_start
        ) AS prev_is_active,
        LAG(is_paying, 1, 0) OVER (
            PARTITION BY account_id
            ORDER BY month_start
        ) AS prev_is_paying
    FROM joined
)
SELECT
    month_start,
    COUNT(*) FILTER (
        WHERE is_paying = 1
    ) AS paying_accounts,
    COUNT(*) FILTER (
        WHERE is_paying = 1
        AND is_active = 1
    ) AS active_paying_accounts,
    COUNT(*) FILTER (
        WHERE is_paying = 1
        AND is_active = 0
    ) AS inactive_but_paying_accounts,
    COUNT(*) FILTER (
        WHERE prev_is_paying = 1
        AND prev_is_active = 1
        AND is_paying = 1
        AND is_active = 0
    ) AS activity_churned_accounts,
    (COUNT(*) FILTER (
        WHERE prev_is_paying = 1
        AND prev_is_active = 1
        AND is_paying = 1
        AND is_active = 0
    ))::numeric
    / NULLIF((COUNT(*) FILTER (
        WHERE prev_is_paying = 1
        AND prev_is_active = 1
    ))::numeric, 0
    ) AS activity_churn_rate
FROM with_prev
GROUP BY 1
ORDER BY month_start;

-- Optional: view first 10 rows (uncomment)
SELECT *
FROM v_activity_churn_month
LIMIT 50;