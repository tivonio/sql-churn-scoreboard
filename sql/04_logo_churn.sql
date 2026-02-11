/*
04_logo_churn.sql

PURPOSE:
    - Calculate customer churn based on the number of customers lost (logo churn).

Definition (monthly)
    - A customer is "paying" if mrr_amount > 0.
    - churned_customer: prev_mrr > 0 AND current_mrr = 0
    - new_customer: prev_mrr = 0 AND current_mrr > 0

Dependencies
    - Requires: v_account_mrr_month (03_account_mrr_month.sql)
    - Uses: subscriptions for month bounds and account scope

Significance
    - Logo churn answers, "Are we losing customers?"
    - It does not capture revenue impact or engagement risk.

Output
    - v_logo_churn_month
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_logo_churn_month AS
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
    -- Only include accounts that appear in subscriptions (keeps the spine smaller).
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
base AS (
    SELECT
        s.month_start,
        s.account_id,
        COALESCE(am.mrr_amount, 0)::numeric(12,2) AS mrr_amount,
        LAG(COALESCE(am.mrr_amount, 0), 1, 0) OVER (
            PARTITION BY s.account_id
            ORDER BY s.month_start
        ) AS prev_mrr
    FROM account_month_spine s
    LEFT JOIN v_account_mrr_month am
        ON am.account_id = s.account_id
       AND am.month_start = s.month_start
)
SELECT
    month_start,
    COUNT(*) FILTER (WHERE prev_mrr > 0) AS customers_start,
    COUNT(*) FILTER (WHERE prev_mrr = 0 AND mrr_amount > 0) AS new_customers,
    COUNT(*) FILTER (WHERE prev_mrr > 0 AND mrr_amount = 0) AS churned_customers,
    COUNT(*) FILTER (WHERE mrr_amount > 0) AS customers_end,
    (COUNT(*) FILTER (WHERE prev_mrr > 0 AND mrr_amount = 0))::numeric
    / NULLIF((COUNT(*) FILTER (WHERE prev_mrr > 0))::numeric, 0) AS logo_churn_rate,
    -- Optional: helpful for stakeholder framing (gross churn can coexist with net growth)
    (COUNT(*) FILTER (WHERE prev_mrr = 0 AND mrr_amount > 0)
     - COUNT(*) FILTER (WHERE prev_mrr > 0 AND mrr_amount = 0)) AS net_customer_change,
    ((COUNT(*) FILTER (WHERE mrr_amount > 0)
      - COUNT(*) FILTER (WHERE prev_mrr > 0))::numeric
     / NULLIF((COUNT(*) FILTER (WHERE prev_mrr > 0))::numeric, 0)) AS net_growth_rate
FROM base
GROUP BY 1
ORDER BY month_start;

-- Optional: view first 10 rows (uncomment)
SELECT *
FROM v_logo_churn_month
LIMIT 50;