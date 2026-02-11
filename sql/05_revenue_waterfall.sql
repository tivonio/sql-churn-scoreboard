/*
05_revenue_waterfall.sql

PURPOSE:
    - Calculate the drivers of revenue change (MRR waterfall) along with key retention metrics.

Components (monthly)
    - starting_mrr: sum of prev_mrr for paying customers (prev_mrr > 0)
    - new_mrr: MRR from accounts that were not paying last month and are paying now
    - expansion_mrr: increases in MRR among continuing customers
    - contraction_mrr: decreases in MRR among continuing customers
    - churned_mrr: lost MRR from customers that stopped paying (prev_mrr > 0 AND mrr_amount = 0)
    - ending_mrr: sum of current MRR for paying customers (mrr_amount > 0)

Metrics
    - gross_revenue_churn_rate = (contraction_mrr + churned_mrr) / starting_mrr
    - net_revenue_retention (NRR) = (starting + expansion - contraction -churn) / starting

Dependencies

    - Requires: v_account_mrr_month (03_account_mrr_month.sql)

Output
    - v_revenue_waterfall_month
*/


SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_revenue_waterfall_month AS
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
),
movements AS (
    SELECT
        month_start,
        SUM(prev_mrr)
            FILTER (WHERE prev_mrr > 0
            ) AS starting_mrr,
        SUM(mrr_amount)
            FILTER (WHERE prev_mrr = 0
            AND mrr_amount > 0
            ) AS new_mrr,
        SUM(mrr_amount - prev_mrr)
            FILTER (WHERE prev_mrr > 0
            AND mrr_amount > prev_mrr
            ) AS expansion_mrr,
        SUM(prev_mrr - mrr_amount)
            FILTER (WHERE prev_mrr > 0
            AND mrr_amount > 0
            AND mrr_amount < prev_mrr
            ) AS contraction_mrr,
        SUM(prev_mrr)
            FILTER (WHERE prev_mrr > 0
            AND mrr_amount = 0
            ) AS churned_mrr,
        SUM(mrr_amount)
            FILTER (WHERE mrr_amount > 0
            ) AS ending_mrr
            -- Check: ending_mrr should equal
            --      starting_mrr + new_mrr + expansion_mrr -
            --      contraction_mrr - churned_mrr
    FROM base
    GROUP BY 1
)
SELECT
    month_start,
    COALESCE(starting_mrr, 0)::numeric(12,2) AS starting_mrr,
    COALESCE(new_mrr, 0)::numeric(12,2) AS new_mrr,
    COALESCE(expansion_mrr, 0)::numeric(12,2) AS expansion_mrr,
    COALESCE(contraction_mrr, 0)::numeric(12,2) AS contraction_mrr,
    COALESCE(churned_mrr, 0)::numeric(12,2) AS churned_mrr,
    COALESCE(ending_mrr, 0)::numeric(12,2) AS ending_mrr,

    (COALESCE(contraction_mrr, 0) + COALESCE(churned_mrr, 0))::numeric
    / NULLIF(COALESCE(starting_mrr, 0)::numeric, 0) AS gross_revenue_churn_rate,

    (COALESCE(starting_mrr, 0)
    + COALESCE(expansion_mrr, 0)
    - COALESCE(contraction_mrr, 0)
    - COALESCE(churned_mrr, 0))::numeric
    / NULLIF(COALESCE(starting_mrr, 0)::numeric, 0) AS net_revenue_retention
FROM movements
ORDER BY month_start;

-- Optional: view first 10 rows (uncomment)
SELECT *
FROM v_revenue_waterfall_month
LIMIT 50;