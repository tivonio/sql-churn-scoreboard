/*
10_extracts_for_python.sql

PURPOSE:
    - Plot-ready extracts for Python.
    - SQL remains the baseline. Python only visualizes outputs.
*/

-- Visual 1: Socreboard trends
SELECT
    month_start,
    logo_churn_rate,
    net_revenue_retention,
    activity_churn_rate
FROM ravenstack.v_churn_scoreboard_month
ORDER BY month_start;

-- Visual 2: Revenue waterfall input (filter one month in Python)
SELECT
    month_start,
    starting_mrr,
    new_mrr,
    expansion_mrr,
    contraction_mrr,
    churned_mrr,
    ending_mrr
FROM ravenstack.v_revenue_waterfall_month
ORDER BY month_start;

-- Visual 3: Mix shift by plan tier
SELECT
    month_start,
    plan_tier,
    paying_accounts,
    total_mrr,
    avg_mrr_per_account
FROM ravenstack.v_mix_shift_by_plan_month
ORDER BY month_start, plan_tier;