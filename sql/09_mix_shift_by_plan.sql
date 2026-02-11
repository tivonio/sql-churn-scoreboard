/*
09_mix_shift_by_plan.sql

PURPOSE:
    - Break down customers and MRR by plan_tier over time.
    - Show how a shift toward higher-value tiers can boost
        revenue even as the total customer count declines.
Significance
    - Total MRR can rise even if a segment shrinks, if customers migrate
        to higher tiers or the business acquires more high_value customers.

Dependencies
    - Requires: v_account_mrr_month

Output
    - v_mix_shift_by_plan_month
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_mix_shift_by_plan_month AS
SELECT
    month_start,
    plan_tier,
    COUNT(*) FILTER (
        WHERE mrr_amount > 0
    ) AS paying_accounts,
    SUM(mrr_amount) FILTER (
        WHERE mrr_amount > 0
    ) AS total_mrr,
    AVG(mrr_amount) FILTER (
        WHERE mrr_amount > 0
    ) AS avg_mrr_per_account
FROM v_account_mrr_month
GROUP BY 1, 2
ORDER BY month_start, plan_tier;

SELECT *
FROM v_mix_shift_by_plan_month;