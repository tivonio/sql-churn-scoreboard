/*
08_checks.sql

PURPOSE:
    - Validate that metric calculations align.
    - Spot join errors and duplicates.

What this file checks
    1) MRR waterfall reconciliations:
        starting + new + expansion - contraction - churned = ending
    2) Customer count reconciliation:
        customers_end = customers_start + new_customers - churned_customers

How to use
    - Run after the churn views are created.
    - Any non-zero diffs should be investigated before publishing results.

Dependencies
    - Requires: v_revenue_waterfall_month and v_logo_churn_month
*/

SET search_path = ravenstack;

-- MRR reconciliation:ending should starting + new + expansion - contraction - churned
SELECT
    month_start,
    (starting_mrr + new_mrr + expansion_mrr - contraction_mrr - churned_mrr) AS recomputed_ending_mrr,
    ending_mrr,
    (starting_mrr + new_mrr + expansion_mrr - contraction_mrr - churned_mrr) - ending_mrr AS diff
FROM v_revenue_waterfall_month
ORDER BY month_start;

-- Customer reconciliation: end should equal start + new - churned
SELECT
    month_start,
    (customers_start + new_customers - churned_customers) AS recomputed_customers_end,
    customers_end,
    (customers_start + new_customers - churned_customers) - customers_end AS diff
FROM v_logo_churn_month
ORDER BY month_start;