/*
18_reason_code_by-tier.sql

PURPOSE:
    - Attach one churn reason to each churned account.
    - Join plan_tier at the churn month.
    - Summarize churn reasons by plan tier.

Significance
    - SQL 17 showed that Basic, Pro, and Enterprise do not behave the same way.
    - SQL 18 adds recorded churn reason to test whether different tiers churn
        for different stated reasons.
    - This helps explain why the support/usage signals are clearer in some tiers
        than in others.

Dependencies
    - Requires:
        - v_account_month_cohorts (13_prechurn_cohorts.sql)
        - churn_events (00_create_tables.sql, 01_load_data.sql)
        - v_account_mrr_month (03_account_mrr_month.sql)

Output
    - v_reason_code_by_tier

Notes
    - The churn anchor is the first logo churn month from v_account_month_cohorts.
    - The reason anchor is the first non-reactivation churn event per account
        from churn_events.
    - share_within_tier is the share of churned accounts in that tier
        assigned to the given reason_code.
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_reason_code_by_tier AS
WITH churned_accounts AS (
    SELECT
        account_id,
        churn_month
    FROM v_account_month_cohorts
    WHERE cohort_label = 'churn_month'
),
first_reason AS (
    SELECT
        account_id,
        reason_code,
        churn_date,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY churn_date
        ) AS rn
    FROM churn_events
    WHERE COALESCE(is_reactivation, false) = false
),
reason_one_per_account AS (
    SELECT
        account_id,
        reason_code,
        churn_date
    FROM first_reason
    WHERE rn = 1
),
tier_at_churn AS (
    SELECT
        m.account_id,
        m.month_start AS churn_month,
        m.plan_tier
    FROM v_account_mrr_month m
),
joined AS (
    SELECT
        c.account_id,
        c.churn_month,
        t.plan_tier,
        r.reason_code
    FROM churned_accounts c
    LEFT JOIN tier_at_churn t
        ON t.account_id = c.account_id
        AND t.churn_month = c.churn_month
    LEFT JOIN reason_one_per_account r
        ON r.account_id = c.account_id
),
counts AS (
    SELECT
        plan_tier,
        COALESCE(reason_code, 'unknown') AS reason_code,
        COUNT(DISTINCT account_id) AS churned_accounts
    FROM joined
    GROUP BY
        plan_tier,
        COALESCE(reason_code, 'unknown')
),
tier_totals AS (
    SELECT
        plan_tier,
        SUM(churned_accounts) AS tier_churned_accounts
    FROM counts
    GROUP BY plan_tier
)
SELECT
    c.plan_tier,
    c.reason_code,
    c.churned_accounts,
    t.tier_churned_accounts,
    c.churned_accounts::numeric / NULLIF(t.tier_churned_accounts, 0) AS share_within_tier
FROM counts c
JOIN tier_totals t
    ON t.plan_tier = c.plan_tier
ORDER BY
    c.plan_tier,
    c.churned_accounts DESC,
    c.reason_code;

-- 1) Check: reason_code distribution by tier
-- plan_tier should be populated (Basic / Pro / Enterprise)
-- reason_code should have plausible values
-- share_within_tier should sum to about 1.0 with each tier
SELECT *
FROM v_reason_code_by_tier;

-- 2) Check: share totals by tier
-- share_within_tier should add up to 1.0 for each plan_tier
-- share_sum should be 1.0 or close
SELECT
    plan_tier,
    SUM(share_within_tier) AS share_sum
FROM v_reason_code_by_tier
GROUP BY plan_tier
ORDER BY plan_tier;

-- 3) Check: missing tier or reason audit
-- If plan_tier or reason_code is missing, interpretation gets weaker.
-- This tells whether nulls are a meaningful issue.
-- Ideally missing_plan_tier is low or zero.
-- missing_reason_code may occur, but should be understood.
WITH churned_accounts AS (
    SELECT
        account_id,
        churn_month
    FROM ravenstack.v_account_month_cohorts
    WHERE cohort_label = 'churn_month'
),
first_reason AS (
    SELECT
        account_id,
        reason_code,
        churn_date,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY churn_date
        ) AS rn
    FROM churn_events
    WHERE COALESCE(is_reactivation, false) = false
),
reason_one_per_account AS (
    SELECT
        account_id,
        reason_code
    FROM first_reason
    WHERE rn = 1
),
tier_at_churn AS (
    SELECT
        m.account_id,
        m.month_start AS churn_month,
        m.plan_tier
    FROM ravenstack.v_account_mrr_month m
)
SELECT
    COUNT(*) AS churned_accounts,
    COUNT(*) FILTER (
        WHERE t.plan_tier IS NULL
    ) AS missing_plan_tier,
    COUNT(*) FILTER (
        WHERE r.reason_code IS NULL
    ) AS missing_reason_code
FROM churned_accounts c
LEFT JOIN tier_at_churn t
    ON t.account_id = c.account_id
    AND t.churn_month = c.churn_month
LEFT JOIN reason_one_per_account r
    ON r.account_id = c.account_id;