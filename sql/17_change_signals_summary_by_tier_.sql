/*
17_change_signals_summary_by_tier.sql

PURPOSE:
    - Add plan_tier at the anchor month to each account's change signals.
    - Summarize change signals by anchor_type and plan_tier

Significance
    - SQL 16 summarized churned vs retained at the blended level.
    - SQL 17 tests whether that blended result is hiding different churn
        stories across Basic, Pro, and Enterpise tiers.
    - This is the segmentation step that makes tier-level churn patterns visible.

Dependencies
    - Requires:
        - v_change_signals_3m_account (15_change_signals_3m.sql)
        - v_account_mrr_month (03_account_mrr_month.sql)

Output
    - v_change_signals_3m_summary_by_tier

Notes
    - plan_tier is taken from the month-end snapshot in v_account_mrr_month 
        for the account at anchor_month.
    - Absolute deltas are summarized for all eligible accounts.
    - Percent deltas are summarized only where prior baseline > 0.
    - This file is used to identify whether Basic, Pro, and Enterpise represent different churn archetypes.
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_change_signals_3m_summary_by_tier AS
WITH base AS (
    SELECT
        account_id,
        anchor_type,
        anchor_month,

        tickets_recent_3m,
        tickets_prior_3m,
        tickets_delta,
        tickets_delta_pct,

        high_priority_recent_3m,
        high_priority_prior_3m,
        high_priority_delta,
        high_priority_delta_pct,

        escalations_recent_3m,
        escalations_prior_3m,
        escalations_delta,
        escalations_delta_pct,

        usage_recent_3m,
        usage_prior_3m,
        usage_delta,
        usage_delta_pct,

        features_prior_3m,
        features_prior_3m,
        features_delta,

        share_active_recent_3m,
        share_active_prior_3m,
        share_active_delta
    FROM v_change_signals_3m_account
),
tier_by_anchor AS (
    SELECT
        m.account_id,
        m.month_start AS anchor_month,
        m.plan_tier
    FROM v_account_mrr_month m
),
joined AS (
    SELECT
        b.*,
        t.plan_tier
    FROM base b
    LEFT JOIN tier_by_anchor t
        ON t.account_id = b.account_id
        AND t.anchor_month = b.anchor_month
),
coverage AS (
    SELECT
        anchor_type,
        plan_tier,

        COUNT(*) AS accounts,

        COUNT(*) FILTER (
            WHERE usage_recent_3m IS NOT NULL
                AND usage_prior_3m IS NOT NULL
        ) AS accounts_with_usage_both_windows,

        COUNT(*) FILTER (
            WHERE tickets_recent_3m IS NOT NULL
                AND tickets_prior_3m IS NOT NULL
        ) AS accounts_with_tickets_both_windows,

        COUNT(*) FILTER (
            WHERE usage_prior_3m > 0
        ) AS accounts_with_usage_prior_gt_0,

        COUNT(*) FILTER (
            WHERE tickets_prior_3m > 0
        ) AS accounts_with_tickets_prior_gt_0

    FROM joined
    GROUP BY
        anchor_type,
        plan_tier
),
signals AS (
    SELECT
        anchor_type,
        plan_tier,

        AVG(usage_delta) AS avg_usage_delta,
        AVG(usage_delta_pct) FILTER (
            WHERE usage_prior_3m > 0
        ) AS avg_usage_delta_pct,
        AVG(
            CASE
                WHEN usage_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_usage_declining,

        AVG(features_delta) AS avg_features_delta,
        AVG(
            CASE
                WHEN features_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_features_declining,

        AVG(share_active_delta) AS avg_share_active_delta,
        AVG(
            CASE
                WHEN share_active_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_active_declining,

        AVG(tickets_delta) AS avg_tickets_delta,
        AVG(tickets_delta_pct) FILTER (
            WHERE tickets_prior_3m > 0
        ) AS avg_tickets_delta_pct,
        AVG(
            CASE
                WHEN tickets_delta > 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_tickets_increasing,

        AVG(high_priority_delta) AS avg_high_priority_delta,
        AVG(
            CASE
                WHEN high_priority_delta > 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_high_priority_increasing,

        AVG(escalations_delta) AS avg_escalations_delta

    FROM joined
    GROUP BY
        anchor_type,
        plan_tier
)
SELECT
    c.anchor_type,
    c.plan_tier,

    c.accounts,
    c.accounts_with_usage_both_windows,
    c.accounts_with_tickets_both_windows,
    c.accounts_with_usage_prior_gt_0,
    c.accounts_with_tickets_prior_gt_0,

    s.avg_usage_delta,
    s.avg_usage_delta_pct,
    s.share_usage_declining,

    s.avg_features_delta,
    s.share_features_declining,

    s.avg_share_active_delta,
    s.share_active_declining,

    s.avg_tickets_delta,
    s.avg_tickets_delta_pct,
    s.share_tickets_increasing,

    s.avg_high_priority_delta,
    s.share_high_priority_increasing,

    s.avg_escalations_delta

FROM coverage c
JOIN signals s
    ON s.anchor_type = c.anchor_type
    AND (
        (s.plan_tier = c.plan_tier)
        OR
        (s.plan_tier IS NULL AND c.plan_tier IS NULL)
    )
ORDER BY
    c.anchor_type,
    c.plan_tier;

-- Check:
-- Rows: churned and retained across plan tiers
-- Coverage columns: confirm that "both_windows" and "prior_gt_0" are not zero
-- Signal columns: look for tiers where churned differs meaningfully from retained
SELECT *
FROM ravenstack.v_change_signals_3m_summary_by_tier;

-- Check:
-- plan_tier should be mostly non-null (Basic / Pro / Enterprise)
SELECT
    a.anchor_type,
    m.plan_tier,
    COUNT(*) AS accounts
FROM v_change_signals_3m_account a
LEFT JOIN v_account_mrr_month m
    ON m.account_id = a.account_id
    AND m.month_start = a.anchor_month
GROUP BY
    a.anchor_type,
    m.plan_tier
ORDER BY
    a.anchor_type,
    m.plan_tier;