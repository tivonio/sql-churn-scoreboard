/*
16_change_signals_summary.sql

PURPOSE:
    - Summarize change signals by anchor_type.
    - Report coverage so NULL pct deltas are explainable (missing windows, zero baselines).

Significance
    - SQL 15 created account-level change signals.
    - SQL 16 summarizes those account-level outputs.
    - This is the first compact comparison of churned vs retained accounts using both
        change metrics and coverage counts.

Dependencies
    - Requires: v_change_signals_3m_account (15_change_signals_3m.sql)

Output
    - v_change_signals_3m_summary

Notes
    - Absolute deltas are summarized broadly because they do not require
        a non-zero prior baseline.
    - Percent deltas are summarized only where prior_3m > 0.
    - Coverage columns show how many accounts actually support each comparison.
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_change_signals_3m_summary AS
WITH base AS (
    SELECT *
    FROM v_change_signals_3m_account
),
coverage AS (
    SELECT
        anchor_type,
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

    FROM base
    GROUP BY anchor_type
),
signals AS (
    SELECT
        anchor_type,

        -- Absolute deltas (broadest coverage)
        AVG(usage_delta) AS avg_usage_delta,
        AVG(features_delta) AS avg_features_delta,
        AVG(share_active_delta) AS avg_share_active_delta,

        AVG(tickets_delta) AS avg_tickets_delta,
        AVG(high_priority_delta) AS avg_high_priority_delta,
        AVG(escalations_delta) AS avg_escalations_delta,

        -- Percent deltas only where prior baseline exist (> 0)
        AVG(usage_delta_pct) FILTER (
            WHERE usage_prior_3m > 0
        ) AS avg_usage_delta_pct,
        AVG(tickets_delta_pct) FILTER (
            WHERE tickets_prior_3m > 0
        ) AS avg_tickets_delta_pct,

        -- Share of accounts showing decline/increase signals
        AVG(
            CASE
                WHEN usage_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_usage_declining,
        AVG(
            CASE
                WHEN features_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_features_declining,
        AVG(
            CASE
                WHEN share_active_delta < 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_active_declining,

        AVG(
            CASE
                WHEN tickets_delta > 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_tickets_increasing,
        AVG(
            CASE
                WHEN high_priority_delta > 0 THEN 1
                ELSE 0
                END::numeric
        ) AS share_high_priority_increasing

    FROM base
    GROUP BY anchor_type
)
SELECT
    c.anchor_type,

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
ORDER BY c.anchor_type;

SELECT *
FROM v_change_signals_3m_summary;