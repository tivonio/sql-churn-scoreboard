/*
15_change_signals_3m.sql

PURPOSE:
    - Build account-level change signals for support and usage using:
        recent_3m vs prior_3m
    - Use a consistent month anchor:
        churned accounts: first churn month (logo churn definition)
        retained accounts: last observed month

Significance
    - SQL `14 compared average levels for pre-churn vs retained.
    - SQL 15 shifts the analysis from levels to within-account change.
    - This is the first file that tests whether support or usage changes
        appear before churn relative to each account's own earlier baseline.

Dependencies
    - Requires:
        - v_account_mrr_month (03_account_mrr_month.sql)
        - v_support_account_month (11_support_metrics.sql)
        - v_usage_account_month (12_usage_metrics.sql)

Output
    - v_change_signals_3m_account

Notes
    - months_to_anchor = anchor_month - month_start (in months)
    - recent_3m = months_to_anchor between 1 and 3
    - prior_3m = months_to_anchor between 4 and 6
    - Absolute deltas work for all accounts with both windows
    - Percent deltas require a non-zero prior baseline
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_change_signals_3m_account AS
WITH base AS (
    SELECT
        month_start,
        account_id,
        mrr_amount,
        LAG(mrr_amount, 1, 0) OVER (
            PARTITION BY account_id
            ORDER BY month_start
        ) AS prev_mrr
    FROM v_account_mrr_month
),
first_churn AS (
    SELECT
        account_id,
        MIN(month_start) AS churn_month
    FROM base
    WHERE prev_mrr > 0
        AND mrr_amount = 0
    GROUP BY account_id
),
last_seen AS (
    SELECT
        account_id,
        MAX(month_start) AS last_month
    FROM v_account_mrr_month
    GROUP BY account_id
),
anchors AS (
    SELECT
        l.account_id,
        f.churn_month,
        l.last_month,
        COALESCE(f.churn_month, l.last_month) AS anchor_month,
        CASE
            WHEN f.churn_month IS NULL THEN 'retained'
            ELSE 'churned'
        END AS anchor_type
    FROM last_seen l
    LEFT JOIN first_churn f
        ON f.account_id = l.account_id
),
window_months AS (
    SELECT
        m.month_start,
        m.account_id,
        a.anchor_type,
        a.anchor_month,

        (
            (date_part('year', a.anchor_month)
            - date_part('year', m.month_start)) * 12
            + (date_part('month', anchor_month)
            - date_part('month', m.month_start))
        )::int AS months_to_anchor

    FROM v_account_mrr_month m
    JOIN anchors a
        ON a.account_id = m.account_id
    WHERE m.month_start <= a.anchor_month
),
window_labeled AS (
    SELECT
        month_start,
        account_id,
        anchor_type,
        anchor_month,
        months_to_anchor,
        CASE
            WHEN months_to_anchor BETWEEN 1 AND 3 THEN 'recent_3m'
            WHEN months_to_anchor BETWEEN 4 AND 6 THEN 'prior_3m'
            ELSE NULL
        END AS window_label
    FROM window_months
    WHERE months_to_anchor BETWEEN 1 AND 6
),
joined AS (
    SELECT
        w.account_id,
        w.anchor_type,
        w.anchor_month,
        w.window_label,

        -- Support metrics (missing months = 0 tickets; averages remain null if no tickets closed)
        COALESCE(s.tickets_opened, 0) AS tickets_opened,
        COALESCE(s.high_priority_tickets, 0) AS high_priority_tickets,
        COALESCE(s.escalated_tickets, 0) AS escalated_tickets,
        s.avg_resolution_hours,
        s.avg_first_response_minutes,
        s.avg_satisfaction_score,

        -- Usage metrics (missing months = 0 usage)
        COALESCE(u.total_usage_count, 0) AS total_usage_count,
        COALESCE(u.distinct_features_used, 0) AS distinct_features_used,
        CASE
            WHEN COALESCE(u.total_usage_count, 0) > 0 THEN 1
            ELSE 0
        END AS active_flag

    FROM window_labeled w
    LEFT JOIN v_support_account_month s
        ON s.account_id = w.account_id
        AND s.month_start = w.month_start

    LEFT JOIN v_usage_account_month u
        ON u.account_id = w.account_id
        AND s.month_start = w.month_start
),
window_avgs AS (
    SELECT
        account_id,
        anchor_type,
        anchor_month,
        window_label,

        AVG(tickets_opened::numeric) AS avg_tickets_opened,
        AVG(high_priority_tickets::numeric) AS avg_high_priority_tickets,
        AVG(escalated_tickets::numeric) AS avg_escalated_tickets,

        AVG(avg_resolution_hours) AS avg_resolution_hours,
        AVG(avg_first_response_minutes) AS avg_first_response_minutes,
        AVG(avg_satisfaction_score) AS avg_satisfaction_score,

        AVG(total_usage_count::numeric) AS avg_total_usage_count,
        AVG(distinct_features_used::numeric) AS avg_distinct_features_used,
        AVG(active_flag::numeric) AS share_active

    FROM joined
    GROUP BY
        account_id,
        anchor_type,
        anchor_month,
        window_label
),
pivoted AS (
    SELECT
        account_id,
        anchor_type,
        anchor_month,

        -- Support (recent vs prior)
        MAX(avg_tickets_opened) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS tickets_recent_3m,
        MAX(avg_tickets_opened) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS tickets_prior_3m,

        MAX(avg_high_priority_tickets) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS high_priority_recent_3m,
        MAX(avg_high_priority_tickets) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS high_priority_prior_3m,

        MAX(avg_escalated_tickets) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS escalations_recent_3m,
        MAX(avg_escalated_tickets) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS escalations_prior_3m,

        -- Usage (recent vs prior)
        MAX(avg_total_usage_count) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS usage_recent_3m,
        MAX(avg_total_usage_count) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS usage_prior_3m,

        MAX(avg_distinct_features_used) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS features_recent_3m,
        MAX(avg_distinct_features_used) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS features_prior_3m,

        MAX(share_active) FILTER (
            WHERE window_label = 'recent_3m'
        ) AS share_active_recent_3m,
        MAX(share_active) FILTER (
            WHERE window_label = 'prior_3m'
        ) AS share_active_prior_3m

    FROM window_avgs
    GROUP BY
        account_id,
        anchor_type,
        anchor_month
)
SELECT
    account_id,
    anchor_type,
    anchor_month,

    tickets_recent_3m,
    tickets_prior_3m,
    (tickets_recent_3m - tickets_prior_3m) AS tickets_delta,
    (tickets_recent_3m - tickets_prior_3m)
    / NULLIF(tickets_prior_3m, 0) AS tickets_delta_pct,

    high_priority_recent_3m,
    high_priority_prior_3m,
    (high_priority_recent_3m - high_priority_prior_3m) AS high_priority_delta,
    (high_priority_recent_3m - high_priority_prior_3m)
    / NULLIF(high_priority_prior_3m, 0) AS high_priority_delta_pct,

    escalations_recent_3m,
    escalations_prior_3m,
    (escalations_recent_3m - escalations_prior_3m) AS escalations_delta,
    (escalations_recent_3m - escalations_prior_3m)
    / NULLIF(escalations_prior_3m, 0) AS escalations_delta_pct,

    usage_recent_3m,
    usage_prior_3m,
    (usage_recent_3m - usage_prior_3m) AS usage_delta,
    (usage_recent_3m - usage_prior_3m)
    / NULLIF(usage_prior_3m, 0) AS usage_delta_pct,

    features_recent_3m,
    features_prior_3m,
    (features_recent_3m - features_prior_3m) AS features_delta,

    share_active_recent_3m,
    share_active_prior_3m,
    (share_active_recent_3m - share_active_prior_3m) AS share_active_delta

FROM pivoted;

-- QC Checks:
-- 1) One row per account
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT account_id) AS distinct_accounts
FROM v_change_signals_3m_account;

-- 2) Count churned vs retained anchors
SELECT
    anchor_type,
    COUNT(*) AS accounts
FROM v_change_signals_3m_account
GROUP BY anchor_type
ORDER BY anchor_type;

-- 3) Sanity check: sample rows (5 each)
SELECT *
FROM v_change_signals_3m_account
WHERE anchor_type = 'churned'
ORDER BY anchor_month DESC
LIMIT 5;

SELECT *
FROM v_change_signals_3m_account
WHERE anchor_type = 'retained'
ORDER BY anchor_month DESC
LIMIT 5;

-- Check window coverage for change signals
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
FROM v_change_signals_3m_account
GROUP BY anchor_type
ORDER BY anchor_type;