/*
14_churn_driver_table.sql

PURPOSE:
    - Join cohort labels to support and usage metrics.
    - Output summary stats that highlight differences before churn.

Dependencies
    - v_support_account_month (11_support_metrics.sql)
    - v_usage_account_month (12_usage_metrics.sql)
    - v_account_month_cohorts (13_prechurn_cohorts.sql)

Output
    - v_churn_driver_table_3m

Notes
    - pre_churn_3m is defined in v_account_month_cohorts (months_to_churn 1-3).
    - support and usage are left joined because some account-months may have no tickets or usage rows.
*/

SET search_path = ravenstack;

CREATE OR REPLACE VIEW v_churn_driver_table_3m AS
WITH spine AS (
    SELECT
        month_start,
        account_id,
        cohort_label
    FROM v_account_month_cohorts
    WHERE cohort_label IN ('retained', 'pre_churn_3m')
),
joined AS (
    SELECT
        s.month_start,
        s.account_id,
        s.cohort_label,

        -- Support metrics (default missing to 0)
        COALESCE(sm.tickets_opened, 0) AS tickets_opened,
        COALESCE(sm.high_priority_tickets, 0) AS high_priority_tickets,
        COALESCE(sm.escalated_tickets, 0) AS escalated_tickets,
        sm.avg_resolution_hours,
        sm.avg_first_response_minutes,
        sm.avg_satisfaction_score,

        -- Usage metrics (default missing to 0)
        COALESCE(um.total_usage_count, 0) AS total_usage_count,
        COALESCE(um.distinct_features_used, 0) AS distinct_features_used,
        CASE
            WHEN COALESCE(um.total_usage_count, 0) > 0 THEN 1
            ELSE 0
        END AS active_flag

    FROM spine s
    LEFT JOIN v_support_account_month sm
        ON sm.account_id = s.account_id
        AND sm.month_start = s.month_start

    LEFT JOIN v_usage_account_month um
        ON um.account_id = s.account_id
        AND um.month_start =s.month_start
),
summary AS (
    SELECT
        cohort_label,
        COUNT(*) AS account_month_rows,
        COUNT(DISTINCT account_id) AS accounts,

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
    GROUP BY cohort_label
)
SELECT *
FROM summary
ORDER BY cohort_label;

-- View output
SELECT *
FROM v_churn_driver_table_3m;

-- Check: retained should have more rows than pre_churn_3m
SELECT
    cohort_label,
    account_month_rows,
    accounts
FROM v_churn_driver_table_3m;