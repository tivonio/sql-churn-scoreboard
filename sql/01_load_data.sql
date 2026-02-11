/*
01_load_data.sql

PURPOSE:
    - Load the raw CSV files into the tables created in 00_create_tables.sql.
    - Keep the load process simple and reproducible.

NOTE:
    - This script uses server-side COPY.
      This means the CSV path must be visible to the Postgres server (the container),
      not the host PC. In Docker, this this typically works by mounting the local
      ./data/raw to /data/raw inside the container.

      If running psql from host machine without the mount, use \copy instead of COPY,
      and point to a host file path (because \copy reads files from the client machine,
      not the server/container).

Dependencies
    - Requires: 00_create_tables.sql

RERUN SAFETY:
    - If you rerun COPY without truncating, you can duplicate rows in tables without PKs.
    - In this schema, feature_usage_raw is particularly vulnerable to duplicateion as it
      has no natural unique key.
    - Clear tables (rerun 00_create_tables.sql) before reloading (safe rerun).
*/

-- Load order follows dependencies:
-- 1) accounts first (parent)
-- 2) subscriptions (depends on accounts)
-- 3) feature_usage_raw (depends on subscriptions via subscriptions_id)
-- 4) support_tickets and churn_events (depends on accounts)
COPY ravenstack.accounts
FROM '/data/raw/ravenstack_accounts.csv'
WITH (format csv, header true);

COPY ravenstack.subscriptions
FROM '/data/raw/ravenstack_subscriptions.csv'
WITH (format csv, header true);

COPY ravenstack.feature_usage_raw
    (usage_id, subscription_id, usage_date, feature_name,
    usage_count, usage_duration_secs, error_count, is_beta_feature)
FROM '/data/raw/ravenstack_feature_usage.csv'
WITH (format csv, header true);

COPY ravenstack.support_tickets
FROM '/data/raw/ravenstack_support_tickets.csv'
WITH (format csv, header true);

COPY ravenstack.churn_events
FROM '/data/raw/ravenstack_churn_events.csv'
WITH (format csv, header true);