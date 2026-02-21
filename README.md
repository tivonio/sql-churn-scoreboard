# SQL Churn Analysis Series

This repo contains the complete SQL walkthrough for the blog post churn series (published on tivon.io):

1) ["We Lost Customers, But Not Revenue (Three churn definitions that explain it)"](https://tivon.io/2026/02/15/we-lost-customers-but-not-revenue/)
   - churn definitions + scoreboard (SQL 00-11, notebook 01)
2) "Why Customers Churn (Linking support tickets and product usage to churn risk)" -> UPCOMING

It uses a public SaaS-style dataset (Ravenstack) to demonstrate a common analytics mismatch:

- A stakeholder sees **customers are down** (logo churn)
- Finance sees **revenue is fine** (MRR movement + retention)
- Product risk still can be building under the surface (activity churn)

* * *

## What you will reproduce

You will compute three churn lenses from the same underlying account-month snapshot and reconcile them in a single monthly scoreboard:

- **Logo churn**: paying last month -> not paying this month
- **Revenue churn + retention**: MRR waterfall components + NRR
- **Activity churn**: paying customers who go inactive (usage drops to zero)

You will also reproduce three supporting views used in the post visuals:

- A **Scoreboard trends** chart
- An **MRR waterfall** for a highlighted month
- **MRR mix shift by plan tier** over time

* * *

## Repo contents

### `data/raw/`

These are the Ravenstack CSVs loaded into Postgres:

- `ravenstack_accounts.csv`
- `ravenstack_subscriptions.csv`
- `ravenstack_feature_usage.csv`
- `ravenstack_support_tickets.csv`
- `ravenstack_churn_events.csv`

### `sql/`

Run these in order:

1. `00_create_tables.sql`  
   Creates the `ravenstack` schema and base tables.

2. `01_load_data.sql`  
   Loads CSVs via server-side `COPY` from `/data/raw`.

3. `02_profile.sql`  
   Quick profiling and validation checks (row counts, uniqueness, and basic integrity checks).

4. `03_account_mrr_month.sql`  
   `v_account_mrr_month` (one row per account-month; month-end style snapshot).

5. `04_logo_churn.sql`  
   `v_logo_churn_month` (logo churn, new customers, customer counts).

6. `05_revenue_waterfall.sql`  
   `v_revenue_waterfall_month` (MRR waterfall, gross revenue churn, NRR).

7. `06_activity_churn.sql`  
   `v_activity_churn_month` (inactive-but-paying, activity churn).

8. `07_scoreboard.sql`  
   `v_churn_scoreboard_month` (logo + revenue + activity churn reconciled in one view).

9. `08_checks.sql`  
   Reconciliation checks (MRR and customer count invariants).

10. `09_mix_shift_by_plan.sql`  
   `v_mix_shift_by_plan_month` (tier mix shift over time).

11. `10_extracts_for_python.sql`  
   Plot-ready extracts used by the notebook.

### `notebooks/`
1. `01_visuals.ipynb`

Notebook that generates the three figures used in the post:

- `fig_01_scoreboard_trends.png`
- `fig_02_mrr_waterfall_2024_09.png`
- `fig_03_mrr_mix_shift_stacked.png`

* * *

## Key modeling decisions (why the numbers are reproducible)

### Month-end subscription snapshot

Churn math is computed on a single consistent grain: **one row per account per month**, anchored to **month close**.

**Month-end rule:** a subscription counts for a month only if it is active **as of the last day of the month**.

Accounts can have overlapping subscription rows. `v_account_mrr_month` resolves this by selecting one “winner” subscription per account-month using a deterministic ranking rule:

- latest `start_date`
- then highest `mrr_amount`
- then `subscription_id`

That avoids double-counting and makes month-over-month comparisons stable.

**Account-month spine:** month-end snapshots omit non-paying months, so downstream churn views build a complete account-month spine (every account × every month) and fill missing months with zeros. This ensures month-to-month comparisons are truly adjacent calendar months (instead of “previous paying month”).

### `feature_usage_raw` is intentionally a landing table

The source usage file can contain duplicate `usage_id` values. The table uses a surrogate `row_id` (bigserial) instead of enforcing a primary key on `usage_id`.

Downstream usage logic aggregates usage to the account-month level.

* * *

## Checks (trust before interpretation)

`08_checks.sql` validates two invariants:

1) **MRR waterfall reconciliation**

`starting + new + expansion - contraction - churned = ending`

2) **Customer reconciliation**

`customers_end = customers_start + new_customers - churned_customers`

If diffs are non-zero, stop and debug before interpreting churn.

* * *

## Prerequisites

- Docker Desktop
- A PostgreSQL container (this repo uses Postgres 18)
- `psql` available inside the container (standard for official postgres images)
- Optional: VS Code + PostgreSQL extension (or SQLTools)
- Optional (for visuals): Python + Jupyter (see `requirements.txt`)

* * *

## Quickstart (PowerShell)

### 1) Clone the repo

    cd $HOME
    git clone https://github.com/tivonio/sql-churn-scoreboard.git
    cd sql-churn-scoreboard

### 2) Start PostgreSQL with Docker Compose (recommended)

    docker compose up -d

Confirm the container is running:

    docker ps

You should see a container named `pg18`.

* * *

## Load the Ravenstack dataset

This project uses server-side `COPY`, which means the CSV path must be visible to the Postgres server.

The included `docker-compose.yml` mounts:

- `./data/raw` (host) → `/data/raw` (container, read-only)

So `/data/raw/*.csv` exists inside the container.

### Option 1: Run via `psql` in the container

If you want to run the SQL files *from inside the container*, the SQL scripts need to be accessible inside the container too.

The simplest approach is to mount the repo into the container (for example: `/workspace`) by adding this to your compose service volumes:

    - ./:/workspace

Then run (example):

    docker exec -it pg18 psql -U postgres -d lab -f /workspace/sql/00_create_tables.sql
    docker exec -it pg18 psql -U postgres -d lab -f /workspace/sql/01_load_data.sql

### Option 2 (recommended for learning): Run inside VS Code

1. Open the repo folder in VS Code.
2. Use the PostgreSQL extension to connect to the container database.
   - Host: `localhost`
   - Port: `5434` (from `docker-compose.yml`)
   - Database: `lab`
   - User: `postgres`
3. Open a file in `sql/` and run the statements from the editor (in order).

This keeps your workflow inside `.sql` files, which is ideal for learning and for clean GitHub diffs.

* * *

## Generate the figures (optional)

The notebook reads from the SQL extracts and produces the three post figures.

If you are using a virtual environment:

    python -m venv .venv
    .\.venv\Scripts\activate
    pip install -r requirements.txt

Then open `01_visuals.ipynb` and run the cells.

* * *

## Reset the project (start over clean)

Resetting is useful when you want to reproduce the walkthrough from scratch.

### If you used Docker Compose

    docker compose down -v
    docker compose up -d

* * *

## Notes

- If `01_load_data.sql` fails with file path errors, the most common cause is that `/data/raw` is not mounted into the container.
- If you are running `psql` from your host machine (client-side), use `\copy` instead of `COPY` so the file path resolves on the client. 
- `.gitignore` excludes typical local artifacts.
- `requirements.txt` pins the Python environment used for the notebook (pandas/matplotlib/psycopg2, plus Jupyter dependencies).
