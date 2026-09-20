# airflow-snowflake-support-pipeline
Airflow pipeline that cleans Superstore sales data with pandas and loads it into Snowflake, with failure-handling practice
# Support Pipeline: Airflow + Snowflake

A small data pipeline built to practise the work of a pipeline-support engineer: running a scheduled workflow, reading task logs, diagnosing failures and recovering safely.

The pipeline cleans a Superstore sales CSV with pandas and loads about 10,000 rows into a Snowflake table. It runs on Apache Airflow, started locally with the Astro CLI and Docker.

## What it does

```
superstore_raw.csv --> clean_data --> superstore_clean.csv --> load_to_snowflake --> SUPPORT_PROJECT_DB.RAW.ORDERS
```

**`clean_data`** (`include/clean_data.py`)
- Standardises column names (lowercase, underscores)
- Removes exact duplicate rows and reports how many
- Drops rows where `sales` is missing
- Adds a `row_id` column and writes the cleaned CSV

**`load_to_snowflake`** (`include/load_to_snowflake.py`)
- Runs two checks before loading: a row-count sanity check and a null check on `sales`
- Loads the cleaned data into `RAW.ORDERS` with `write_pandas`
- Overwrites the table on each run, so re-running the DAG never creates duplicates
- Logs errors, then re-raises them so Airflow marks the task as failed

**DAG settings** (`dags/pipeline_dag.py`): daily schedule, one retry per task, no catch-up runs.

## Project layout

```
airflow-project/
  dags/
    pipeline_dag.py        # DAG definition: clean_data >> load_to_snowflake
  include/
    clean_data.py
    load_to_snowflake.py
    superstore_raw.csv
  requirements.txt         # includes snowflake-connector-python[pandas]
  .env                     # local credentials (never committed)
```

## Run it locally

**Prerequisites:** Docker Desktop (with WSL on Windows), the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli), and a Snowflake account with a database `SUPPORT_PROJECT_DB`, a schema `RAW` and a table `ORDERS`.

1. Clone the repository and open the `airflow-project` folder.
2. Create a `.env` file in that folder with your Snowflake login. Use plain `NAME=value` lines, with no quotes and no spaces:
```
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
```
3. Set your Snowflake account and warehouse in `include/load_to_snowflake.py`.
4. Start Airflow:
```
   astro dev start
```
5. Open http://localhost:8080 (default login `admin` / `admin`), unpause `support_pipeline_dag` and trigger it.
6. Check the result in Snowflake:
```
   SELECT COUNT(*) FROM SUPPORT_PROJECT_DB.RAW.ORDERS;
```

Useful commands: `astro dev restart` after changing `.env` or `requirements.txt`, `astro dev logs -s` for scheduler logs, `astro dev stop` to shut down.

## Problems I hit and how I fixed them

| Problem | How I found it | Fix |
|---|---|---|
| Load task showed green but loaded nothing | Task log said "Load failed", yet the task succeeded | The `except` block caught the error and never re-raised it. Added `raise` so Airflow marks the task as failed |
| `Missing optional dependency: pandas` | Task log | Changed `requirements.txt` to `snowflake-connector-python[pandas]` and rebuilt with `astro dev restart` |
| Every run doubled the rows (19,954 instead of 9,977) | Row count in Snowflake | `write_pandas` appends by default. Added `overwrite=True` so re-runs are idempotent |
| Password hard-coded in the source | Code review | Moved credentials into `.env` and read them with `os.environ` |
| `NameError: name 'os' is not defined` | Task log traceback | Added the missing `import os` |
| Snowflake error 390102, account temporarily locked | Task log | Repeated retries with a bad password locked the login. Fixed the value in `.env`, waited for the lock to expire, then cleared the task |
| `.env` changes not picked up | Checked variables inside the container with `astro dev bash` | Restarted with `astro dev restart`, since Airflow reads `.env` only at startup |

## Failure drill

To practise recovery, I deliberately broke the pipeline:

1. Renamed `include/superstore_raw.csv` and triggered the DAG.
2. `clean_data` failed with `FileNotFoundError`, retried once, then failed. `load_to_snowflake` showed `upstream_failed`.
3. Found the error in the task log, restored the file, then used **Clear Task** with downstream tasks included.
4. Both tasks re-ran and turned green.

## Tech

Apache Airflow, Astro CLI, Docker, Python, pandas, Snowflake (`snowflake-connector-python`), SQL.

## Security

Credentials live only in `.env`, which is listed in `.gitignore` and must never be committed.
