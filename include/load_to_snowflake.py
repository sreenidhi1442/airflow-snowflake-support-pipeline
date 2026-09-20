import pandas as pd
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

def log_error(conn, error_type, message, severity="HIGH"):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO support_project_db.raw.pipeline_errors
        (run_timestamp, error_type, error_message, severity)
        VALUES (%s, %s, %s, %s)
    """, (datetime.now(), error_type, message, severity))
    cur.close()

def load_data():
    conn = snowflake.connector.connect(
           user=os.environ['SNOWFLAKE_USER'],
password=os.environ['SNOWFLAKE_PASSWORD'],        account='mo67055.ap-southeast-7.aws',
        warehouse='COMPUTE_WH',
        database='support_project_db',
        schema='raw'
    )

    try:
        df = pd.read_csv("/usr/local/airflow/include/superstore_clean.csv")
        # MONITORING CHECK 1: row count sanity check
        if len(df) < 100:
            log_error(conn, "LOW_ROW_COUNT", f"Only {len(df)} rows found, expected 1000+")

        # MONITORING CHECK 2: null check on critical column
        null_sales = df['sales'].isna().sum()
        if null_sales > 0:
            log_error(conn, "NULL_VALUES", f"{null_sales} null values in sales column")

        # Rename columns to match Snowflake table (uppercase, Snowflake default)
        df.columns = [c.upper() for c in df.columns]

        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name="ORDERS", schema="RAW", database="SUPPORT_PROJECT_DB",
            overwrite=True
        )
        print(f"Loaded {nrows} rows successfully across {nchunks} chunk(s)")

    except Exception as e:
        log_error(conn, "LOAD_FAILURE", str(e), severity="CRITICAL")
        print(f"Load failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_data()