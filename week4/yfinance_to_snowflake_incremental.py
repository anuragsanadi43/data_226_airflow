from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

import os
import snowflake.connector
import requests
import yfinance as yf


# now putting everything together and create a function
def save_stock_price_as_file(symbol, start_date, end_date, file_path):
    """
    Download stock price data for a given symbol and save it as a CSV file.
    """
    data = yf.download([symbol], start=start_date, end=end_date)
    data.columns = data.columns.droplevel(1)
    data["Symbol"] = symbol
    data.to_csv(file_path)


def populate_table_via_stage(cur, database, schema, table, file_path):
    """
    Populate a table with data from a given CSV file using Snowflake's COPY INTO command.
    """
    # Create a temporary named stage instead of using the table stage
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)  # extract only filename from the path

    # First set the schema since table stage or temp stage needs to have the schema as the target table
    cur.execute(f"USE SCHEMA {database}.{schema}")

    # Create a temporary named stage
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name}")

    # Copy the given file to the temporary stage
    cur.execute(f"PUT file://{file_path} @{stage_name}")

    # Run copy into command with fully qualified table name
    copy_query = f"""
        COPY INTO {schema}.{table}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


def get_logical_date():
    # Get the current context
    context = get_current_context()
    return str(context['logical_date'])[:10]


def get_next_day(date_str):
    """
    Given a date string in 'YYYY-MM-DD' format, returns the next day as a string in the same format.
    """
    # Convert the string date to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Add one day using timedelta
    next_day = date_obj + timedelta(days=1)

    # Convert back to string in "YYYY-MM-DD" format
    return next_day.strftime("%Y-%m-%d")


@task
def extract(symbol):
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)

    print(f"========= Reading {date_to_fetch}'s data ===========")

    file_path = f"/tmp/{symbol}_{date_to_fetch}.csv"
    save_stock_price_as_file(symbol, date_to_fetch, next_day_of_date_to_fetch, file_path)

    return file_path


@task
def load(file_path, database, schema, target_table):
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)

    print(f"========= Updating {date_to_fetch}'s data ===========")
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")
        cur.execute(f"DELETE FROM {database}.{schema}.{target_table} WHERE date='{date_to_fetch}'")
        populate_table_via_stage(cur, database, schema, target_table, file_path)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'YfinanceToSnowflake_Inc',
    start_date = datetime(2026,9,5),
    catchup=False,
    tags=['ETL'],
    schedule = '30 3 * * *'
) as dag:
    database = "dev"  # change this to according to your database
    schema = "raw"
    target_table = "stock_price"
    symbol = "AAPL"

    file_path = extract(symbol)
    load(file_path, database, schema, target_table)
