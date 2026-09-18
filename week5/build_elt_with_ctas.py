from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your snowflake DB
 - user_session_channel
 - session_timestamp

With regard to how to set up these two tables, please refer to this README file:
 - https://github.com/keeyong/sjsu-data226-FA25/blob/main/week8/How-to-setup-ETL-tables-for-ELT.md
"""

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def prep_data():
    sqls = """
    CREATE OR REPLACE TABLE raw.user_session_channel (
        userId int not NULL,
        sessionId varchar(32) primary key,
        channel varchar(32) default 'direct'  
    );

    CREATE OR REPLACE TABLE raw.session_timestamp (
        sessionId varchar(32) primary key,
        ts timestamp  
    );

    -- for the following query to run, 
    -- the S3 bucket should have LIST/READ privileges for everyone
    CREATE OR REPLACE STAGE raw.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');

    COPY INTO raw.user_session_channel
    FROM @raw.blob_stage/user_session_channel.csv;

    COPY INTO raw.session_timestamp
    FROM @raw.blob_stage/session_timestamp.csv;
    """
    cur = return_snowflake_conn()
    for sql in sqls.split(";"):
        cur.execute(sql)


@task
def run_ctas(schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'BuildELT_CTAS',
    start_date = datetime(2026,9,14),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """
        SELECT u.*, s.ts
        FROM raw.user_session_channel u
        JOIN raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    prep_data() >> run_ctas(schema, table, select_sql, primary_key='sessionId')
