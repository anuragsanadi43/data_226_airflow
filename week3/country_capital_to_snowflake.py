from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from cryptography.hazmat.primitives import serialization

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    user_id = Variable.get('snowflake_userid')
    account = Variable.get('snowflake_account')
    database = Variable.get('snowflake_database')
    warehouse = Variable.get('snowflake_warehouse')

    private_key_str = Variable.get("snowflake_rsa_private_key")
    passphrase = Variable.get("snowflake_rsa_private_key_passphrase")
    # PEM string -> private key object
    private_key = serialization.load_pem_private_key(
        private_key_str.encode("utf-8"),
        password=passphrase.encode("utf-8")
    )

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        account=account,  # Example: 'sfedu02-lvb17920'
        authenticator="SNOWFLAKE_JWT",
        private_key=private_key,
        warehouse=warehouse,
        database=database,
        role="ACCOUNTADMIN"
    )
    # Create a cursor object
    return conn.cursor()


@task
def extract(url):
    f = requests.get(url)
    return (f.text)


@task
def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    return records[1:]

@task
def load(records, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (country varchar primary key, capital varchar);")
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            country = r[0].replace("'", "''")
            capital = r[1].replace("'", "''")
            print(country, "-", capital)

            sql = f"INSERT INTO {target_table} (country, capital) VALUES ('{country}', '{capital}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'CountryCaptial',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    schedule = '0 2 * * *'
) as dag:
    target_table = "raw.country_capital"
    url = Variable.get("country_capital_url")

    data = extract(url)
    lines = transform(data)
    load(lines, target_table)
