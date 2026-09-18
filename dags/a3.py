from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import AirflowException
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

url = "https://jsonplaceholder.typicode.com/posts/1"

@task
def extract():
    raw_text = requests.get(url)

    # Raise an HTTPError if the response code was 4xx or 5xx
    raw_text.raise_for_status()

    return raw_text.json()

@task
def transform(json_data):

    if not json_data:
        raise AirflowException("Received empty dataset from the extract task")
        
    """selecting the first 19 characters of both title and body columns of the post
        because inserting the entire the values in the title and body column are too long
    """
    json_data["title"] = json_data.get("title", "")[:19]
    json_data["body"] = json_data.get("body", "")[:19]

    return json_data

@task
def load(json_data):
    curr = return_snowflake_conn()
    try:
        curr.execute("BEGIN")
        curr.execute("""create table if not exists DATA226.RAW.posts
            (userId int primary key, id int, title varchar(20), body varchar(20))""")
        curr.execute("delete from DATA226.RAW.posts")
        insert_query = "insert into DATA226.RAW.posts (userId, id, title, body) values (%s, %s, %s, %s)"
        insert_q_bindings = (
            json_data.get("userId"),
            json_data.get("id"),
            json_data.get("title"),
            json_data.get("body")
        )
        curr.execute(insert_query, insert_q_bindings)
        curr.execute("commit")
    except Exception as e:
        curr.execute("rollback")
        print(e)
        raise e


with DAG(
    dag_id = 'GetPosts',
    start_date = datetime(2026, 9, 18),
    catchup=False,
    tags=['ETL'],
    schedule = '0 2 * * *',
) as dag:
    raw_data = extract()
    json_data = transform(raw_data)
    load(json_data)