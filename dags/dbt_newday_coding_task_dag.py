from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import psycopg2
import json

docs_md = """

# DAG: dbt_newday_coding_task_dag

## Overview
This DAG orchestrates a sequence of dbt tasks for the **dbt_newday_coding_assignment** project.  
It uses BashOperators to execute dbt commands inside the Airflow environment.

"""


DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_newday_coding_assignment"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_TARGET = "dev"                                   # must exist in profiles.yml

default_args = {
    "owner": "airflow",
    "retries": 1,
}


def get_schema_table_counts(**context):
    conn = psycopg2.connect(
        host="192.168.0.208",
        port=5432,
        dbname="dbt_db",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()

    # Get list of schemas (excluding system schemas)
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    """)
    schemas = [row[0] for row in cur.fetchall()]

    schema_table_counts = {}

    for schema in schemas:
        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
        """)
        tables = [row[0] for row in cur.fetchall()]

        schema_table_counts[schema] = {}
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = cur.fetchone()[0]
            schema_table_counts[schema][table] = count

    cur.close()
    conn.close()
    print(json.dumps(schema_table_counts, indent=2))  # Also print in logs


with DAG(
    dag_id="dbt_newday_coding_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "bash"],
    doc_md=docs_md
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    dbt_seeds = BashOperator(
        task_id="dbt_seeds",
        bash_command=(
            f'export PATH="/home/airflow/.local/bin:$PATH"; '
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt seed --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}'
        ),
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=(
            f'export PATH="/home/airflow/.local/bin:$PATH"; '
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt compile --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}'
        ),
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f'export PATH="/home/airflow/.local/bin:$PATH"; '
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt run --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}'
        ),
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f'export PATH="/home/airflow/.local/bin:$PATH"; '
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt test --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}'
        ),
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    get_counts_task = PythonOperator(
        task_id="get_schema_table_counts",
        python_callable=get_schema_table_counts,
        provide_context=True
    )

    start >> dbt_seeds >> dbt_compile >> dbt_run >> get_counts_task >> dbt_test >> end
