from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_newday_coding_assignment"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_TARGET = "dev"                                   # must exist in profiles.yml

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="dbt_newday_coding_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "bash"],
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

    start >> dbt_seeds >> dbt_compile >> dbt_run >> dbt_test >> end
