from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pendulum import datetime

profile_config = ProfileConfig(
        profile_name="dbt_dw",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="docker_postgres_db",
                profile_args={"schema": "public"}
        ),

)

my_cosmos_dag = DbtDag(

    project_config=ProjectConfig(
        "/usr/local/airflow/dbt/dbt_dw",
    
    ),
    
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    
    ),

    operator_args={
        "install_deps": True,
       
    },

    schedule="@daily",
    start_date=datetime(2025,9,23),
    catchup=False,
    dag_id="dag_dbt_dw",
    default_args={"retries": 2},
)