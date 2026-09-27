"""
An example Airflow DAG that uses cosmos + dbt + snowflake.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.profiles import SnowflakePrivateKeyFilePemProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) 
PROJECT_NAME = "my_snowflake_project"

# profile_config = ProfileConfig(
#     profile_name="my_snowflake_project", 
#     target_name="my_snowflake_project", 
#     profile_mapping=SnowflakeUserPasswordProfileMapping(
#         conn_id="snowflake_conn", 
#         profile_args={
#             "database": "dbt_db", 
#             "schema": "dbt_schema"
#         }
#     ),
# )

profile_config = ProfileConfig(
    profile_name="my_snowflake_project", 
    target_name="my_snowflake_project", 
    profile_mapping=SnowflakePrivateKeyFilePemProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={
            "database": "dbt_db", 
            "schema": "dbt_schema",
            "private_key_path": "/usr/local/airflow/keys/snowflake_rsa_key.p8",
            "private_key_passphrase": os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
        }
    ),
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / PROJECT_NAME,   
    ), 
    profile_config=profile_config, 
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV
    ), 
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command 
        "py_requirements": ["dbt-core", "dbt-snowflake"],
    }, 
    schedule="@daily", 
    start_date=datetime(2026, 9, 26), 
    catchup=False, 
    dag_id="dbt_dag",
    # default_args={"retries": 2},
    tags=["dbt", "snowflake"],
)