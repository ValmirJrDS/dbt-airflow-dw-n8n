# 3_airflow/dags/dag_jornada_dw.py

import os
from pendulum import datetime

def _generate_dbt_dag(env: str):
    # -- só aqui dentro é que carregamos tudo que pode atrasar o parsing --
    from airflow.models import Variable
    from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.profiles import PostgresUserPasswordProfileMapping

    # configurações de Dev vs Prod
    profile_dev = ProfileConfig(
        profile_name="jornada_dw",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="docker_postgres_db",
            profile_args={"schema": "public"},
        ),
    )
    profile_prod = ProfileConfig(
        profile_name="jornada_dw",
        target_name="prod",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="railway_postgres_db",
            profile_args={"schema": "public"},
        ),
    )

    # leitura do env: primeiro tenta env var, depois Airflow Variable
    dbt_env = os.getenv("DBT_ENV")
    if not dbt_env:
        # variável no Airflow (apenas se DBT_ENV não estiver setada no SO)
        dbt_env = Variable.get("dbt_env", default_var="dev")
    dbt_env = dbt_env.lower()
    if dbt_env not in ("dev", "prod"):
        raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

    profile_config = profile_dev if dbt_env == "dev" else profile_prod

    # fábrica do DAG Cosmos
    return DbtDag(
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/jornada_dw",
            project_name="jornada_dw",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        operator_args={
            "install_deps": True,
            "target": profile_config.target_name,
        },
        schedule="@daily",
        start_date=datetime(2025, 6, 12),
        catchup=False,
        dag_id=f"dag_jornada_dw_{dbt_env}",
        default_args={"retries": 2},
    )

# === aqui embaixo mantemos o module-level code mínimo ===
# Se você exportar apenas um ambiente (dev ou prod), 
# defina DBT_ENV no seu shell antes de rodar o Airflow:
#
#   export DBT_ENV=prod
#
# Senão, deixamos registrados os dois DAGs — útil em QA/staging:
for _env in ("dev", "prod"):
    globals()[f"dag_jornada_dw_{_env}"] = _generate_dbt_dag(_env)
