from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="run_all_pipelines",
    start_date=datetime(2026, 5, 11),
    catchup=False,
    schedule_interval=None,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["run-all"],
) as dag:

    trigger_csv_to_bronze = TriggerDagRunOperator(
        task_id="trigger_neo_csv_to_bronze",
        trigger_dag_id="neo_csv_to_bronze",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_neo_bronze_to_silver",
        trigger_dag_id="neo_bronze_to_silver",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_neo_silver_to_gold",
        trigger_dag_id="neo_silver_to_gold",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    trigger_model_training = TriggerDagRunOperator(
        task_id="trigger_neo_model_training",
        trigger_dag_id="neo_model_training",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    trigger_csv_to_bronze >> trigger_bronze_to_silver >> trigger_silver_to_gold >> trigger_model_training
