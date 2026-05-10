from __future__ import annotations
import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

AIRFLOW_DATA_DIR = "/opt/airflow/data"
RAW_CSV_PATH = "/opt/airflow/neo.csv"
BRONZE_PATH = os.path.join(AIRFLOW_DATA_DIR, "bronze", "neo.parquet")


def _pg_type_from_dtype(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def _create_table_sql(table_name: str, df: pd.DataFrame) -> str:
    columns = []
    for column_name, dtype in df.dtypes.items():
        columns.append(f'"{column_name}" {_pg_type_from_dtype(dtype)}')
    schema = ", ".join(columns)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({schema});'


with DAG(
    dag_id="neo_csv_to_bronze",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["neo", "bronze"],
) as dag:

    @task
    def extract_to_bronze() -> None:
        os.makedirs(os.path.dirname(BRONZE_PATH), exist_ok=True)
        df = pd.read_csv(RAW_CSV_PATH)
        df.to_parquet(BRONZE_PATH, index=False)

    extract_to_bronze()


def extract_bronze() -> None:
    df = pd.read_parquet(BRONZE_PATH)

    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "staging_neo";')
    hook.run(_create_table_sql("staging_neo", df))

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    hook.insert_rows(
        table="staging_neo",
        rows=rows,
        target_fields=df.columns.tolist(),
        commit_every=1000,
    )
    

def drop_zero_variance_columns() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    df = hook.get_pandas_df('SELECT * FROM "bronze_neo";')
    nunique = df.nunique()
    zero_var_cols = nunique[nunique <= 1].index.tolist()
    if zero_var_cols:
        cols_str = ", ".join(f"DROP COLUMN {col}" for col in zero_var_cols)
    hook.run(f'ALTER TABLE "staging_neo" {cols_str};')
    

def rename_cols_and_create_unique_id() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run("""
        ALTER TABLE "staging_neo" RENAME COLUMN "id" TO "asteroid_id";
        ALTER TABLE "staging_neo" RENAME COLUMN "name" TO "asteroid_name";
        ALTER TABLE "staging_neo" RENAME COLUMN "absolute_magnitude" TO "luminosity_abs_mag";
        ALTER TABLE "staging_neo" RENAME COLUMN "hazardous" TO "is_asteroid_hazardous";
        ALTER TABLE "staging_neo" ADD COLUMN observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
    """)


def create_silver_table() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "silver_neo";')
    hook.run("""
        CREATE TABLE IF NOT EXISTS "silver_neo" AS
        SELECT * FROM "staging_neo";
    """)

    
def cleanup_staging() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "staging_neo";')


with DAG(
    dag_id="neo_bronze_to_silver",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["neo", "silver"],
) as dag:

    extract = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_bronze
    )
    
    drop_cols = PythonOperator(
        task_id="drop_zero_variance_columns",
        python_callable=drop_zero_variance_columns
    )
    
    rename_and_create_id = PythonOperator(
        task_id="rename_and_create_unique_id",
        python_callable=rename_cols_and_create_unique_id
    )
    
    load_silver = PythonOperator(
        task_id="create_silver_table",
        python_callable=create_silver_table
    )
    
    cleanup = PythonOperator(
        task_id="cleanup_staging",
        python_callable=cleanup_staging
    )
    
    extract >> drop_cols >> rename_and_create_id >> load_silver >> cleanup
    

def exctract_silver() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "staging_neo_gold";')
    try:
        hook.run("""
            CREATE TABLE "staging_neo_gold" AS
            SELECT * FROM "silver_neo";
        """)
    except Exception as e:
        print(f"Error occurred while creating 'staging_neo_gold': {e}")


def create_asteroid_dimension_table() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "asteroid_dimension";')
    hook.run("""
        CREATE TABLE IF NOT EXISTS "asteroid_dimension" AS
        SELECT DISTINCT asteroid_id, asteroid_name, luminosity_abs_mag, is_asteroid_hazardous
        FROM "staging_neo_gold";
    """)
    try:
        hook.run('ALTER TABLE "asteroid_dimension" ADD PRIMARY KEY (asteroid_id);')
    except Exception as e:
        print(f"Error occurred while adding primary key to 'asteroid_dimension': {e}")


def create_observations_fact_table() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "observations_fact";')
    hook.run("""
        CREATE TABLE IF NOT EXISTS "observations_fact" AS
        SELECT observation_id, asteroid_id, est_diameter_min, est_diameter_max, relative_velocity, miss_distance
        FROM "staging_neo_gold";
    """)
    try:
        hook.run('ALTER TABLE "observations_fact" ADD PRIMARY KEY (observation_id);')
        hook.run("""
            ALTER TABLE "observations_fact"
            ADD CONSTRAINT fk_asteroid
            FOREIGN KEY (asteroid_id)
            REFERENCES "asteroid_dimension"(asteroid_id);
        """)
    except Exception as e:
        print(f"Error occurred while adding primary key or foreign key to 'observations_fact': {e}")
    

def create_neo_gold_ml() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "neo_gold_ml";')
    hook.run("""
        CREATE TABLE IF NOT EXISTS "neo_gold_ml" AS
        SELECT
            asteroid_id,
            count(*) as num_observations,
            min(est_diameter_min) as est_diameter_min,
            max(est_diameter_max) as est_diameter_max,
            min(relative_velocity) as min_relative_velocity,
            max(relative_velocity) as max_relative_velocity,
            avg(relative_velocity) as avg_relative_velocity,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY relative_velocity) as median_relative_velocity,
            stddev_samp(relative_velocity) as stddev_relative_velocity,
            min(miss_distance) as min_miss_distance,
            max(miss_distance) as max_miss_distance,
            avg(miss_distance) as avg_miss_distance,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY miss_distance) as median_miss_distance,
            stddev_samp(miss_distance) as stddev_miss_distance,
            avg(luminosity_abs_mag) as avg_luminosity_abs_mag,
            cast(is_asteroid_hazardous as int) as is_asteroid_hazardous
        FROM "staging_neo_gold"
        GROUP BY asteroid_id, is_asteroid_hazardous
        ORDER BY asteroid_id;
    """)
    try:
        hook.run('ALTER TABLE "neo_gold_ml" ADD PRIMARY KEY (asteroid_id);')
    except Exception as e:
        print(f"Error occurred while adding primary key to 'neo_gold_ml': {e}")


def cleanup_gold_staging() -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run('DROP TABLE IF EXISTS "staging_neo_gold";')


with DAG(
    dag_id="neo_silver_to_gold",
    start_date=datetime(2026, 5, 10),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["neo", "gold"],
) as dag:

    extract = PythonOperator(
        task_id="extract_silver",
        python_callable=exctract_silver
    )
    
    create_dim = PythonOperator(
        task_id="create_asteroid_dimension_table",
        python_callable=create_asteroid_dimension_table
    )
    
    create_fact = PythonOperator(
        task_id="create_observations_fact_table",
        python_callable=create_observations_fact_table
    )
    
    create_ml = PythonOperator(
        task_id="create_neo_gold_ml",
        python_callable=create_neo_gold_ml
    )
    
    cleanup = PythonOperator(
        task_id="cleanup_gold_staging",
        python_callable=cleanup_gold_staging
    )
    
    extract >> [create_dim, create_fact] >> create_ml >> cleanup