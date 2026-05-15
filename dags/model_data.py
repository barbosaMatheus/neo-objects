from __future__ import annotations
from datetime import datetime
import json
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV


X_COLS = ["num_observations", "est_diameter_min", "est_diameter_max",
          "min_relative_velocity", "max_relative_velocity", "avg_relative_velocity",
          "median_relative_velocity", "stddev_relative_velocity", "min_miss_distance",
          "max_miss_distance", "avg_miss_distance", "median_miss_distance",
          "stddev_miss_distance", "avg_luminosity_abs_mag"]

TARGET = "is_asteroid_hazardous"


def get_gold_ml_data() -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id="neo_db")
    df = hook.get_pandas_df('SELECT * FROM "neo_gold_ml";')
    return df


def train_model(**context) -> None:
    df = get_gold_ml_data()
    X = df[X_COLS]
    y = df[TARGET]
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = GridSearchCV(
        DecisionTreeClassifier(random_state=123),
        param_grid={
            'criterion': ['gini', 'entropy'],
            'max_depth': [None, 5, 10],
            'min_samples_split': [5, 10, 15, 20],
        },
        cv=5,
        scoring='accuracy',
        refit=True,
        n_jobs=-1,
    )
    
    model.fit(X_scaled, y)
    print(f"Best Parameters: {model.best_params_}")
    print(f"Best Accuracy from Grid Search: {model.best_score_:.4f}")
    
    accuracy = model.score(X_scaled, y)
    print(f"Model Accuracy: {accuracy:.4f}")
    feature_importances = sorted(
        zip(X.columns, model.best_estimator_.feature_importances_),
        key=lambda item: item[1],
        reverse=True,
    )
    feature_importances_list = [
        {"feature": name, "importance": importance}
        for name, importance in feature_importances
    ]
    print(f"Feature Importances: {feature_importances_list}")
    
    model_analysis_results = {
        "best_params": model.best_params_,
        "best_score": model.best_score_,
        "accuracy": accuracy,
        "feature_importances": feature_importances_list
    }
    
    return model_analysis_results


def update_model_analysis_table(**context) -> None:
    hook = PostgresHook(postgres_conn_id="neo_db")
    hook.run("""
        CREATE TABLE IF NOT EXISTS "model_analysis" (
            id SERIAL PRIMARY KEY,
            best_params JSONB,
            best_score FLOAT,
            accuracy FLOAT,
            feature_importances JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    results = context['ti'].xcom_pull(task_ids='train_model')
    if results:
        hook.run("""
            INSERT INTO "model_analysis" (best_params, best_score, accuracy, feature_importances)
            VALUES (%s, %s, %s, %s);
        """, parameters=(
            json.dumps(results["best_params"]),
            results["best_score"],
            results["accuracy"],
            json.dumps(results["feature_importances"])
        ))


with DAG(
    dag_id="neo_model_training",
    start_date=datetime(2026, 5, 11),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["gold", "model"],
) as dag:

    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model
    )
    
    analysis_export = PythonOperator(
        task_id="update_model_analysis_table",
        python_callable=update_model_analysis_table,
        provide_context=True
    )

    train >> analysis_export