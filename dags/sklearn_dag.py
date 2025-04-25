from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import pandas as pd

def train_model():
    # Load dataset
    iris = load_iris()
    X = iris.data
    y = iris.target

    # Split dataset
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Train model
    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)

    # Score
    accuracy = clf.score(X_test, y_test)
    print(f"Model Accuracy: {accuracy:.2f}")

with DAG(
    dag_id="sklearn_training_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",  # Run once
    catchup=False,
    tags=["ml", "sklearn"]
) as dag:

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model
    )

    train_task
