from __future__ import annotations
import mlflow
import mlflow.sklearn
import pandas as pd
import glob
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
)
from sklearn.model_selection import train_test_split


def main():
    # 1. Path Setup (Using absolute path for reliability)
    path_pattern = "~/data-engineering-portfolio/fraud-pipeline/delta/bronze/bronze_date=*/part-*.parquet"
    expanded_path = os.path.expanduser(path_pattern)
    data_files = glob.glob(expanded_path)

    if not data_files:
        print(f"Error: No files found matching pattern: {expanded_path}")
        return

    # 2. Load Data
    df = pd.read_parquet(data_files)
    
    # 3. Define Features based on your actual column list
    # We will use the raw numerical and boolean flags available in bronze
    features = [
        "amount",
        "is_international",
        "is_velocity_burst",
        "is_high_risk_merchant",
        "baseline_risk_score"
    ]

    # Convert boolean columns to int for the model
    for col in ["is_international", "is_velocity_burst", "is_high_risk_merchant"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    X = df[features]
    
    # Your target column in bronze is 'fraud_label'
    y = df["fraud_label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    mlflow.set_tracking_uri("file:./mlflow_tracking")

    mlflow.set_experiment("fraud-detection")

    with mlflow.start_run():
        n_estimators = 100
        max_depth = 10

        model = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=42
        )

        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        # Metrics
        accuracy = accuracy_score(y_test, predictions)
        precision = precision_score(y_test, predictions)
        recall = recall_score(y_test, predictions)
        f1 = f1_score(y_test, predictions)

        # Logging
        mlflow.log_params({"n_estimators": n_estimators, "max_depth": max_depth})
        mlflow.log_metrics({
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1
        })

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="fraud_model",
            registered_model_name="fraud_detection_model"
        )

        print("=" * 60)
        print("MODEL TRAINING COMPLETE (USING BRONZE DATA)")
        print("=" * 60)
        print(f"Features used: {features}")
        print(f"Accuracy : {accuracy:.4f}")
        print(f"F1 Score : {f1:.4f}")

if __name__ == "__main__":
    main()