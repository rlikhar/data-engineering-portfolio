import pandas as pd

def calculate_metrics(input_path, output_path):
    df = pd.read_csv(input_path)

    # Flags
    df["is_late"] = pd.to_datetime(df["check_in"], errors="coerce").dt.time > pd.to_datetime("09:30").time()
    df["is_absent"] = df["status"] == "Absent"

    # Aggregation
    kpi = df.groupby("employee_id").agg({
        "working_hours": "mean",
        "is_late": "sum",
        "is_absent": "sum"
    }).reset_index()

    kpi.to_csv(output_path, index=False)
    print("Metrics calculated")

    return output_path
