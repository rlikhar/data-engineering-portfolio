import pandas as pd

def validate_data(input_path):
    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    if df["employee_id"].isnull().any():
        raise ValueError("Missing employee_id found")

    if df["date"].isnull().any():
        raise ValueError("Missing date values")

    # Validate time logic (skip nulls)
    valid_df = df.dropna(subset=["check_in", "check_out"])

    if not valid_df.empty:
        invalid_time = valid_df[
            pd.to_datetime(valid_df["check_out"], format="%H:%M") <
            pd.to_datetime(valid_df["check_in"], format="%H:%M")
        ]
        if not invalid_time.empty:
            raise ValueError("Invalid time records found")

    print("Validation passed")
    return input_path
