import pandas as pd

def transform_data(input_path, output_path):
    df = pd.read_csv(input_path)

    # Convert times
    df["check_in"] = pd.to_datetime(df["check_in"], format="%H:%M", errors="coerce")
    df["check_out"] = pd.to_datetime(df["check_out"], format="%H:%M", errors="coerce")

    # Calculate working hours
    df["working_hours"] = (df["check_out"] - df["check_in"]).dt.total_seconds() / 3600

    # Handle nulls
    df["working_hours"] = df["working_hours"].fillna(0)

    df.to_csv(output_path, index=False)
    print("Transformation complete")

    return output_path
