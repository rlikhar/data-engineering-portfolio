import pandas as pd
import sqlite3

def load_to_sqlite(input_path, db_path):
    df = pd.read_csv(input_path)

    conn = sqlite3.connect(db_path)
    df.to_sql("employee_kpi", conn, if_exists="replace", index=False)
    conn.close()

    print("Data loaded into SQLite")
