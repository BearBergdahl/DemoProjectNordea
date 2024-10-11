import pandas as pd
import os



def extract_from_csv():
    if not os.path.exists(".DAGS/resources/titanic.csv"):
        raise FileNotFoundError(f"CSV file not found: {"./resources/titanic.csv"}")

    df = pd.read_csv(".DAGS/resources/titanic.csv")
    print(df.head())
    return df