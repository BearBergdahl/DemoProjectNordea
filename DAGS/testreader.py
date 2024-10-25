import pandas as pd
import os



def extract_from_csv():
    if not os.path.exists("resources/titanic.csv"):
        raise FileNotFoundError(f"CSV file not found: {"./resources/titanic.csv"}")

    df = pd.read_csv("./resources/titanic.csv")
    print(df.head())
    return df