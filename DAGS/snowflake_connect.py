import snowflake.connector
import pandas as pd

# Snowflake connection parameters
snowflake_conn_params = {
    "account": "your_account_identifier",
    "user": "your_username",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

def create_conn():
    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_conn_params)
    # Create a cursor object
    cur = conn.cursor()
    return conn, cur


# Function to insert data into Snowflake
def insert_data(df, table_name):
    # Create the table if it doesn't exist
    conn, cur = create_conn()
    columns = ", ".join([f"{col} VARCHAR" for col in df.columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cur.execute(create_table_query)

    # Insert data into the table
    for _, row in df.iterrows():
        values = ", ".join([f"'{str(value)}'" for value in row])
        insert_query = f"INSERT INTO {table_name} VALUES ({values})"
        cur.execute(insert_query)

    # Commit the changes
    conn.commit()
    # Close the cursor and connection
    cur.close()
    conn.close()

    print("Data inserted successfully!")



