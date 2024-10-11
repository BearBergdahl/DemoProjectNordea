import cx_Oracle

# Oracle connection parameters
oracle_conn_params = {
    "user": "your_username",
    "password": "your_password",
    "dsn": "your_host:your_port/your_service_name"
}

# Connect to Oracle
def createconn():
    conn = cx_Oracle.connect(**oracle_conn_params)
    # Create a cursor object
    cur = conn.cursor()
    return conn, cur

# Function to execute a query and fetch results
def execute_query(query):
    conn, cur = createconn()
    cur.execute(query)
    columns = [col[0] for col in cur.description]
    # Close the cursor and connection
    cur.close()
    conn.close()
    print("Data retrieved successfully!")
    return ([dict(zip(columns, row)) for row in cur.fetchall()], columns)




