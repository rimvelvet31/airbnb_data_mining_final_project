import sqlite3
import pandas as pd


def select_data(sql):

    # Check if the SQL statement is a SELECT query
    if not sql.strip().lower().startswith('select'):
        print("Use only SELECT commands.")
        return None

    # Connect to SQLite database
    conn = sqlite3.connect("data/airbnb.db")

    # Execute the SQL query and fetch data into a DataFrame
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        df = None

    # Close connection
    conn.close()

    return df
