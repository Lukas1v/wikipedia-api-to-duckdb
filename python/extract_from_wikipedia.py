import requests
import duckdb
import pandas as pd
from typing import List, Dict, Any


# main pipeline function
def wikipedia_duckdb_pipeline(db_name: str, table_name: str, start_time: str, end_time: str) -> None:
    # extract
    recent_changes = fetch_recent_changes(start_time, end_time)

    #load
    print(f"loading to duckdb")
    load_into_duckdb(recent_changes, db_name=db_name, table_name=table_name)


# Function to fetch recent changes from Wikipedia API
def fetch_recent_changes(start_time: str, end_time: str) -> List[Dict[str, Any]]:
    """
    Fetches recent changes from the English Wikipedia API between start_time and end_time.
    """
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "recentchanges",
        "rcstart": end_time,
        "rcend": start_time,
        "rcnamespace": "*", 
        "rclimit": "max",
        "rcprop": "user|userid|comment|parsedcomment|flags|timestamp|title|ids|sizes|redirect|loginfo|tags|sha1",
        "format": "json"
    }
    changes = []
    while True:
        print("Number of entries fetched: ", str(len(changes)))
        response = requests.get(url, params=params).json()
        if "query" in response and "recentchanges" in response["query"]:
            changes.extend(response["query"]["recentchanges"])
        if "continue" in response:
            params.update(response["continue"])
        else:
            break
    
    return changes



# Load data into DuckDB
def load_into_duckdb(data: List[Dict[str, Any]], db_name: str, table_name: str) -> None:
    """
    Dynamically loads the fetched data into a DuckDB database, ensuring consistent keys.
    If the table already exists, it is dropped and recreated
    
    Args:
        data (List[Dict[str, Any]]): The data to load, represented as a list of dictionaries.
        db_name (str): Name of the DuckDB database file.
        table_name (str): Name of the table to create or overwrite.
    """
    if not data:
        print("No data to insert.")
        return

    # Ensure all records have consistent keys
    all_keys = get_all_keys(data)
    normalized_data = normalize_data(data, all_keys)

    with duckdb.connect(db_name) as conn:
        # Set up the table (drop and create)
        setup_table(conn, table_name, all_keys)

        # Insert data into the table
        insert_data(conn, table_name, all_keys, normalized_data)

        print(f"{len(normalized_data)} records inserted into DuckDB table '{table_name}'!")   


def get_all_keys(data: List[Dict[str, Any]]) -> List:
    """ Get all keys from the json data"""
    all_keys = set()
    for record in data:
        all_keys.update(record.keys())
    # Keep consistent order
    all_keys = sorted(all_keys)  

    return all_keys


def normalize_data(data: List[Dict[str, Any]], all_keys: List[str]) -> List[List[Any]]:
    """
    Normalize data to align with the schema by producing a list of ordered values.
    """
    return [[record.get(key, None) for key in all_keys] for record in data]


def setup_table(
        conn: duckdb.DuckDBPyConnection, 
        table_name: str, 
        all_keys:List[str]
)-> None:
    """
    Drops the table if it exists and creates a new table with the specified schema.
    """
    # Drop the table if it exists
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Dynamically create the table schema
    columns = ", ".join(f"{key} TEXT" for key in all_keys)
    conn.execute(f"CREATE TABLE {table_name} ({columns})")


def insert_data(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str, 
    all_keys: List[str], 
    normalized_data: List[Dict[str, Any]]
) -> None:
    """
    Inserts data into the specified table using DuckDB's Pandas integration for bulk upload.
    """
    # Convert normalized data into a Pandas DataFrame
    df = pd.DataFrame(normalized_data, columns=all_keys)
    
    # Use DuckDB's native support for bulk importing from Pandas
    conn.register("temp_df", df)  # Register the DataFrame as a DuckDB table
    conn.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM temp_df
    """)
    conn.unregister("temp_df")




if __name__ == "__main__":
    # parameters
    db_name = "wiki_recent_changes.db"
    table_name = "bronze_recent_changes"
    start_time = "2024-10-31T00:00:00Z"
    end_time = "2024-10-31T23:59:59Z"

    # run pipeline
    wikipedia_duckdb_pipeline(db_name, table_name, start_time, end_time)


