import requests
import duckdb
import datetime
from typing import List, Dict, Any

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
    
    Args:
        data (List[Dict[str, Any]]): The data to load, represented as a list of dictionaries.
        db_name (str): Name of the DuckDB database file.
        table_name (str): Name of the table to create or overwrite.
    """
    if not data:
        print("No data to insert.")
        return
    
    # Ensure all records have consistent keys
    all_keys = set()
    for record in data:
        all_keys.update(record.keys())
    
    # Normalize data: fill missing keys with None
    all_keys = sorted(all_keys)  # Keep consistent order
    normalized_data = [
        {key: record.get(key, None) for key in all_keys} for record in data
    ]

    # Connect to DuckDB and Drop the table if it exists
    conn = duckdb.connect(db_name)
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Dynamically create the table schema
    columns = ", ".join(f"{key} TEXT" for key in all_keys)
    conn.execute(f"CREATE TABLE {table_name} ({columns})")

    # Insert data into the table
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(all_keys)})
        VALUES ({', '.join(['?' for _ in all_keys])})
    """
    conn.executemany(insert_query, [list(record.values()) for record in normalized_data])

    print(f"{len(data)} records inserted into DuckDB table '{table_name}'!")
    conn.close()


if __name__ == "__main__":
    # parameters
    db_name = "wiki_recent_changes.db"
    start_time = "2024-10-31T00:00:00Z"
    end_time = "2024-10-31T23:59:59Z"

    # extract
    recent_changes = fetch_recent_changes(start_time, end_time)

    #load
    load_into_duckdb(recent_changes, db_name="wiki_recent_changes.db", table_name="recent_changes_raw")


