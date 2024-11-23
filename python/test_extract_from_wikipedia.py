import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from extract_from_wikipedia import (
    fetch_recent_changes, 
    load_into_duckdb, 
    get_all_keys, 
    normalize_data, 
    setup_table, 
    insert_data, 
    wikipedia_duckdb_pipeline
    )


class TestWikipediaDuckDBPipeline(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_recent_changes(self, mock_get):
        # Mock the API response
        mock_response = {
            "query": {
                "recentchanges": [
                    {"timestamp": "2024-10-31T01:00:00Z", "title": "Page 1", "user": "User1", "comment": "Edit 1"},
                    {"timestamp": "2024-10-31T02:00:00Z", "title": "Page 2", "user": "User2", "comment": "Edit 2"}
                ]
            }
        }
        mock_get.return_value.json.return_value = mock_response

        # Test fetch_recent_changes function
        result = fetch_recent_changes("2024-10-31T00:00:00Z", "2024-10-31T23:59:59Z")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["title"], "Page 1")
        self.assertEqual(result[1]["title"], "Page 2")


    @patch('duckdb.connect')
    def test_setup_table(self, mock_connect):
        # Mock a database connection
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        #run setup_table
        all_keys = ["timestamp", "title", "user", "comment"]
        table_name = "test_table"        
        setup_table(mock_conn,  table_name, all_keys)
        
        # Check if the drop and create statements have been executed
        mock_conn.execute.assert_any_call(f"DROP TABLE IF EXISTS {table_name}")
        mock_conn.execute.assert_any_call(f"CREATE TABLE {table_name} (timestamp TEXT, title TEXT, user TEXT, comment TEXT)")


    def test_get_all_keys(self):
        data = [
            {"timestamp": "2024-10-31T01:00:00Z", "title": "Page 1", "user": "User1"},
            {"timestamp": "2024-10-31T02:00:00Z", "comment": "Edit 2", "user": "User2"}
        ]
        # Test that get_all_keys extracts keys and sorts them
        keys = get_all_keys(data)
        self.assertEqual(keys, ['comment', 'timestamp', 'title', 'user'])


    def test_normalize_data(self):
        data = [
            {"timestamp": "2024-10-31T01:00:00Z", "title": "Page 1", "user": "User1"},
            {"timestamp": "2024-10-31T02:00:00Z", "comment": "Edit 2", "user": "User2"}
        ]
        all_keys = ['comment', 'timestamp', 'title', 'user']
        
        # Test that normalize_data normalizes the data according to all_keys
        normalized = normalize_data(data, all_keys)
        expected = [
            [None, "2024-10-31T01:00:00Z", "Page 1", "User1"],
            ["Edit 2", "2024-10-31T02:00:00Z", None, "User2"]
        ]
        self.assertEqual(normalized, expected)


    @patch('duckdb.connect')
    def test_insert_data(self, mock_connect):
        # Mock a database connection
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        all_keys = ['timestamp', 'title', 'user', 'comment']
        normalized_data = [
            ["2024-10-31T01:00:00Z", "Page 1", "User1", "Edit 1"]
        ]
        table_name = "test_table"

        # Test the insert_data function
        insert_data(mock_conn, table_name, all_keys, normalized_data)
        

        # Verify that the DataFrame was registered
        mock_conn.register.assert_called_once()
        registered_call_args = mock_conn.register.call_args
        assert registered_call_args[0][0] == "temp_df", "DataFrame was not registered with the correct name"

        # Verify the INSERT INTO statement
        mock_conn.execute.assert_any_call(f"\n        INSERT INTO {table_name}\n        SELECT * FROM temp_df\n    ")


    @patch('duckdb.connect')
    @patch('extract_from_wikipedia.insert_data')
    @patch('extract_from_wikipedia.setup_table')
    @patch('extract_from_wikipedia.normalize_data')
    @patch('extract_from_wikipedia.get_all_keys')
    def test_load_into_duckdb(self, mock_get_all_keys, mock_normalize_data, mock_setup_table, mock_insert_data, mock_connect):
        # setup- mocks
        mock_get_all_keys.return_value = ['timestamp', 'title', 'user', 'comment']
        mock_normalize_data.return_value = [
            ["2024-10-31T01:00:00Z", "Page 1", "User1", "Edit 1"]
        ]
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        #parameters
        data = [
            {"timestamp": "2024-10-31T01:00:00Z", "title": "Page 1", "user": "User1", "comment": "Edit 1"}
        ]
        db_name = "wiki_recent_changes.db"
        table_name = "bronze_recent_changes"     

        # Run function nand Assert that the sub-functions were called with the expected arguments
        load_into_duckdb(data, db_name, table_name)
        mock_get_all_keys.assert_called_once_with(data)
        mock_normalize_data.assert_called_once_with(data, mock_get_all_keys.return_value)
        mock_setup_table.assert_called_once_with(mock_conn, table_name, mock_get_all_keys.return_value)
        mock_insert_data.assert_called_once_with(mock_conn, table_name, mock_get_all_keys.return_value, mock_normalize_data.return_value)

        
    @patch('extract_from_wikipedia.load_into_duckdb')
    @patch('extract_from_wikipedia.fetch_recent_changes')
    def test_wikipedia_duckdb_pipeline(self, mock_fetch_recent_changes, mock_load_into_duckdb):
        # Test data
        db_name = "wiki_recent_changes.db"
        table_name = "bronze_recent_changes"
        start_time = "2024-10-31T00:00:00Z"
        end_time = "2024-10-31T23:59:59Z"
        
        # Mock the return value of fetch_recent_changes
        mock_fetch_recent_changes.return_value = [
            {"timestamp": "2024-10-31T01:00:00Z", "title": "Page 1", "user": "User1", "comment": "Edit 1"}
        ]
        
        # Call the pipeline function
        wikipedia_duckdb_pipeline(db_name, table_name, start_time, end_time)
        
        # Ensure fetch_recent_changes is called with the correct arguments
        mock_fetch_recent_changes.assert_called_once_with(start_time, end_time)
        
        # Ensure load_into_duckdb is called with the correct arguments
        mock_load_into_duckdb.assert_called_once_with(
            mock_fetch_recent_changes.return_value, db_name=db_name, table_name=table_name
        )



if __name__ == '__main__':
    unittest.main()
