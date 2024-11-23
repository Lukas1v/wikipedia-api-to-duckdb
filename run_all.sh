#!/bin/bash
set -e

#run unit tests
python "python/test_extract_from_wikipedia.py"

#execute extract and load script
python "python/extract_from_wikipedia.py"

# run dbt incl tests
dbt build --project-dir /workspaces/wikipedia-api-to-duckdb/dbt_wikipedia_changes

