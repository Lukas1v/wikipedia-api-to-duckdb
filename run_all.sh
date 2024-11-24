#!/bin/bash
set -e

#run unit tests
python "python/test_extract_from_wikipedia.py"

#execute extract and load script
python "python/extract_from_wikipedia.py"

#install dbt dependencies
dbt deps --project-dir dbt_wikipedia_changes

# run dbt incl tests
dbt build --project-dir dbt_wikipedia_changes

