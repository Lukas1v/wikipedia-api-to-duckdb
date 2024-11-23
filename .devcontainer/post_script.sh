mkdir /home/vscode/.dbt

cat <<EOF > /home/vscode/.dbt/profiles.yml
dbt_wikipedia_changes:
  target: dev
  outputs:
    dev:
      type: duckdb
      threads: 10
      path: /workspaces/wikipedia-api-to-duckdb/wiki_recent_changes.db
      schema: main
EOF

python -m ipykernel install --user --name myenv --display-name "Python (myenv)"