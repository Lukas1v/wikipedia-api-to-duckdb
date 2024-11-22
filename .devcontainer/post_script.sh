mkdir /home/vscode/.dbt

cat <<EOF > /home/vscode/.dbt/profiles.yml
test_duckdb_userdata:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/workspaces/dbt_test/dev.duckdb'
      threads: 10
EOF

python -m ipykernel install --user --name myenv --display-name "Python (myenv)"