# Dockerfile
FROM mcr.microsoft.com/vscode/devcontainers/python:3.12

# Install Python and system dependencies #nodejs
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv curl build-essential && \ 
    pip3 install --upgrade pip



# Install dbt and DuckDB
RUN pip install requests dbt-core dbt-duckdb duckdb numpy ipykernel pandas


# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
