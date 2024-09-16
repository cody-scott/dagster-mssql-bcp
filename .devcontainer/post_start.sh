# /bin/bash

/bin/bash /workspace/.devcontainer/setup_database.sh

cd /workspace/dagster_mssql_bcp_core
pip install -e ".[dev]"

cd /workspace/dagster_mssql_polars
pip install -e ".[dev]"

cd /workspace/dagster_mssql_pandas
pip install -e ".[dev]"
