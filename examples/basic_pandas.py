from dagster import asset, Definitions
from dagster_mssql_bcp import PandasBCPIOManager
import pandas as pd

io_manager = PandasBCPIOManager(
    host="my_mssql_server",
    database="my_database",
    user="username",
    password="password",
    query_props={
        "TrustServerCertificate": "yes",
    },
    bcp_arguments={"-u": ""},
    bcp_path="/opt/mssql-tools18/bin/bcp",
)

@asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_asset(context):
    return pd.DataFrame({"id": [1, 2, 3]})


defs = Definitions(
    assets=[my_asset],
    io_managers={
        "io_manager": io_manager,
    },
)
