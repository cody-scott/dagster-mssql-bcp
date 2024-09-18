from dagster import asset, Definitions, DailyPartitionsDefinition
from dagster_mssql_bcp import PolarsBCPIOManager
import polars as pl

io_manager = PolarsBCPIOManager(
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
            {"name": "datetime_column", "type": "DATETIME2"},
        ],
        "schema": "my_schema",
        "partition_expr": "datetime_column"
    },
    partition=DailyPartitionsDefinition(start_date="2022-01-01", end_date="2022-01-03"),
)
def my_asset(context):
    return pl.DataFrame({"id": [1, 2, 3], "datetime_column": ["2022-01-01", "2022-01-01", "2022-01-01"]})


defs = Definitions(
    assets=[my_asset],
    io_managers={
        "io_manager": io_manager,
    },
)
