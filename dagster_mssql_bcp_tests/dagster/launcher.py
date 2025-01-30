import dagster as dg
from dagster_mssql_bcp import PolarsBCPIOManager
import polars as pl


io_manager = PolarsBCPIOManager(
    host=dg.EnvVar("TARGET_DB__HOST"),
    database=dg.EnvVar("TARGET_DB__DATABASE"),
    username=dg.EnvVar("TARGET_DB__USERNAME"),
    password=dg.EnvVar("TARGET_DB__PASSWORD"),
    port=dg.EnvVar("TARGET_DB__PORT"),
    query_props={
        "TrustServerCertificate": "yes",
    },
    bcp_arguments={"-u": ""},
    bcp_path="/opt/mssql-tools18/bin/bcp",
)


@dg.asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset(context):
    return pl.DataFrame({"id": [1, 2, 3]})


@dg.asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset_lazy(context):
    return pl.LazyFrame({"id": [1, 2, 3]})


defs = dg.Definitions(
    assets=[my_polars_asset, my_polars_asset_lazy],
    resources={
        "io_manager": io_manager,
    },
)
