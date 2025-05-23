import dagster as dg
from dagster_mssql_bcp import PolarsBCPIOManager, PolarsBCPResource
import polars as pl

polars_rsc = PolarsBCPResource(
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

io_manager = PolarsBCPIOManager(resource=polars_rsc)


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


@dg.asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset_resource(context, my_polars_resource: PolarsBCPResource):
    my_polars_resource.load_bcp(
        data=pl.LazyFrame({"id": [1, 2, 3]}),
        schema="my_schema",
        table="polars_resource_test",
        asset_schema=[
            {"name": "id", "type": "INT"},
        ],
    )


defs = dg.Definitions(
    assets=[my_polars_asset, my_polars_asset_lazy, my_polars_asset_resource],
    resources={"io_manager": io_manager, "my_polars_resource": polars_rsc},
)
