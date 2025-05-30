# dagster-mssql-bcp

![Unit tests](https://github.com/cody-scott/dagster-mssql-bcp/actions/workflows/python-test.yml/badge.svg)


ODBC is slow 🐢 bcp is fast! 🐰

This is a custom dagster IO manager for loading data into SQL Server using the `bcp` utility.

## What you need to run it

### Pypi

![PyPI](https://img.shields.io/pypi/v/dagster-mssql-bcp?label=latest%20stable&logo=pypi)

`pip install dagster-mssql-bcp`

### BCP Utility

The `bcp` utility must be installed on the machine that is running the dagster pipeline.

See [Microsoft's documentation](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows) for more information.

Ideally you should place this on your `PATH`, but you can specify in the IO configuration where it is located.

### ODBC Drivers

You need the ODBC drivers installed on the machine that is running the dagster pipeline.

See [Microsoft's documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) for more information.

### Permissions

The user running the dagster pipeline must have the necessary permissions to load data into the SQL Server database. 

* `CREATE SCHEMA`
* `CREATE/ALTER TABLES`

## Basic Usage

## Polars

Polars processes as a `LazyFrame`. Either a `DataFrame` or `LazyFrame` can be provided as an output of your asset before its cast automatically to `lazy`

```python
from dagster import asset, Definitions
from dagster_mssql_bcp import PolarsBCPIOManager, PolarsBCPResource
import polars as pl

io_manager = PolarsBCPIOManager(
    resource=PolarsBCPResource(
        host="my_mssql_server",
        database="my_database",
        port='1433',
        username="username",
        password="password",
        query_props={
            "TrustServerCertificate": "yes",
        },
        bcp_arguments={"-u": ""},
        bcp_path="/opt/mssql-tools18/bin/bcp",
    )
)

@asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset(context):
    return pl.DataFrame({"id": [1, 2, 3]})


@asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset_lazy(context):
    return pl.LazyFrame({"id": [1, 2, 3]})

defs = Definitions(
    assets=[my_polars_asset, my_polars_asset_lazy],
    resources={
        "io_manager": io_manager,
    },
)

```

## Pandas

```python
from dagster import asset, Definitions
from dagster_mssql_bcp import PandasBCPIOManager, PandasBCPResource
import pandas as pd

io_manager = PandasBCPIOManager(
    resource=PandasBCPResource(
        host="my_mssql_server",
        database="my_database",
        port='1433',
        username="username",
        password="password",
        query_props={
            "TrustServerCertificate": "yes",
        },
        bcp_arguments={"-u": ""},
        bcp_path="/opt/mssql-tools18/bin/bcp",
    )
)


@asset(
    metadata={
        "asset_schema": [
            {"name": "id", "type": "INT"},
        ],
        "schema": "my_schema",
    }
)
def my_pandas_asset(context):
    return pd.DataFrame({"id": [1, 2, 3]})


defs = Definitions(
    assets=[my_pandas_asset],
    resources={
        "io_manager": io_manager,
    },
)

```

The `asset schema` defines your table structure and your asset returns your data to load.

## Docs

For more details see [assets doc](https://github.com/cody-scott/dagster-mssql-bcp/blob/main/docs/assets.md), [io manager doc](https://github.com/cody-scott/dagster-mssql-bcp/blob/main/docs/io_manager.md), and for how its implemented, the [dev doc](https://github.com/cody-scott/dagster-mssql-bcp/blob/main/docs/dev.md).
