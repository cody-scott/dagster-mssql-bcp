# dagster-mssql-bcp

ODBC is slow 🐢; bcp is fast! 🐰

This is a customer dagster IO manager for loading data into SQL Server using the `bcp` utility.

## What you need to run it

### BCP Utility

The `bcp` utility must be installed on the machine that is running the dagster pipeline.

See [Microsoft's documentation](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows) for more information.

Ideally you should place this on the path, but you can specify in the IO configuration where it is located.

### ODBC Drivers

You need the ODBC drivers installed on the machine that is running the dagster pipeline.

See [Microsoft's documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) for more information.

### Permissions

The user running the dagster pipeline must have the necessary permissions to load data into the SQL Server database. 

* `CREATE SCHEMA`
* `CREATE TABLES`

## Basic Usage

## Polars

```python
from dagster import asset, Definitions
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
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset(context):
    return pl.DataFrame({"id": [1, 2, 3]})


defs = Definitions(
    assets=[my_polars_asset],
    io_managers={
        "io_manager": io_manager,
    },
)

```

## Pandas

```python
from dagster import asset, Definitions
from dagster_mssql_bcp import PolarsBCPIOManager
import pandas as pd

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
        ],
        "schema": "my_schema",
    }
)
def my_polars_asset(context):
    return pl.DataFrame({"id": [1, 2, 3]})


defs = Definitions(
    assets=[my_polars_asset],
    io_managers={
        "io_manager": io_manager,
    },
)

```

The `asset schema` defines your table structure and your asset returns your data to load.

## Docs

For more details see [assets doc](docs/assets.md), [io manager doc](docs/io_manager.md), and for how its implemented, the [dev doc](docs/dev.md).