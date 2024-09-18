# IO Manager

The IO manager is a custom IO manager that loads data using BCP into MSSQL.

## Basic Usage

The IO manager requires the following arguments:

* `host`: str -> The host of the MSSQL server.
* `database`: str -> The database to load the data into.

You should also pass in the authentication requirements and other connection string and bcp properties you require to connect.

These are passed through the `query_props` dictionary and `bcp_arguments` dictionary. 

`query_props` are directly passed along to the pyodbc/sqlalchemy connection. 
[See the sqlalchemy docs for details](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.engine.URL.create)

`bcp_arguments` are passed along to the `bcp` command. [BCP doc link](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows#download-the-latest-version-of-the-bcp-utility)

The following example shows connecting using a username and password, trusting the server certificate and setting the path to the BCP tool, as its not on the path.

```python
from dagster_mssql_bcp import PandasBCPIOManager

io_manager = PandasBCPIOManager(
    host='my_mssql_server',
    database='my_database',
    user='username',
    password='password',
    query_props={
                "TrustServerCertificate": "yes",
    },
    bcp_arguments={
        '-u': ''
    },
    bcp_path="/opt/mssql-tools18/bin/bcp",
)

{'io_manager': io_manager}
```

The IO manager supports all partition arguments from dagster. Columns are passed in as `partition_expr` as a string or a dict of strings.

## BCP Data Requirements

The BCP tool requires the data to be in a specific structure. This includes...

* Datetimes as `YYYY-MM-DD HH:MM:SS.SSS+00:00`. Example `2020-01-01 00:00:00.000+00:00`
    * UTC should **not** be `+Z`, as this will cause an error. Should be `+00:00`
* `NULLS` or `blanks` should be empty strings
* Numbers should be in the format `123456` not `123,456`
* `Tabs` and `new lines` removed before loading.

These are all handled by the IO manager automatically for you, but you can optionally disabled if you wish, such as if your data is already in the correct format.

* process_datetime: bool -> Flag to process the datetime columns. Default is `True`.
* process_replacements: bool -> Flag to process the replacements. Default is `True`.

These can also be added as arguments to the metadata dictionary in the asset function.

```python
metadata={
    'process_datetime': False,
    'process_replacements': False
}
```

## Arguments

### connection arguments

These include the host, database, user, password, driver and query_props. These are passed along to SQLAlchemy/pyodbc to connect to the MSSQL server. This is used to create the intial tables, schemas and connections.

* `host`: str -> The host of the MSSQL server.
* `database`: str -> The database to load the data into.
* `user`: str -> The username to connect to the MSSQL server.
* `password`: str -> The password to connect to the MSSQL server.
* `driver`: str -> The driver to use to connect to the MSSQL server. Default is `ODBC Driver 18 for SQL Server`.
* `query_props`: Dict[str, Any] -> The properties to pass to the pyodbc/sqlalchemy connection.


### bcp arguments

The `bcp_arguments` dictionary is passed along to the `bcp` command. [BCP doc link](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows#download-the-latest-version-of-the-bcp-utility)

If your BCP tool is not on your `path`, then you can set the path to the BCP tool using the `bcp_path` argument.

* `bcp_arguments`: Dict[str, str] -> The arguments to pass to the BCP command.
* `bcp_path`: str -> The path to the BCP tool. If not on the path, you must set this.


### meta column arguments

These are the arguments that are added to the data when loaded. These are used to track the data as it is loaded into the database. These are to help identify the loads and rows that are loaded from the job. Setting the `add_*` flags to `False` will disable the addition of these columns.

* `add_row_hash`: bool -> Flag to add a hash of the row to the data. Default is `True`.
* `add_load_uuid`: bool -> Flag to add a load UUID to the data. Default is `True`.
* `add_load_datetime`: bool -> Flag to add a load date to the data. Default is `True`.

* `row_hash_column_name`: str -> The name of the column to store the row hash in. Default is `row_hash`.
* `load_uuid_column_name`: str -> The name of the column to store the load UUID in. Default is `load_uuid`.
* `load_datetime_column_name`: str -> The name of the column to store the load datetime in. Default is `load_datetime`.

```python
metadata={
    'add_row_hash': False,
    'add_load_uuid': False,
    'add_load_datetime': False,
}
```

## Additional Details

This is built around username/password. If you connect using other arguments (tokens, etc.) you should pass these through the `query_props` dictionary and the bcp arguments as needed.
