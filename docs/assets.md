# Assets

Loading data using BCP requires, at minimum, two items in addition to the IO manager resource.

1. Data to load from your `asset` function. If using pandas or polars this will be a dataframe.
2. An `Asset Schema` that describes the data you are loading.

## Asset Function

Decorate a standard dagster asset function like normal. The dataframe should be returned from the function.

The arguments for BCP should be added to `metadata` as a dictionary. The dictionary should have the following keys:

* `asset_schema`: List[Dict[str, Any]] -> The schema of the data you are loading. See below for more information.
* `table`: str -> The name of the table to load the data into. This is optional. If skipped then it will use the name of the asset function.
* `schema`: str -> The schema of the table to load the data into. This is optional. If skipped it will use the key_prefix of the asset function.

```python
# example
from dagster import asset
import pandas as pd

asset_schema = [
    {'name': 'first_column', 'type': 'INT'},
]

@asset(
    metadata={
        'asset_schema': asset_schema,
        'schema': 'my_schema'
    },
    key_prefix=['my','bcp']
)
def my_bcp_table():
    return pd.DataFrame({'first_column': [1, 2, 3]})

```

## Asset Schema

The `Asset Schema` is a list of dictionaries that describe the data you are loading. Each dictionary in the list should have the following keys:

* name: str -> The name of the column in the data.
* alias: str | None -> Optional alias to rename the column to. If not present, the column will not be renamed and `name` will be applied.
* type: str -> The SQL type of the column in the data, such as `NVARCHAR`, `BIGINT`, `FLOAT`, `DATETIME2`, etc.
* length: int -> If the column is a string, the length of the string. If not a string, this key is ignored. Default is `MAX`
* precision: int -> If the column is a `DECIMAL` or `NUMERIC`, the precision of the number. If not a number, this key is ignored. Default is `18`.
* scale: int -> If the column is a `DECIMAL` or `NUMERIC`, the scale of the number. If not a number, this key is ignored. Default is `0`.
* hash: bool -> Flag if this should be included in the hash of the row. If `False`, the column will not be included in the hash. If not present, the column will be included in the hash. Default/Ommitted is `True`

```python
# example

asset_schema = [
    {'name': 'column_a', 'type': 'INT'}
    {'name': 'column_b', 'type': 'NVARCHAR', length: 20}
    {'name': 'column_c', 'type': 'DECIMAL', precision: 18, scale: 3}
    {'name': 'MyColumn', 'alias': 'column_d' 'type': 'DATETIME2'}
]

```

### Notes about loading

Important that only the columns defined in your asset schema will be loaded.

This means.

1. If you have a column in your data that is not in the schema, it will not be loaded.
2. If you have a column in SQL that is not in the schema, it will not be loaded.

Additionally, this assumes that what you define is **at minimum** what is sent. 
If you say "I expect column X" by defining an entry in the schema, and it not longer appears in the delivery,
we fail the load.

You are then able to modify the delivery (as long as the same columns still are delivered) by addding new data.
Modify the SQL schema indepentant of the asset schema using liquibase or something else.

Add additional columns in SQL such as identity or other columns without breaking the job.
