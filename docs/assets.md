# Assets

Loading data using BCP requires, at minimum, two items in addition to the IO manager resource.

1. Data to load from your `asset` function. If using pandas or polars this will be a dataframe.
2. An `Asset Schema` that describes the data you are loading.

## Asset Function

Create a standard dagster asset function like normal. The dataframe should be returned from the function.

The arguments for BCP should be added to `metadata` as a dictionary. The dictionary should have the following keys:

* `asset_schema`: List[Dict[str, Any]] -> The schema of the data you are loading. See below for more information.
* `table`: str | None -> The name of the table to load the data into. This is optional. If skipped then it will use the name of the asset function.
* `schema`: str | None -> The schema of the table to load the data into. This is optional. If skipped it will use the key_prefix of the asset function. If no key_prefix present, `dbo` will be used.

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

* `name`: str -> The name of the column in the data.
* `alias`: str | None -> Optional alias to rename the column to. If not present, the column will not be renamed and `name` will be applied.
* `type`: str -> The SQL type of the column in the data, such as `NVARCHAR`, `BIGINT`, `FLOAT`, `DATETIME2`, etc.
* `length`: int -> If the column is a string, the length of the string. If not a string, this key is ignored. Default is `MAX`
* `precision`: int -> If the column is a `DECIMAL` or `NUMERIC`, the precision of the number. If not a number, this key is ignored. Default is `18`.
* `scale`: int -> If the column is a `DECIMAL` or `NUMERIC`, the scale of the number. If not a number, this key is ignored. Default is `0`.
* `hash`: bool -> Flag if this should be included in the hash of the row. If `False`, the column will not be included in the hash. If not present, the column will be included in the hash. Default/Ommitted is `True`
* `identity`: bool -> Flag if this column is an identity column. If `True`, the column will be set to `IDENTITY(1,1)`. If not present, the column will not be an identity column. Default/Ommitted is `False`. When using an identity the column may or may not appear in the data. If it does, it will be ignored. Should be an integer based type.

```python
# example

asset_schema = [
    {'name': 'column_a', 'type': 'INT'}
    {'name': 'column_b', 'type': 'NVARCHAR', length: 20}
    {'name': 'column_c', 'type': 'DECIMAL', precision: 18, scale: 3}
    {'name': 'MyColumn', 'alias': 'column_d' 'type': 'DATETIME2'}
]

```

### About loading

Only the columns defined in your asset schema will be loaded.

This means:

1. If you have a column in your data that is not in the schema, it will not be loaded.
2. If you have a column in SQL that is not in the schema, it will not be loaded.
3. If a column in your schema stops being sent, the load fails.

This assumes that what you define is **at minimum** what is sent. 
You are saying *"I expect column X"* by defining an entry in the schema. If it no longer appears in the delivery, the load fails.

The input data and SQL tables can be be modified by adding new columns to either set. This sets a good boundary for schema migrations in the DB prior to schema migrations in the `asset schema`.