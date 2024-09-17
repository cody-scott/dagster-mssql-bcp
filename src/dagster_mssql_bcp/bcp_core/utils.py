import re
from datetime import datetime
from typing import Tuple

from dagster import (
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    OutputContext,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)


def calculate_max_chunk(df):
    """SQL Server has a max chunk (rows*cols) of 2100, so control that here
    chunk size determines the number of rows it will do at once.
    therefore...
    2000 rows and 1 column means it can process 2000 rows at a time -> chunk size 2000
    2000 rows and 2 columns means it can process 1000 rows at a time -> chunk size 1000
    etc.
    """
    rows, cols = df.shape
    if cols == 0:
        return 1000

    max_params = 2000
    max_chunksize = max_params // cols

    return max_chunksize


def get_select_statement(table: str, schema: str, context, columns: list[str] = None) -> str:
    where_clause = _partition_where_clause(context)
    if where_clause is None:
        where_clause = ""

    if columns is not None:
        columns_str = ", ".join(columns)
    else:
        columns_str = "*"

    sql = f"SELECT {columns_str} FROM {schema}.{table} {where_clause}"
    return sql.strip()


def get_cleanup_statement(table: str, schema: str, context: OutputContext) -> str:
    """Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    exist_block = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
    if context.has_partition_key or context.has_asset_partitions:
        part_txt = _partition_where_clause(context)
        delete_block = f"DELETE FROM {schema}.{table} {part_txt}".strip()
    else:
        delete_block = f"DELETE FROM {schema}.{table}".strip()

    return f"IF EXISTS ({exist_block}) {delete_block}"


def _partition_where_clause(context: OutputContext):
    if not context.has_asset_partitions:
        return ""

    if context.definition_metadata is None:
        return ""

    assert context.definition_metadata.get("partition_expr") is not None

    results = []
    if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
        # this stores the column names
        partition_expr = context.definition_metadata.get("partition_expr", {})

        # this stores the reference to the actual partition. Useful for static
        keys = context.asset_partition_key.keys_by_dimension  # type: ignore
        for _ in context.asset_partitions_def.partitions_defs:
            field_name = partition_expr.get(_.name)

            assert field_name is not None

            if isinstance(_.partitions_def, TimeWindowPartitionsDefinition):
                results.append(
                    _time_window_where_clause(
                        context.asset_partitions_time_window, field_name
                    )
                )
            else:
                static_partition_value = keys.get(_.name)
                results.append(_static_where_clause(static_partition_value, field_name))

    elif isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
        field_name = context.definition_metadata.get("partition_expr")
        assert isinstance(field_name, str)
        results.append(
            _time_window_where_clause(
                context.asset_partitions_time_window, str(field_name)
            )
        )

    elif isinstance(
        context.asset_partitions_def, StaticPartitionsDefinition
    ) or isinstance(context.asset_partitions_def, DynamicPartitionsDefinition):
        field_name = context.definition_metadata.get("partition_expr")
        results.append(_static_where_clause(context.partition_key, field_name))

    else:
        raise NotImplementedError("Partition definition type not defined")

    results_str = " AND ".join(results)
    final_where = f"WHERE {results_str}"
    return final_where


def _time_window_where_clause(
    time_window: Tuple[datetime, datetime], time_column: str
) -> str:
    """
    Generate the WHERE clause for a time window query.

    Args:
        time_window (Tuple[datetime, datetime]): The time window to filter on.
        time_column (str): The name of the time column in the database.

    Returns:
        str: The WHERE clause string.

    """
    start_dt, end_dt = time_window
    datetime_format = "%Y-%m-%d %H:%M:%S"
    return f"""{time_column} >= '{start_dt.strftime(datetime_format)}' AND {time_column} < '{end_dt.strftime(datetime_format)}'"""


def _static_where_clause(value, column):
    """
    Generate a static WHERE clause for SQL queries based on the given value and column.

    Args:
        value (str): The value to search for.
        column (str): The column to search in.

    Returns:
        str: The generated WHERE clause.

    """
    value = value.replace("'", "''")

    _special_character_search(value)

    return f"""{column} LIKE '{value}'"""


def _special_character_search(value):
    """
    Check if the given value contains any special characters.
    This is primarily to check for an instance of SQL injection.
    Args:
        value (str): The value to be checked.

    Raises:
        AssertionError: If the value contains special characters like ; or ().

    Returns:
        None
    """
    assert (
        re.search(r"""([^a-zA-z\/\.\_\d\s\-'"])""", value) is None
    ), f"Value {value} contains special characters like ; or (). Cannot process..."
