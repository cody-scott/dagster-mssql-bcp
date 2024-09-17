from datetime import datetime

import pandas as pd
import pytest
from dagster import (
    OutputContext,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
)
from pytest_mock import MockerFixture
from sqlalchemy import Connection

from dagster._core.definitions.time_window_partitions import TimeWindow
import pendulum

from dagster_mssql_bcp.bcp_core import utils


@pytest.fixture
def output_context__time_partition(mocker: MockerFixture):
    mp = DailyPartitionsDefinition("2020-01-01")
    mock = mocker.MagicMock(spec=OutputContext)
    mock.asset_partitions_def = mp
    mock.has_asset_partitions = True
    mock.has_partition_key = True
    mock.metadata = {"partition_expr": "sample_field"}
    mock.definition_metadata = {"partition_expr": "sample_field"}
    mock.partition_key = "sample_key"
    mock.asset_partitions_time_window = [datetime(2020, 1, 1), datetime(2020, 1, 2)]

    yield mock


@pytest.fixture
def output_context__static_partition(mocker: MockerFixture):
    mp = StaticPartitionsDefinition(["p1"])
    mock = mocker.MagicMock(spec=OutputContext)
    mock.asset_partitions_def = mp
    mock.has_asset_partitions = True
    mock.has_partition_key = True
    mock.metadata = {"partition_expr": "sample_field"}
    mock.definition_metadata = {"partition_expr": "sample_field"}
    mock.partition_key = "sample_key"
    yield mock


@pytest.fixture
def output_context__static_partition_with_quote(mocker: MockerFixture):
    mp = StaticPartitionsDefinition(["p1"])
    mock = mocker.MagicMock(spec=OutputContext)
    mock.asset_partitions_def = mp
    mock.has_asset_partitions = True
    mock.has_partition_key = True
    mock.metadata = {"partition_expr": "sample_field"}
    mock.definition_metadata = {"partition_expr": "sample_field"}
    mock.partition_key = "sample_key'with_quote'"
    yield mock


@pytest.fixture
def output_context__multi_partition(mocker: MockerFixture):
    mp = MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition("2020-01-01"),
            "static": StaticPartitionsDefinition(["p1"]),
        }
    )

    mock = mocker.MagicMock(spec=OutputContext)

    mock.has_asset_partitions = True

    mock.asset_partitions_def = mp
    mock.asset_partitions_time_window = TimeWindow(
        start=pendulum.datetime(2020, 1, 1), end=pendulum.datetime(2020, 1, 2)
    )
    mock.asset_partition_key.keys_by_dimension = {
        "date": "2020-01-01'",
        "static": "static_key",
    }
    mock.metadata = {"partition_expr": {"date": "date_field", "static": "static_field"}}
    mock.definition_metadata = {
        "partition_expr": {"date": "date_field", "static": "static_field"}
    }
    yield mock


@pytest.fixture
def output_context__no_partitions_mock(mocker: MockerFixture):
    mock = mocker.MagicMock(spec=OutputContext)
    mock.has_asset_partitions = False
    mock.has_partition_key = False
    return mock


@pytest.fixture
def connection_mock(mocker: MockerFixture):
    mock = mocker.MagicMock(spec=Connection)
    return mock


def test__static_where_clause():
    input_value = "hello"
    input_column = "sample"
    target = f"""{input_column} LIKE '{input_value}'"""
    result = utils._static_where_clause(input_value, input_column)
    assert result == target


def test___time_window_where_clause():
    _start = datetime(2020, 1, 1)
    _end = datetime(2021, 1, 1)
    datetime_format = "%Y-%m-%d %H:%M:%S"
    time_column = "sample"
    target = f"""{time_column} >= '{_start.strftime(datetime_format)}' AND {time_column} < '{_end.strftime(datetime_format)}'"""
    result = utils._time_window_where_clause((_start, _end), time_column)
    assert result == target


def test__calculate_max_chunksize():
    df = pd.DataFrame(list(range(5000)), columns=["data"])
    df["data2"] = df["data"]
    # two columns, 5000 rows means a chunk of 1000 over 5 rounds of inserts
    target = 1000
    result = utils.calculate_max_chunk(df)
    assert target == result

    df["data3"] = df["data"]
    target = 666
    result = utils.calculate_max_chunk(df)
    assert target == result

    df = pd.DataFrame()
    target = 1000
    result = utils.calculate_max_chunk(df)
    assert target == result


def test_static___partition_where_clause(output_context__static_partition):
    target = "WHERE sample_field LIKE 'sample_key'"
    result = utils._partition_where_clause(output_context__static_partition)
    assert target == result


def test_static___partition_where_clause_with_quote(
    output_context__static_partition_with_quote,
):
    target = "WHERE sample_field LIKE 'sample_key''with_quote'''"
    "sample_key'with_quote'"
    result = utils._partition_where_clause(output_context__static_partition_with_quote)
    assert target == result


def test_time__partition_where_clause(output_context__time_partition):
    target = "WHERE sample_field >= '2020-01-01 00:00:00' AND sample_field < '2020-01-02 00:00:00'"
    result = utils._partition_where_clause(output_context__time_partition)
    assert target == result


def test_multi__partition_where_clause(output_context__multi_partition):
    ctx = output_context__multi_partition
    target = "WHERE date_field >= '2020-01-01 00:00:00' AND date_field < '2020-01-02 00:00:00' AND static_field LIKE 'static_key'"
    result = utils._partition_where_clause(ctx)
    assert target == result


def test_no_partitions__partition_where_clause(output_context__no_partitions_mock):
    assert utils._partition_where_clause(output_context__no_partitions_mock) == ""


def test_time__get_cleanup_statement(output_context__time_partition):
    table_name = "table"
    schema_name = "schema"
    target = f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'schema' AND TABLE_NAME = 'table') DELETE FROM {schema_name}.{table_name} WHERE sample_field >= '2020-01-01 00:00:00' AND sample_field < '2020-01-02 00:00:00'"
    result = utils.get_cleanup_statement(
        table_name, schema_name, output_context__time_partition
    )
    assert target == result


def test_no_partition__get_cleanup_statement(output_context__no_partitions_mock):
    table_name = "table"
    schema_name = "schema"
    target = f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'schema' AND TABLE_NAME = 'table') DELETE FROM {schema_name}.{table_name}"
    result = utils.get_cleanup_statement(
        table_name, schema_name, output_context__no_partitions_mock
    )
    assert target == result


def test___get_select_statement(
    output_context__no_partitions_mock, output_context__time_partition
):
    target = "SELECT * FROM schema.table"
    result = utils.get_select_statement(
        "table", "schema", output_context__no_partitions_mock 
    )
    assert target == result

    target += " WHERE sample_field >= '2020-01-01 00:00:00' AND sample_field < '2020-01-02 00:00:00'"
    result = utils.get_select_statement(
        "table", "schema", output_context__time_partition
    )
    assert target == result


def test__special_character_search():
    # Test case 1: Value without special characters
    value = "Hello World"
    assert utils._special_character_search(value) is None

    # Test case 2: Value with special characters
    value = "Hello; World"
    with pytest.raises(AssertionError) as ae:
        utils._special_character_search(value)
    assert (
        str(ae.value)
        == f"Value {value} contains special characters like ; or (). Cannot process..."
    )

    # Test case 3: Value with special characters
    value = "Hello() World"
    with pytest.raises(AssertionError) as ae:
        utils._special_character_search(value)
    assert (
        str(ae.value)
        == f"Value {value} contains special characters like ; or (). Cannot process..."
    )

    # Test case 4: Value with special characters
    value = "Hello' World"
    assert utils._special_character_search(value) is None

    # Test case 5: Value with special characters
    value = 'Hello" World'
    assert utils._special_character_search(value) is None

    # Test case 6: Value with path
    value = "a/v/c.excel"
    assert utils._special_character_search(value) is None
