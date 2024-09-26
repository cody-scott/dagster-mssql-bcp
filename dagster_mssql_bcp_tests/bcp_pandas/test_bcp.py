import os
import tempfile
from contextlib import contextmanager

import pandas as pd
import pandas.testing as pl_testing
import pendulum
import pytest
from sqlalchemy import URL, create_engine, text

from dagster_mssql_bcp.bcp_core.asset_schema import AssetSchema
from dagster_mssql_bcp.bcp_pandas import pandas_mssql_bcp

from dagster_mssql_bcp.bcp_core import bcp_core
from unittest.mock import MagicMock


class TestPandasBCP:
    @pytest.fixture
    def pandas_io(self) -> pandas_mssql_bcp.PandasBCP:
        return pandas_mssql_bcp.PandasBCP(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
            query_props={
                "TrustServerCertificate": "yes",
            },
            bcp_arguments={"-u": "", "-b": 20},
            bcp_path="/opt/mssql-tools18/bin/bcp",
        )

    @contextmanager
    def connect_mssql(self):
        config = self.get_database_connection()
        connection_url = URL(
            "mssql+pyodbc",
            username=config.get("username"),
            password=config.get("password"),
            host=config.get("host"),
            port=int(config.get("port", "1433")),
            database=config.get("database"),
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
            },  # type: ignore
        )
        with create_engine(
            connection_url, fast_executemany=True, hide_parameters=True
        ).begin() as conn:
            yield conn

    def get_database_connection(self) -> dict[str, str]:
        db_config = dict(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
        )

        return db_config

    def cleanup_table(self, connection, schema, table):
        connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{table}"))

    def cleanup_tables(self, connection, tables):
        for schema, table in tables:
            self.cleanup_table(connection, schema, table)

    def test_bcp_load(self, pandas_io: pandas_mssql_bcp.PandasBCP):
        schema = "test"
        table = "table_data"
        with self.connect_mssql() as con:
            con.execute(text(f"DROP TABLE IF EXISTS {schema}.{table}"))

        asset_schema = AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR", "length": 50},
                {"name": "d", "type": "DATETIME2"},
            ]
        )
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": ["a", "b", "c"],
                "d": [
                    "2021-01-01 00:00:00",
                    "2021-02-01 00:00:00",
                    "2021-03-01 00:00:00",
                ],
            }
        )
        pandas_io.load_bcp(df, schema, table, asset_schema)

        df["e"] = [1, 2, 3]
        pandas_io.load_bcp(df, schema, table, asset_schema)

        with self.connect_mssql() as con:
            con.execute(text(f"ALTER TABLE {schema}.{table} ADD e INT"))

        pandas_io.load_bcp(df, schema, table, asset_schema)

        asset_schema.add_column(
            {"name": "e", "type": "BIGINT"},
        )
        pandas_io.load_bcp(df, schema, table, asset_schema)

    def test_validate_columns(self, pandas_io: pandas_mssql_bcp.PandasBCP):
        result = pandas_io._validate_columns(
            ["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"]
        )
        assert result == {}

        result = pandas_io._validate_columns(
            ["a", "b", "c"], ["a", "b", "c"], ["a", "b"]
        )
        expect = {
            "Columns in DataFrame that are not in SQL": ["c"],
            "Columns in Schema that are not in SQL": ["c"],
        }
        assert result == expect

        with pytest.raises(ValueError) as ve:
            pandas_io._validate_columns(["b", "c"], ["a", "b", "c"], ["a", "b", "c"])
        assert (
            str(ve.value)
            == "Columns in SQL that are not in DataFrame:\na\n\nColumns in Schema that are not in DataFrame:\na"
        )

        with pytest.raises(ValueError) as ve:
            pandas_io._validate_columns(["b"], ["a", "b", "c"], ["c"])
        assert (
            str(ve.value)
            == "Columns in SQL that are not in DataFrame:\nc\n\nColumns in DataFrame that are not in SQL:\nb\n\nColumns in Schema that are not in SQL:\na\nb\n\nColumns in Schema that are not in DataFrame:\na\nc"
        )

        with pytest.raises(ValueError) as ve:
            pandas_io._validate_columns(["b"], ["a", "b", "c"], None)
        assert str(ve.value) == "Columns in Schema that are not in DataFrame:\na\nc"

        pandas_io._validate_columns(["b"], ["b"])

    def test_add_metadata(self, pandas_io):
        uuid = "1234"
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._add_meta_columns(df, uuid_value=uuid)
        assert df.columns.tolist() == [
            "a",
            "b",
            "c",
            "row_hash",
            "load_uuid",
            "load_datetime",
        ]

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=True, add_datetime=True
        )
        assert df.columns.tolist() == [
            "a",
            "b",
            "c",
            "row_hash",
            "load_uuid",
            "load_datetime",
        ]

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=True, add_datetime=False
        )
        assert df.columns.tolist() == ["a", "b", "c", "row_hash", "load_uuid"]

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=False, add_datetime=False
        )
        assert df.columns.tolist() == ["a", "b", "c", "row_hash"]

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=False, add_uuid=False, add_datetime=False
        )
        assert df.columns.tolist() == ["a", "b", "c"]

    def test_reorder_columns(self, pandas_io):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = pandas_io._reorder_columns(df, ["b", "c", "a"])
        assert df.columns.tolist() == ["b", "c", "a"]

    def test_replace_values(self, pandas_io):
        df = pd.DataFrame(
            {"a": ["1,000", "2", "3"], "b": [4000, 5, 6], "c": ["a", "b", "c"]}
        )
        expected = df = pd.DataFrame(
            {"a": ["1000", "2", "3"], "b": [4000, 5, 6], "c": ["a", "b", "c"]}
        )
        schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = pandas_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

        df = pd.DataFrame(
            {"c": ["nan", "NAN", "c", "abc\tdef", "abc\t\ndef", "abc\ndef", "nan", "somenanthing"]}
        )
        expected = df = pd.DataFrame(
            {
                "c": [
                    "",
                    "",
                    "c",
                    "abc__TAB__def",
                    "abc__TAB____NEWLINE__def",
                    "abc__NEWLINE__def",
                    "",
                    "somenanthing"
                ]
            }
        )
        schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = pandas_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

        df = pd.DataFrame(
            {
                "c": [
                    "2021-01-01 00:00:00",
                    "2021-01-01 05:00:00",
                    # "2021-01-01 00:00:00-05:00",
                ],
                "d": ["2021-01-01 00:00:00-05:00", "2021-01-01 00:00:00-05:00"],
            }
        )
        expected = pd.DataFrame(
            {
                "c": [
                    "2021-01-01 00:00:00",
                    "2021-01-01 05:00:00",
                    # "2021-01-01 00:00:00-05:00",
                ],
                "d": ["2021-01-01 00:00:00-05:00", "2021-01-01 00:00:00-05:00"],
            }
        )

        schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "c", "type": "DATETIME2"},
                {"name": "d", "type": "DATETIME2"},
            ]
        )
        df = pandas_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

    def test_process_datetime(self, pandas_io: pandas_mssql_bcp.PandasBCP):
        input = pd.DataFrame(
            {
                "a": [
                    "2021-01-01 00:00:00",
                    "2021-01-01 05:00:00",
                    "2021-01-01 10:00:00",
                    "",
                ],
                "b": [4, 5, 6, 7],
                "c": [
                    "2021-01-01 00:00:00+05:00",
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 00:00:00-05:00",
                    "",
                ],
            }
        )
        expected = pd.DataFrame(
            {
                "a": [
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 05:00:00+00:00",
                    "2021-01-01 10:00:00+00:00",
                    "",
                ],
                "b": [4, 5, 6, 7],
                "c": [
                    "2021-01-01 00:00:00+05:00",
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 00:00:00-05:00",
                    "",
                ],
            }
        )

        schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "DATETIME2"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "DATETIME2"},
            ]
        )
        df = pandas_io._process_datetime(input, schema)
        pd.testing.assert_frame_equal(df, expected, check_index_type=False)

    def test_save_csv(self, pandas_io):
        with tempfile.TemporaryDirectory() as dir:
            pandas_io._save_csv(
                pd.DataFrame(
                    {
                        "a": [1, 2, 3],
                        "b": [4, 5, 6],
                        "c": ["a", "b", "c"],
                        "dt": [
                            pendulum.now(tz="America/Toronto"),
                            pendulum.now(tz="America/Toronto"),
                            pendulum.now(tz="America/Toronto"),
                        ],
                    }
                ),
                dir,
                "test.csv",
            )
            assert os.path.exists(os.path.join(dir, "test.csv"))

    def test_get_sql_columns(self, pandas_io):
        with self.connect_mssql() as conn:
            sql = """
            CREATE TABLE test.pandas_test_sql_columns (
                a BIGINT,
                b BIGINT,
                c NVARCHAR(50)
            )
            """
            conn.exec_driver_sql(sql)
            columns = pandas_io._get_sql_columns(
                conn, "test", "pandas_test_sql_columns"
            )
            assert columns == ["a", "b", "c"]
            self.cleanup_table(conn, "test", "pandas_test_sql_columns")

            columns = pandas_io._get_sql_columns(
                conn, "test", "pandas_test_sql_columns"
            )
            assert columns is None

    def test_create_table(self, pandas_io):
        base_schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "DATETIME2"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "DATETIME2"},
            ]
        )

        row_hash_col = {"name": "row_hash", "type": "NVARCHAR", "length": 200}
        load_uuid_col = {"name": "load_uuid", "type": "NVARCHAR", "length": 200}
        load_datetime_col = {"name": "load_datetime", "type": "DATETIME2"}

        with self.connect_mssql() as conn:
            schema = pandas_mssql_bcp.AssetSchema(base_schema.schema[:])
            schema.add_column(row_hash_col)
            schema.add_column(load_uuid_col)
            schema.add_column(load_datetime_col)

            pandas_io._create_table(
                conn,
                "test",
                "pandas_test_create_table",
                schema.get_sql_columns(),
            )
            columns = pandas_io._get_sql_columns(
                conn, "test", "pandas_test_create_table"
            )
            assert columns == ["a", "b", "c", "row_hash", "load_uuid", "load_datetime"]
            self.cleanup_table(conn, "test", "pandas_test_create_table")

            schema = pandas_mssql_bcp.AssetSchema(base_schema.schema[:])
            schema.add_column(load_uuid_col)
            schema.add_column(load_datetime_col)

            pandas_io._create_table(conn, "test", "pandas_test_create_table", schema.get_sql_columns())

            columns = pandas_io._get_sql_columns(
                conn,
                "test",
                "pandas_test_create_table",
            )
            assert columns == ["a", "b", "c", "load_uuid", "load_datetime"]
            self.cleanup_table(conn, "test", "pandas_test_create_table")

    def test_bcp_fail_print_late_column(
        self, monkeypatch, pandas_io: pandas_mssql_bcp.PandasBCP
    ):
        io = pandas_io
        logger_mock = MagicMock()
        logger_mock.error = MagicMock()
        pandas_mssql_bcp.get_dagster_logger = logger_mock
        bcp_core.get_dagster_logger = logger_mock

        asset_schema_obj = pandas_mssql_bcp.AssetSchema(
            [{"name": f"c_{_}", "type": "BIGINT"} for _ in range(0, 20)]
            + [
                {
                    "name": "column_that_is_too_long",
                    "type": "NVARCHAR",
                    "length": 5,
                }
            ]
            + [{"name": f"c_{_}", "type": "BIGINT"} for _ in range(30, 40)],
        )

        data_set = [1] * 20 + ["1234567890"] + [1] * 10

        df = pd.DataFrame(
            [data_set],
            columns=[f"c_{_}" for _ in range(0, 20)]
            + ["column_that_is_too_long"]
            + [f"c_{_}" for _ in range(30, 40)],
        )

        monkeypatch.setattr(bcp_core, "uuid4", lambda: "uuid_table")

        schema = "test_schema"
        table = "test_table_3_too_long2"

        with self.connect_mssql() as conn:
            self.cleanup_tables(
                conn,
                [(schema, table), (schema, "test_table_3_too_long2_uuid_table")],
            )

        with pytest.raises(RuntimeError) as e:
            io.load_bcp(
                data=df,
                schema=schema,
                table=table,
                asset_schema=asset_schema_obj,
            )

        assert logger_mock.mock_calls[-1].args == (
            "BCP error: #@ Row 1, Column 21: String data, right truncation @# - column name: column_that_is_too_long",
        )

        with self.connect_mssql() as conn:
            self.cleanup_tables(
                conn,
                [
                    (schema, table),
                    (schema, "test_table_3_too_long2_uuid_table"),
                    (schema, "test_table_3_too_long2_staging_uuid_table"),
                ],
            )

    def test_filter_columns(self, monkeypatch, pandas_io: pandas_mssql_bcp.PandasBCP):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": ["a", "b", "c"],
                "d": [1.1, 2.2, 3.3],
            }
        )
        schema = pandas_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = pandas_io._filter_columns(df, schema.get_columns())
        assert df.columns.tolist() == ["a", "b", "c"]
