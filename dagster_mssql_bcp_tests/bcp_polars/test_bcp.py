import os
from contextlib import contextmanager

import polars as pl
import pytest
from sqlalchemy import URL, create_engine, text

from dagster_mssql_bcp.bcp_polars import polars_mssql_bcp
from dagster_mssql_bcp.bcp_core.asset_schema import AssetSchema

import polars.testing as pl_testing

import pendulum
import tempfile
import datetime


class TestPolarsBCP:
    @pytest.fixture
    def polars_io(self):
        return polars_mssql_bcp.PolarsBCP(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
            query_props={
                "TrustServerCertificate": "yes",
            },
            bcp_arguments={
                '-u': '',
                '-b': 20
            },
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

    def test_bcp_load(self, polars_io: polars_mssql_bcp.PolarsBCP):
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
        df = pl.DataFrame(
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
        polars_io.load_bcp(df, schema, table, asset_schema)

        df = df.with_columns(e = pl.Series([1, 2, 3]))
        polars_io.load_bcp(df, schema, table, asset_schema)

        with self.connect_mssql() as con:
            con.execute(text(f"ALTER TABLE {schema}.{table} ADD e INT"))

        polars_io.load_bcp(df, schema, table, asset_schema)

        asset_schema.add_column(
            {"name": "e", "type": "BIGINT"},
        )
        polars_io.load_bcp(df, schema, table, asset_schema)

    def test_bcp_load_alternative_column_names(self):
        polars_io = polars_mssql_bcp.PolarsBCP(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
            query_props={
                "TrustServerCertificate": "yes",
            },
            bcp_arguments={
                '-u': '',
                '-b': 20
            },
            bcp_path="/opt/mssql-tools18/bin/bcp",
            load_datetime_column_name='dt_col',
            load_uuid_column_name='uuid_col',
            row_hash_column_name='hash_col',
        )
        
        schema = "test"
        table = "table_data_alt_cols"
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
        df = pl.DataFrame(
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
        polars_io.load_bcp(df, schema, table, asset_schema)

        df = df.with_columns(e = pl.Series([1, 2, 3]))
        polars_io.load_bcp(df, schema, table, asset_schema)

        with self.connect_mssql() as con:
            con.execute(text(f"ALTER TABLE {schema}.{table} ADD e INT"))

        polars_io.load_bcp(df, schema, table, asset_schema)

        asset_schema.add_column(
            {"name": "e", "type": "BIGINT"},
        )
        polars_io.load_bcp(df, schema, table, asset_schema)


    def test_validate_columns(self, polars_io):
        result = polars_io._validate_columns(
            ["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"]
        )
        assert result == {}

        result = polars_io._validate_columns(
            ["a", "b", "c"], ["a", "b", "c"], ["a", "b"]
        )
        expect = {
            "Columns in DataFrame that are not in SQL": ["c"],
            "Columns in Schema that are not in SQL": ["c"],
        }
        assert result == expect

        with pytest.raises(ValueError) as ve:
            polars_io._validate_columns(["b", "c"], ["a", "b", "c"], ["a", "b", "c"])
        assert (
            str(ve.value)
            == "Columns in SQL that are not in DataFrame:\na\n\nColumns in Schema that are not in DataFrame:\na"
        )

        with pytest.raises(ValueError) as ve:
            polars_io._validate_columns(["b"], ["a", "b", "c"], ["c"])
        assert (
            str(ve.value)
            == "Columns in SQL that are not in DataFrame:\nc\n\nColumns in DataFrame that are not in SQL:\nb\n\nColumns in Schema that are not in SQL:\na\nb\n\nColumns in Schema that are not in DataFrame:\na\nc"
        )

        with pytest.raises(ValueError) as ve:
            polars_io._validate_columns(["b"], ["a", "b", "c"], None)
        assert str(ve.value) == "Columns in Schema that are not in DataFrame:\na\nc"

        polars_io._validate_columns(["b"], ["b"])

    def test_add_metadata(self, polars_io):
        uuid = "1234"
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._add_meta_columns(df, uuid_value=uuid)
        assert df.columns == ["a", "b", "c", "row_hash", "load_uuid", "load_datetime"]

        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=True, add_datetime=True
        )
        assert df.columns == ["a", "b", "c", "row_hash", "load_uuid", "load_datetime"]

        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=True, add_datetime=False
        )
        assert df.columns == ["a", "b", "c", "row_hash", "load_uuid"]

        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=True, add_uuid=False, add_datetime=False
        )
        assert df.columns == ["a", "b", "c", "row_hash"]

        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._add_meta_columns(
            df, uuid_value=uuid, add_hash=False, add_uuid=False, add_datetime=False
        )
        assert df.columns == ["a", "b", "c"]

    def test_reorder_columns(self, polars_io):
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
        df = polars_io._reorder_columns(df, ["b", "c", "a"])
        assert df.columns == ["b", "c", "a"]

    def test_replace_values(self, polars_io):
        df = pl.DataFrame(
            {"a": ["1,000", "2", "3"], "b": [4000, 5, 6], "c": ["a", "b", "c"]}
        )
        expected = df = pl.DataFrame(
            {"a": ["1000", "2", "3"], "b": [4000, 5, 6], "c": ["a", "b", "c"]}
        )
        schema = polars_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = polars_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

        df = pl.DataFrame(
            {"c": ["nan", "NAN", "c", "abc\tdef", "abc\t\ndef", "abc\ndef", "nan", "somenanthing"]}
        )
        expected = df = pl.DataFrame(
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
        schema =  polars_mssql_bcp.AssetSchema(
            [
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = polars_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

        df = pl.DataFrame(
            {
                "c": [
                    "2021-01-01",
                    "2021-01-01 05:00:00",
                    # "2021-01-01 00:00:00-05:00",
                ],
                "d": ["2021-01-01 00:00:00-05:00", "2021-01-01 00:00:00-05:00"],
            }
        )
        expected = pl.DataFrame(
            {
                "c": [
                    "2021-01-01",
                    "2021-01-01 05:00:00",
                    # "2021-01-01 00:00:00-05:00",
                ],
                "d": ["2021-01-01 00:00:00-05:00", "2021-01-01 00:00:00-05:00"],
            }
        )

        schema = polars_mssql_bcp.AssetSchema(
            [
                {"name": "c", "type": "DATETIME2"},
                {"name": "d", "type": "DATETIME2"},
            ]
        )
        df = polars_io._replace_values(df, schema)
        pl_testing.assert_frame_equal(df, expected)

        schema = [{"name": "a", "type": "BIT"}]
        df = pl.DataFrame({"a": [True, True, False]})
        df = polars_io._replace_values(df, schema)
        expected = pl.DataFrame({"a": [1, 1, 0]})
        pl_testing.assert_frame_equal(df, expected)

    def test_process_datetime(self, polars_io: polars_mssql_bcp.PolarsBCP):
        input = pl.DataFrame(
            {
                "a": ["2021-01-01", "2021-01-01 05:00:00", "2021-01-01 10:00:00"],
                "b": [4, 5, 6],
                "c": [
                    "2021-01-01 00:00:00+05:00",
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 00:00:00-05:00",
                ],
                "d": [
                    "2021-01-01 00:00:00-05:00",
                    "2021-01-01 00:00:00-05:00",
                    "2021-01-01 00:00:00-05:00",
                ],
            }
        )
        expected = pl.DataFrame(
            {
                "a": [
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 05:00:00+00:00",
                    "2021-01-01 10:00:00+00:00",
                ],
                "b": [4, 5, 6],
                "c": [
                    "2020-12-31 19:00:00+00:00",
                    "2021-01-01 00:00:00+00:00",
                    "2021-01-01 05:00:00+00:00",
                ],
                "d": [
                    "2021-01-01 00:00:00-05:00",
                    "2021-01-01 00:00:00-05:00",
                    "2021-01-01 00:00:00-05:00",
                ],
            }
        )
        schema = polars_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "DATETIME2"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "DATETIME2"},
            ]
        )
        df = polars_io._process_datetime(input, schema)
        pl_testing.assert_frame_equal(df, expected)


        schema = polars_mssql_bcp.AssetSchema(
            [
                {'name': 'a', 'type': 'DATETIME2'},
            ]
        )

        input = pl.date_range(datetime.date(2021,1,1), datetime.date(2021,1,3), eager=True).alias('a').to_frame()
        df = polars_io._process_datetime(input, schema)
        expected = pl.DataFrame(
            {'a': ["2021-01-01 00:00:00+00:00", "2021-01-02 00:00:00+00:00", "2021-01-03 00:00:00+00:00"]}
        )
        pl_testing.assert_frame_equal(df, expected)

    def test_save_csv(self, polars_io):
        with tempfile.TemporaryDirectory() as dir:
            polars_io._save_csv(
                pl.DataFrame(
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

    def test_get_sql_columns(self, polars_io):
        with self.connect_mssql() as conn:
            sql = """
            CREATE TABLE test.polars_test_sql_columns (
                a BIGINT,
                b BIGINT,
                c NVARCHAR(50)
            )
            """
            conn.execute(text(sql))
            columns = polars_io._get_sql_columns(conn, "test", "polars_test_sql_columns")
            assert columns == ["a", "b", "c"]
            self.cleanup_table(conn, "test", "polars_test_sql_columns")

            columns = polars_io._get_sql_columns(conn, "test", "polars_test_sql_columns")
            assert columns is None

    def test_create_table(self, polars_io):
        base_schema = polars_mssql_bcp.AssetSchema(
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
            schema = polars_mssql_bcp.AssetSchema(base_schema.schema[:])
            schema.add_column(row_hash_col)
            schema.add_column(load_uuid_col)
            schema.add_column(load_datetime_col)

            polars_io._create_table(
                conn,
                "test",
                "pandas_test_create_table",
                schema.get_sql_columns(),
            )
            columns = polars_io._get_sql_columns(
                conn, "test", "pandas_test_create_table"
            )
            assert columns == ["a", "b", "c", "row_hash", "load_uuid", "load_datetime"]
            self.cleanup_table(conn, "test", "pandas_test_create_table")

            schema = polars_mssql_bcp.AssetSchema(base_schema.schema[:])
            schema.add_column(load_uuid_col)
            schema.add_column(load_datetime_col)
            polars_io._create_table(conn, "test", "pandas_test_create_table", schema.get_sql_columns())
            columns = polars_io._get_sql_columns(
                conn,
                "test",
                "pandas_test_create_table",
            )
            assert columns == ["a", "b", "c", "load_uuid", "load_datetime"]
            self.cleanup_table(conn, "test", "pandas_test_create_table")


    def test_row_hash(self, polars_io: polars_mssql_bcp.PolarsBCP):
        with self.connect_mssql() as conn:
            schema = 'test'
            table = 'polars_test_row_hash'

            create_schema_sql = f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
            BEGIN
                EXEC('CREATE SCHEMA {schema}')
            END
            """

            drop_table_sql = f"""
                DROP TABLE IF EXISTS {schema}.{table}
                """

            create_table_sql = f"""
                CREATE TABLE {schema}.{table} (
                    a INT,
                    b INT,
                    c NVARCHAR(1)
                )
                """

            insert_sql = f"""
                INSERT INTO {schema}.{table} (a, b, c)
                VALUES
                (1, 4, 'a'),
                (2, 5, 'b'),
                (3, 6, 'c')
                """

            conn.execute(text(create_schema_sql))
            conn.execute(text(drop_table_sql))
            conn.execute(text(create_table_sql))
            conn.execute(text(insert_sql))

            conn.execute(
                text("ALTER TABLE test.polars_test_row_hash ADD row_hash NVARCHAR(200)")
            )
            polars_io._calculate_row_hash(
                conn, "test", "polars_test_row_hash", ["a", "b", "c"]
            )

            c = conn.execute(text("SELECT row_hash FROM test.polars_test_row_hash"))
            results = c.fetchall()
            assert results == [
                ("EEB6ACC06A96DD0FBA6B48CF398D19CD",),
                ("E66363C52F94C0F3B94049AE2A9FC020",),
                ("2590BB270F856656070045CCE1AD9B69",),
            ]
            self.cleanup_table(conn, "test", "polars_test_row_hash")

    def test_filter_columns(self, monkeypatch, polars_io: polars_mssql_bcp.PolarsBCP):
        df = pl.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": ["a", "b", "c"],
                "d": [1.1, 2.2, 3.3],
            }
        )
        schema = polars_mssql_bcp.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR", "length": 50},
            ]
        )
        df = polars_io._filter_columns(df, schema.get_columns())
        assert df.columns == ["a", "b", "c"]
