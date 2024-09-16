import os
from contextlib import contextmanager

import polars as pl
import pytest
from sqlalchemy import URL, create_engine, text

from dagster_mssql_polars import polars_mssql_bcp
from dagster_mssql_bcp_core.asset_schema import AssetSchema

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

    def test_evolution(self, polars_io: polars_mssql_bcp.PolarsBCP):
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
