from dagster_mssql_bcp.bcp_polars import polars_mssql_resource
import os

from contextlib import contextmanager
from sqlalchemy import create_engine, URL

import dagster as dg
import polars as pl

class TestPolarsBCPResource:
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

    def io(self):
        return polars_mssql_resource.PolarsBCPResource(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
            query_props={
                "TrustServerCertificate": "yes",
            },
            bcp_arguments={"-u": ""},
            bcp_path="/opt/mssql-tools18/bin/bcp",
        )
    
    def test_io(self):
        @dg.asset()
        def my_asset(context, polars_rsc: polars_mssql_resource.PolarsBCPResource):
            data = pl.DataFrame(
                {
                    "b": ["2021-01-01", "2021-01-01"],
                }
            )

            polars_rsc.load_bcp(
                data, 'dbo', 'tst', [{'name': 'b', 'type': 'DATETIME2'}]
            )
            return None
        
        dg.materialize(
            [my_asset],
            resources={'polars_rsc': self.io()}
        )