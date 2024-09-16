from dagster_mssql_polars import polars_mssql_io_manager
import os

from contextlib import contextmanager
from sqlalchemy import create_engine, URL, text
from dagster import build_output_context
import polars as pl

class TestPolarsBCPIO:
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
        return polars_mssql_io_manager.PolarsBCPIOManager(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
            query_props={
                "TrustServerCertificate": "yes",
            },
            bcp_arguments={
                '-u': ''
            },
            bcp_path="/opt/mssql-tools18/bin/bcp",
        )

    def test_evolution(self):
        # setup
        schema = "test_polars_bcp_schema"
        table = "test_polars_bcp_table_io_handle_output"

        create_schema = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """

        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.exec_driver_sql(create_schema)
            connection.exec_driver_sql(drop)
            connection.exec_driver_sql(drop + "_old")

        io_manager = self.io()

        # original structure
        data = pl.DataFrame(
            {
                "a": [1, 1, 1],
                "b": ["2", "2", "2"],
                "c": ["a", "a", "a"],
            }
        )
        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT", "identity": True},
            {"name": "b", "type": "NVARCHAR", "length": 10},
            {"name": "c", "type": "NVARCHAR", "length": 10},
        ]

        # first run
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

        # second run
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

        # add a column to delivery
        data = pl.DataFrame(
            {
                "a": [1, 1, 1],
                "b": ["2", "2", "2"],
                "c": ["a", "a", "a"],
                "z": ["z", "z", "z"],
            }
        )
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

        # add the column to table but dont update schema. Table should have column but not be filled.
        with self.connect_mssql() as connection:
            connection.execute(text(f"ALTER TABLE {schema}.{table} ADD z NVARCHAR(10)"))

        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

        # add column to schema - next run should fill data in
        asset_schema += [{"name": "z", "type": "NVARCHAR", "length": 10}]

        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

        with self.connect_mssql() as connection:
            connection.execute(
                text(f"""EXEC sp_rename '{schema}.{table}', '{table}_old'""")
            )
            connection.execute(
                text(
                    f"""
                SELECT
                    b,
                    load_timestamp,
                    a,
                    c,
                    z,
                    row_hash,
                    load_uuid
                INTO
                    {schema}.{table}
                FROM
                    {schema}.{table}_old
                """
                )
            )
            connection.execute(text(f"DROP TABLE {schema}.{table}_old"))
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)
