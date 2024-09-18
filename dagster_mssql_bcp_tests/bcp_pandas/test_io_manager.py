
from dagster_mssql_bcp.bcp_pandas import pandas_mssql_io_manager
import os

from contextlib import contextmanager
from sqlalchemy import create_engine, URL, text
from dagster import build_output_context, asset, DailyPartitionsDefinition, StaticPartitionsDefinition, materialize
import pandas as pd


class TestPandasBCPIO:
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
        return pandas_mssql_io_manager.PandasBCPIOManager(
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

    def test_handle_output_basic(self):
        # setup
        schema = "test_pandas_bcp_schema"
        table = "test_pandas_bcp_table_io_handle_output"

        create_schema = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """

        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(create_schema))
            connection.execute(text(drop))
            connection.execute(text(drop + "_old"))

        io_manager = self.io()

        # original structure
        data = pd.DataFrame(
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
        data = pd.DataFrame(
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
                    load_datetime,
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

    def test_handle_output_time_partition(self):
        schema = 'test_pandas_bcp_schema'
        table = 'my_pandas_asset_time_part'
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        io_manager = self.io()

        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT", "identity": True},
            {"name": "b", "type": "DATETIME2"},
        ]

        @asset(
            name=table,
            key_prefix=['data'],
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "partition_expr": 'b'
            },
            partitions_def=DailyPartitionsDefinition(
                start_date="2021-01-01", end_date="2021-01-03"
            )
        )
        def my_asset(context):
            return data
        
         # original structure
        data = pd.DataFrame(
            {
                "a": [1, 1],
                "b": ["2021-01-01", "2021-01-01"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )

        data = pd.DataFrame(
            {
                "a": [1, 1],
                "b": ["2021-01-02", "2021-01-02"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-02",
            resources={"io_manager": io_manager},
        )

        data = pd.DataFrame(
            {
                "a": [2, 2, 2],
                "b": ["2021-01-01", "2021-01-01", '2021-01-02'],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )

    def test_handle_output_static_partition(self):
        schema = 'test_pandas_bcp_schema'
        table = 'my_pandas_asset_static_part'
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        io_manager = self.io()

        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT", "identity": True},
            {"name": "b", "type": "NVARCHAR", "length": 10},
        ]

        @asset(
            name=table,
            key_prefix=['data'],
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "partition_expr": 'b'
            },
            partitions_def=StaticPartitionsDefinition(
                ['a', 'b']
            )
        )
        def my_asset(context):
            return data
        
                # original structure
        data = pd.DataFrame(
            {
                "a": [1, 1],
                "b": ["a", "a"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="a",
            resources={"io_manager": io_manager},
        )
        data = pd.DataFrame(
            {
                "a": [1, 1],
                "b": ["b", "b"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="a",
            resources={"io_manager": io_manager},
        )

        data = pd.DataFrame(
            {
                "a": [1, 1, 2],
                "b": ["a", "a", 'a'],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="a",
            resources={"io_manager": io_manager},
        )


