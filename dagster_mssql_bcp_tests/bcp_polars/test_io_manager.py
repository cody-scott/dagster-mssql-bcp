from dagster_mssql_bcp.bcp_polars import polars_mssql_io_manager
from dagster_mssql_bcp.bcp_polars import polars_mssql_resource
import os

from contextlib import contextmanager
from sqlalchemy import create_engine, URL, text
from dagster import (
    build_output_context,
    materialize,
    asset,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
)
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

    def rsc(self):
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

    def io(self):
        return polars_mssql_io_manager.PolarsBCPIOManager(
            resource=self.rsc()
        )
    
    def io_stagingdb(self):
        rsc = polars_mssql_resource.PolarsBCPResource(
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
            staging_database="staging"
        )

        return polars_mssql_io_manager.PolarsBCPIOManager(
            resource=rsc
        )
    
    def io_identity(self):
        rsc = polars_mssql_resource.PolarsBCPResource(
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
            add_identity_column=True
        )

        return polars_mssql_io_manager.PolarsBCPIOManager(
            resource=rsc
        )

    def test_handle_output_basic(self):
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
                "c": ["a", "a", "Ö"],
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
                "c": ["a", "a", "Ö"],
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
            r = io_manager.handle_output(ctx, data)
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

    def test_handle_output_basic_stg(self):
        # setup
        io_manager = self.io_stagingdb()

        schema = "test_polars_bcp_schema"
        table = "test_polars_bcp_table_io_handle_output_stg"

        create_schema = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """

        drop = f"""DROP TABLE IF EXISTS {io_manager.resource.database}.{schema}.{table}"""
        use_sql = 'USE {db}'
        with self.connect_mssql() as connection:
            connection.exec_driver_sql(use_sql.format(db=io_manager.resource.staging_database))
            connection.exec_driver_sql(create_schema)

            connection.exec_driver_sql(use_sql.format(db=io_manager.resource.database))
            connection.exec_driver_sql(create_schema)
            
            connection.exec_driver_sql(drop)
            connection.exec_driver_sql(drop + "_old")


        # original structure
        data = pl.DataFrame(
            {
                "a": [1, 1, 1],
                "b": ["2", "2", "2"],
                "c": ["a", "a", "Ö"],
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
                "c": ["a", "a", "Ö"],
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
            connection.execute(text(f"ALTER TABLE {io_manager.resource.database}.{schema}.{table} ADD z NVARCHAR(10)"))

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
                    {io_manager.resource.database}.{schema}.{table}
                FROM
                    {io_manager.resource.database}.{schema}.{table}_old
                """
                )
            )
            connection.execute(text(f"DROP TABLE {io_manager.resource.database}.{schema}.{table}_old"))
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            r = io_manager.handle_output(ctx, data)
        with build_output_context(
            asset_key=[schema, table],
            definition_metadata={"asset_schema": asset_schema, "schema": schema},
        ) as ctx:
            io_manager.handle_output(ctx, data)

    def test_handle_output_time_partition(self):
        schema = "test_polars_bcp_schema"
        table = "asset_time_part"
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
            key_prefix=["data"],
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "partition_expr": "b",
            },
            partitions_def=DailyPartitionsDefinition(
                start_date="2021-01-01", end_date="2021-01-03"
            ),
        )
        def my_asset(context):
            return data

        # original structure
        data = pl.DataFrame(
            {
                # "a": [1, 1],
                "b": ["2021-01-01", "2021-01-01"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )

        data = pl.DataFrame(
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

        data = pl.DataFrame(
            {
                "a": [2, 2, 2],
                "b": ["2021-01-01", "2021-01-01", "2021-01-02"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )


    def test_handle_output_time_partition_staging(self):
        io_manager = self.io_stagingdb()

        schema = "test_polars_bcp_schema"
        table = "asset_time_part_stg"
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))


        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT", "identity": True},
            {"name": "b", "type": "DATETIME2"},
        ]

        @asset(
            name=table,
            key_prefix=["data"],
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "partition_expr": "b",
            },
            partitions_def=DailyPartitionsDefinition(
                start_date="2021-01-01", end_date="2021-01-03"
            ),
        )
        def my_asset(context):
            return data

        # original structure
        data = pl.DataFrame(
            {
                # "a": [1, 1],
                "b": ["2021-01-01", "2021-01-01"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )

        data = pl.DataFrame(
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

        data = pl.DataFrame(
            {
                "a": [2, 2, 2],
                "b": ["2021-01-01", "2021-01-01", "2021-01-02"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="2021-01-01",
            resources={"io_manager": io_manager},
        )

    def test_handle_output_static_partition(self):
        schema = "test_polars_bcp_schema"
        table = "my_polars_asset_static_part"
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
            key_prefix=["data"],
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "partition_expr": "b",
            },
            partitions_def=StaticPartitionsDefinition(["a", "b"]),
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
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
        data = pl.DataFrame(
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

        data = pl.DataFrame(
            {
                "a": [1, 1, 2],
                "b": ["a", "a", "a"],
            }
        )
        materialize(
            assets=[my_asset],
            partition_key="a",
            resources={"io_manager": io_manager},
        )

    def test_basic_no_extras(self):
        schema = "test_polars_bcp_schema"
        table = "basic_no_extra"
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
            metadata={
                "asset_schema": asset_schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
                "schema": schema
            },
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
            {
                "a": [1, 1],
                "b": ["a", "a"],
            }
        )
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )

    def test_basic_replacements(self):
        schema = "test_polars_bcp_schema"
        table = "test_basic_replacements"
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT"},
            {"name": "b", "type": "NVARCHAR", "length": 100},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
            },
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
            {
                "a": [1, 1, 1],
                "b": ["a\t\nb", "a\t\tb", ""],
            }
        )


        io_manager = self.io()
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        with self.connect_mssql() as connection:
            data = connection.exec_driver_sql(f'SELECT * FROM {schema}.{table}')
            assert data.fetchall() == [(1, 'a\t\nb'), (1, 'a\t\tb'), (1, None)]

    def test_absent_identity(self):
        schema = "test_polars_bcp_schema"
        table = "test_absent_identity"
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        asset_schema = [
            {"name": "a", "alias": "a", "type": "INT", "identity": True},
            {"name": "b", "type": "NVARCHAR", "length": 100},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
            },
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
            {
                "b": ["a\t\nb", "a\t\tb"],
            }
        )


        io_manager = self.io()
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        with self.connect_mssql() as connection:
            result = connection.exec_driver_sql(f'SELECT * FROM {schema}.{table}')
            assert result.fetchall() == [(1, 'a\t\nb'), (2, 'a\t\tb')]
 
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        with self.connect_mssql() as connection:
            result = connection.exec_driver_sql(f'SELECT * FROM {schema}.{table}')
            assert result.fetchall() == [(3, 'a\t\nb'), (4, 'a\t\tb')]

    def test_xml(self):
        schema = 'test_polars_bcp_schema'
        table = 'test_polars_bcp_table_xml'
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        asset_schema = [
            {"name": "a", "type": "INT", 'identity': True},
            {"name": "xml_data", "type": "XML"},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
            },
        )
        def my_asset(context):
            return pl.DataFrame({'xml_data': ["""<?xml version="1.0" encoding="UTF-8"?>
                <note>
                <to>Tove</to>
                <from>Jani</from>
                <heading>Reminder</heading>
                <body>Don't forget me this weekend!</body>
                </note>""".encode('utf-8').hex()]})

            # original structure

        io_manager = self.io()
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        with self.connect_mssql() as connection:
            result = connection.exec_driver_sql(
                f'SELECT * FROM {schema}.{table}')
            assert result.fetchall() == [(
                1, "<note><to>Tove</to><from>Jani</from><heading>Reminder</heading><body>Don't forget me this weekend!</body></note>")]

    def test_geo(self):
        schema = 'test_polars_bcp_schema'
        table = 'test_polars_bcp_table_geo'
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        asset_schema = [
            {"name": "a", "type": "INT", 'identity': True},
            {"name": "geo_data", "type": "GEOGRAPHY", 'srid': 4326},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "schema": schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
            },
        )
        def my_asset(context):
            return pl.DataFrame({'geo_data': ['0103000000010000000500000000000000004054C0000000000080464000000000004054C0000000000000464000000000000054C0000000000000464000000000000054C0000000000080464000000000004054C00000000000804640']})

            # original structure

        io_manager = self.io()
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )


    def test_load_input(self):
        io_manager = self.io()
        @asset(key_prefix=['dbo'])
        def polars_base_asset():
            with self.connect_mssql() as connection:
                connection.exec_driver_sql(
                    'DROP TABLE IF EXISTS dbo.polars_base_asset'
                )
                connection.exec_driver_sql(
                    "SELECT 1 as col1, 'b' as col2 INTO dbo.polars_base_asset"
                )

        @asset(key_prefix=['dbo'], io_manager_key='io_manager')
        def load_input_asset(polars_base_asset: pl.DataFrame):
            ...

        materialize(
            assets=[
                polars_base_asset,
                load_input_asset
            ],
            resources={'io_manager': io_manager}
        )

    def test_identity_io(self):
        schema = "test_polars_bcp_schema"
        table = "basic_io_identity"
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        io_manager = self.io_identity()

        asset_schema = [
            {"name": "b", "type": "NVARCHAR", "length": 10},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
                "schema": schema
            },
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
            {
                "a": [1, 1],
                "b": ["a", "a"],
            }
        )
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )

        with self.connect_mssql() as con:
            s = f"select column_name from information_schema.columns where table_name = '{table}' and table_schema = '{schema}'"
            r = con.exec_driver_sql(s)
            res = r.fetchall()

            assert {_[0] for _ in res} == {'id', 'b'}

            s = f"select max(id) from {schema}.{table}"
            r = con.exec_driver_sql(s)
            res = r.fetchone()
            assert res[0] == 4


    def test_identity_io_as_arg(self):
        schema = "test_polars_bcp_schema"
        table = "basic_io_identity_flag"
        drop = f"""DROP TABLE IF EXISTS {schema}.{table}"""

        with self.connect_mssql() as connection:
            connection.execute(text(drop))

        io_manager = self.io()

        asset_schema = [
            {"name": "b", "type": "NVARCHAR", "length": 10},
        ]

        @asset(
            name=table,
            metadata={
                "asset_schema": asset_schema,
                "add_row_hash": False,
                "add_load_datetime": False,
                "add_load_uuid": False,
                "add_identity_column": True,
                "schema": schema
            },
        )
        def my_asset(context):
            return data

            # original structure

        data = pl.DataFrame(
            {
                "a": [1, 1],
                "b": ["a", "a"],
            }
        )
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )
        materialize(
            assets=[my_asset],
            resources={"io_manager": io_manager},
        )

        with self.connect_mssql() as con:
            s = f"select column_name from information_schema.columns where table_name = '{table}' and table_schema = '{schema}'"
            r = con.exec_driver_sql(s)
            res = r.fetchall()

            assert {_[0] for _ in res} == {'id', 'b'}

            s = f"select max(id) from {schema}.{table}"
            r = con.exec_driver_sql(s)
            res = r.fetchone()
            assert res[0] == 4
