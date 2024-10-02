from uuid import uuid4

from dagster import ConfigurableIOManager, InputContext, OutputContext, get_dagster_logger

from .asset_schema import AssetSchema
from .mssql_connection import connect_mssql
from .utils import get_cleanup_statement, get_select_statement

from .bcp_core import BCPCore

class BCPIOManagerCore(ConfigurableIOManager):
    host: str
    port: str
    database: str
    username: str | None = None
    password: str | None = None
    driver: str = "ODBC Driver 18 for SQL Server"
    query_props: dict[str, str] = {}

    add_row_hash: bool = True
    add_load_datetime: bool = True
    add_load_uuid: bool = True

    bcp_arguments: dict[str, str] = {}
    bcp_path: str | None = None

    process_datetime: bool = True
    process_replacements: bool = True

    row_hash_column_name: str = "row_hash"
    load_uuid_column_name: str = "load_uuid"
    load_datetime_column_name: str = "load_datetime"

    def load_input(self, context: InputContext):
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            get_dagster_logger().info("No data to load")
            return

        bcp_manager = self.get_bcp(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            driver=self.driver,
            bcp_arguments=self.bcp_arguments,
            query_props=self.query_props,
            add_row_hash=self.add_row_hash,
            add_load_datetime=self.add_load_datetime,
            add_load_uuid=self.add_load_uuid,
            bcp_path=self.bcp_path,
            row_hash_column_name=self.row_hash_column_name,
            load_uuid_column_name=self.load_uuid_column_name,
            load_datetime_column_name=self.load_datetime_column_name,
        )

        metadata = (
            context.definition_metadata
            if context.definition_metadata is not None
            else {}
        )

        if len(context.asset_key.path) < 2:
            schema = 'dbo'
            table = context.asset_key.path[-1]
        else:
            schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
            
        schema = metadata.get("schema", schema)
        table = metadata.get("table", table)

        asset_schema = metadata.get("asset_schema")
        if asset_schema is None:
            raise ValueError("No data table provided in metadata")
        asset_schema = AssetSchema(asset_schema)

        add_row_hash = metadata.get("add_row_hash", True)
        add_load_datetime = metadata.get("add_load_datetime", True)
        add_load_uuid = metadata.get("add_load_uuid", True)

        process_datetime = metadata.get("process_datetime", self.process_datetime)
        process_replacements = metadata.get("process_replacements", self.process_replacements)

        uuid = str(uuid4())
        uuid_table = uuid.replace("-", "_").split("_")[0]
        io_table = f"{table}__io__{uuid_table}"

        asset_schema = bcp_manager._add_meta_to_asset_schema(
            asset_schema,
            add_row_hash=add_row_hash,
            add_load_datetime=add_load_datetime,
            add_load_uuid=add_load_uuid,
        )

        # create the table
        with connect_mssql(bcp_manager.connection_config) as connection:
            # if the table doesn't exist do this otherwise select 1=0
            result = connection.exec_driver_sql(
                f"""SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"""
            )
            if len(result.fetchall()) == 0:
                bcp_manager._create_schema(connection, schema)
                bcp_manager._create_table(
                    connection=connection,
                    schema=schema,
                    table=table,
                    columns=asset_schema.get_sql_columns(),
                )


        results = bcp_manager.load_bcp(
            data=obj,
            schema=schema,
            table=io_table,
            asset_schema=asset_schema,
            add_row_hash=add_row_hash,
            add_load_datetime=add_load_datetime,
            add_load_uuid=add_load_uuid,
            uuid=uuid,
            process_datetime=process_datetime,
            process_replacements=process_replacements,
        )

        uuid_value, row_count, deltas = (
            results["uuid"],
            results["row_count"],
            results["schema_deltas"],
        )
        asset_schema_columns_str = ",".join(asset_schema.get_columns())
        with connect_mssql(bcp_manager.connection_config) as connection:
            cleanup_sql = get_cleanup_statement(table, schema, context)
            connection.exec_driver_sql(cleanup_sql)
            connection.exec_driver_sql(
                f"""
                    INSERT INTO {schema}.{table} ({asset_schema_columns_str})
                    SELECT {asset_schema_columns_str}
                    FROM {schema}.{io_table}"""
            )
            connection.exec_driver_sql(f"DROP TABLE {schema}.{io_table}")

        context.add_output_metadata(
            dict(
                query=get_select_statement(
                    table,
                    schema,
                    context,
                    (context.definition_metadata or {}).get("columns"),
                ),
                uuid_query=f"SELECT * FROM {schema}.{table} WHERE load_uuid = '{uuid_value}'",
                row_count=row_count,
            )
            | deltas
        )

    def check_empty(self, obj) -> bool:
        """Checks if frame is empty"""
        raise NotImplementedError

    def get_bcp(self, *args, **kwargs) -> BCPCore:
        """Returns an instance of the BCP class for the given connection details."""
        raise NotImplementedError
