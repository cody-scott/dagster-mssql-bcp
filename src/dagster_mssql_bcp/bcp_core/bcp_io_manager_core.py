from uuid import uuid4

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    get_dagster_logger,
)

from abc import abstractmethod, ABC
from .asset_schema import AssetSchema
from .mssql_connection import connect_mssql
from .utils import get_cleanup_statement, get_select_statement

from .bcp_core import BCPCore


class BCPIOManagerCore(ConfigurableIOManager, ABC):
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

    @property
    def config(self):
        return dict(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            add_row_hash=self.add_row_hash,
            add_load_datetime=self.add_load_datetime,
            add_load_uuid=self.add_load_uuid,
            driver=self.driver,
            query_props=self.query_props,
            bcp_arguments=self.bcp_arguments,
            bcp_path=self.bcp_path,
            process_datetime=self.process_datetime,
            process_replacements=self.process_replacements,
            row_hash_column_name=self.row_hash_column_name,
            load_uuid_column_name=self.load_uuid_column_name,
            load_datetime_column_name=self.load_datetime_column_name,
        )

    @abstractmethod
    def load_input(self, context: InputContext):
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            get_dagster_logger().info("No data to load")
            return

        bcp_manager = self.get_bcp(**self.config)

        metadata = (
            context.definition_metadata
            if context.definition_metadata is not None
            else {}
        )

        if len(context.asset_key.path) < 2:
            schema = "dbo"
            table = context.asset_key.path[-1]
        else:
            schema, table = context.asset_key.path[-2], context.asset_key.path[-1]

        schema = metadata.get("schema", schema)
        table = metadata.get("table", table)

        asset_schema = AssetSchema(metadata.get("asset_schema"))

        add_row_hash = metadata.get("add_row_hash", True)
        add_load_datetime = metadata.get("add_load_datetime", True)
        add_load_uuid = metadata.get("add_load_uuid", True)

        process_datetime = metadata.get("process_datetime", self.process_datetime)
        process_replacements = metadata.get(
            "process_replacements", self.process_replacements
        )

        uuid = str(uuid4())
        uuid_table = uuid.replace("-", "_").split("_")[0]
        staging_Table = f"{table}__io__{uuid_table}"

        obj = bcp_manager._rename_columns(obj, asset_schema.get_rename_dict())

        asset_schema = bcp_manager._add_meta_to_asset_schema(
            asset_schema,
            add_row_hash=add_row_hash,
            add_load_datetime=add_load_datetime,
            add_load_uuid=add_load_uuid,
        )

        with connect_mssql(bcp_manager.connection_config) as connection:
            data, schema_deltas = bcp_manager._pre_bcp_stage(
                connection=connection,
                data=obj,
                schema=schema,
                table=table,
                asset_schema=asset_schema,
                add_row_hash=add_row_hash,
                add_load_datetime=add_load_datetime,
                add_load_uuid=add_load_uuid,
                uuid=uuid,
                process_datetime=process_datetime,
                process_replacements=process_replacements,
                staging_table=staging_Table,
            )

        bcp_manager._bcp_stage(data, schema, staging_Table)

        with connect_mssql(bcp_manager.connection_config) as connection:
            cleanup_sql = get_cleanup_statement(table, schema, context)
            connection.exec_driver_sql(cleanup_sql)
            row_count = bcp_manager._post_bcp_stage(
                connection=connection,
                data=obj,
                schema=schema,
                table=table,
                staging_table=staging_Table,
                asset_schema=asset_schema,
                add_row_hash=add_row_hash,
                process_replacements=process_replacements,
            )

        context.add_output_metadata(
            dict(
                query=get_select_statement(
                    table,
                    schema,
                    context,
                    (context.definition_metadata or {}).get("columns"),
                ),
                uuid_query=f"SELECT * FROM {schema}.{table} WHERE load_uuid = '{uuid}'",
                row_count=row_count,
            )
            | schema_deltas
        )

    @abstractmethod
    def check_empty(self, obj) -> bool:
        """Checks if frame is empty"""
        raise NotImplementedError

    @abstractmethod
    def get_bcp(self, *args, **kwargs) -> BCPCore:
        """Returns an instance of the BCP class for the given connection details."""
        raise NotImplementedError
