from uuid import uuid4
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    get_dagster_logger,
    TableColumn,
    TableSchema
)

from abc import abstractmethod, ABC
from .asset_schema import AssetSchema
from .mssql_connection import connect_mssql
from .utils import get_cleanup_statement, get_select_statement

from .bcp_resource import BCPResource

from sqlalchemy import URL


class BCPIOManagerCore(ConfigurableIOManager, ABC):
    resource: BCPResource

    def load_input(self, context: InputContext):
        asset_key = context.asset_key
        schema, table = asset_key.path[-2], asset_key.path[-1]

        _sql = get_select_statement(
            table,
            schema,
            context,
            (context.definition_metadata or {}).get("columns"),
        )

        connection_str = URL(**
            self.resource.connection_config # type: ignore
        ).render_as_string(hide_password=False)

        return self._read_from_database(sql=_sql, connection_string=connection_str)

    @abstractmethod
    def _read_from_database(self, sql: str, connection_string: str):
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            get_dagster_logger().info("No data to load")
            return

        bcp_manager = self.resource

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

        _asset_schema = metadata.get("asset_schema")
        if _asset_schema is None:
            raise ValueError('Missing asset schema in metadata def')
        asset_schema = AssetSchema(_asset_schema)

        add_row_hash = metadata.get("add_row_hash", True)
        add_load_datetime = metadata.get("add_load_datetime", True)
        add_load_uuid = metadata.get("add_load_uuid", True)

        process_datetime = metadata.get("process_datetime", bcp_manager.process_datetime)
        process_replacements = metadata.get(
            "process_replacements", bcp_manager.process_replacements
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

        get_dagster_logger().debug('Connecting to sql...')
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

        get_dagster_logger().debug('Connecting to sql...')
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

        meta_tbl = []
        for _ in asset_schema.get_sql_columns_as_dict(False):
            
            meta_tbl.append(
                TableColumn(
                    name=_['name'],
                    type=_['type']
                )
            )
        meta_schema = TableSchema(
            columns=meta_tbl
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
            | {
                "dagster/column_schema": meta_schema
            }
            | schema_deltas
        )

    @abstractmethod
    def check_empty(self, obj) -> bool:
        """Checks if frame is empty"""
        raise NotImplementedError
