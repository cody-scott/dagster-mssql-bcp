from dagster_mssql_bcp.bcp_core import BCPIOManagerCore, get_select_statement
from dagster_mssql_bcp.bcp_polars import PolarsBCP

import polars as pl

from dagster import InputContext

class PolarsBCPIOManager(BCPIOManagerCore):
    def load_input(self, context: InputContext) -> pl.DataFrame:
        asset_key = context.asset_key
        schema, table = asset_key.path[-2], asset_key.path[-1]

        _sql = get_select_statement(
            table,
            schema,
            (context.definition_metadata or {}).get("columns"),
            context,
        )

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
        )

        connection_str = bcp_manager.generate_connection_url(
            self.connection_config
        ).render_as_string(hide_password=False)

        df = pl.read_database_uri(
            query=_sql,
            uri=connection_str,
        )
        return df

    def get_bcp(
        self,
        *args,
        **kwargs,
    ) -> PolarsBCP:
        return PolarsBCP(
            *args,
            **kwargs,
        )

    def check_empty(self, obj):
        if obj is None or obj.is_empty():
            return True
        else:
            return False
