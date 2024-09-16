import pandas as pd
from dagster import InputContext

from dagster_mssql_bcp_core import (
    BCPIOManagerCore,
    get_select_statement,
)
from dagster_mssql_pandas.pandas_mssql_bcp import PandasBCP


class PandasBCPIOManager(BCPIOManagerCore):
    def load_input(self, context: InputContext) -> pd.DataFrame:
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
            add_load_timestamp=self.add_load_timestamp,
            add_load_uuid=self.add_load_uuid,
            bcp_path=self.bcp_path,
        )

        asset_key = context.asset_key
        schema, table = asset_key.path[-2], asset_key.path[-1]
        _sql = get_select_statement(
            table,
            schema,
            context,
            (context.definition_metadata or {}).get("columns"),
        )
        connection_str = bcp_manager.generate_connection_url(
            self.connection_config
        ).render_as_string(hide_password=False)
        df = pd.read_sql(sql=_sql, con=connection_str, dtype="str")
        return df

    def get_bcp(
        self,
        *args,
        **kwargs,
    ) -> PandasBCP:
        return PandasBCP(
            *args,
            **kwargs,
        )

    def check_empty(self, obj):
        if obj is None or obj.empty:
            return True
        else:
            return False
