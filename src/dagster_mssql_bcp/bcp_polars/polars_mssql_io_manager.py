from dagster_mssql_bcp.bcp_core import BCPIOManagerCore
from dagster_mssql_bcp.bcp_polars import PolarsBCP

import polars as pl



class PolarsBCPIOManager(BCPIOManagerCore):
    def _read_from_database(self, sql, connection_string):
        df = pl.read_database_uri(
            query=sql,
            uri=connection_string,
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
