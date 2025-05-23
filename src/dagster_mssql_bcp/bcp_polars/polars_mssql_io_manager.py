from dagster_mssql_bcp.bcp_core import BCPIOManagerCore

import polars as pl
from .polars_mssql_resource import PolarsBCPResource


class PolarsBCPIOManager(BCPIOManagerCore):
    resource: PolarsBCPResource
    
    def _read_from_database(self, sql, connection_string):
        df = pl.read_database_uri(
            query=sql,
            uri=connection_string,
        )
        return df

    def check_empty(self, obj):
        if obj is None or obj.is_empty():
            return True
        else:
            return False
