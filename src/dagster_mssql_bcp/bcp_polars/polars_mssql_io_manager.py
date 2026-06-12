from dagster_mssql_bcp.bcp_core import BCPIOManagerCore, BCPResource

import polars as pl

class PolarsBCPIOManager(BCPIOManagerCore):
    resource: BCPResource
    
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
