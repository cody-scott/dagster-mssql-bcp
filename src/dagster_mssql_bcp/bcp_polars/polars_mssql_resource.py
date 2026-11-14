from dagster_mssql_bcp.bcp_core.bcp_core import BCPCore
from dagster_mssql_bcp.bcp_core.bcp_resource import BCPResource
from .polars_mssql_bcp import PolarsBCP

class PolarsBCPResource(BCPResource):
    def get_engine(self) -> BCPCore:
        return PolarsBCP(config=self)
