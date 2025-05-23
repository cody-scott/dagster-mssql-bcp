from dagster_mssql_bcp.bcp_core.bcp_resource import BCPResource
from .pandas_mssql_bcp import PandasBCP

class PandasBCPResource(BCPResource, PandasBCP):
    ...
