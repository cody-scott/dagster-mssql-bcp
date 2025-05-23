
from dagster_mssql_bcp.bcp_core import (
    BCPIOManagerCore,
)

try:
    import pandas as pd
except ImportError:
    has_pandas = False

from .pandas_mssql_resource import PandasBCPResource

class PandasBCPIOManager(BCPIOManagerCore):
    resource: PandasBCPResource
    
    def _read_from_database(self, sql, connection_string):
        df = pd.read_sql(sql=sql, con=connection_string, dtype="str")
        return df
    
    def check_empty(self, obj):
        if obj is None or obj.empty:
            return True
        else:
            return False

