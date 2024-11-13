
from dagster_mssql_bcp.bcp_core import (
    BCPIOManagerCore,
)

try:
    import pandas as pd
except ImportError:
    has_pandas = False

from dagster_mssql_bcp.bcp_pandas import PandasBCP


class PandasBCPIOManager(BCPIOManagerCore):
    def _read_from_database(self, sql, connection_string):
        df = pd.read_sql(sql=sql, con=connection_string, dtype="str")
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
