from dagster_mssql_bcp.bcp_core import BCPIOManagerCore, BCPResource

import pandas as pd


class PandasBCPIOManager(BCPIOManagerCore):
    resource: BCPResource

    def _read_from_database(self, sql, connection_string):
        df = pd.read_sql(sql=sql, con=connection_string, dtype="str")
        return df

    def check_empty(self, obj):
        if obj is None or obj.empty:
            return True
        else:
            return False
