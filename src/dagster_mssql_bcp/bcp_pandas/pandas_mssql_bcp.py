import csv
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    has_pandas = False

import pendulum
from dagster import get_dagster_logger

from dagster_mssql_bcp.bcp_core import AssetSchema, BCPCore



class PandasBCP(BCPCore):
    def _add_meta_columns(
        self,
        data: pd.DataFrame,
        uuid_value: str,
        add_hash: bool = True,
        add_uuid: bool = True,
        add_datetime: bool = True,
    ):
        if add_hash:
            data[self.row_hash_column_name] = None

        if add_uuid:
            data[self.load_uuid_column_name] = uuid_value

        if add_datetime:
            data[self.load_datetime_column_name] = (
                pendulum.now()
                .to_iso8601_string()
                .replace("T", " ")
                .replace("Z", "+00:00")
            )
        return data

    def _replace_values(self, data: pd.DataFrame, asset_schema: AssetSchema):
        get_dagster_logger().debug("Replacing characters for BCP")
        # replace tab characters with __TAB__
        get_dagster_logger().debug("Replacing tab characters with __TAB__")
        tab = self._tab_character  # type: ignore
        data = data.replace("\t", tab, regex=True)

        get_dagster_logger().debug("Replacing newline characters with __NEWLINE__")
        # replace newline characters with __NEWLINE__
        new_line = self._new_line_character  # type: ignore
        data = data.replace("\n", new_line, regex=True)

        get_dagster_logger().debug("Replacing True/False with 1/0")
        # replace True/False with 1/0
        data = data.replace(True, 1)
        data = data.replace(False, 0)
        data = data.replace("True", 1)
        data = data.replace("False", 0)

        get_dagster_logger().debug("Replacing NaN with empty string")
        # replace NaN with empty string
        data = data.fillna("")
        data = data.replace("^nan$", "", regex=True)
        data = data.replace("^NAN$", "", regex=True)
        data = data.replace("^NaT$", "", regex=True)

        for column in asset_schema.get_numeric_columns():
            data[column] = data[column].replace(",", "", regex=True)

        return data

    def _process_datetime(self, data: pd.DataFrame, asset_schema: AssetSchema):
        get_dagster_logger().debug("Replacing datetime to iso8601 string")
        for column_name in asset_schema.get_datetime_columns():
            data[column_name] = data[column_name].apply(PandasBCP.__process_dt)
        return data

    @staticmethod
    def __process_dt(row):
        """Process datetime columns to iso8601 string"""
        if row is None or pd.isna(row) or row == "":
            return row
        elif isinstance(row, pd.Timestamp):
            return (
                pendulum.parse(row.strftime("%Y-%m-%d %H:%M:%S.%f%z"))
                .to_iso8601_string()  # type: ignore
                .replace("Z", "+00:00")
                .replace("T", " ")
            )
        else:
            return (
                pendulum.parse(row)
                .to_iso8601_string()  # type: ignore
                .replace("Z", "+00:00")
                .replace("T", " ")
            )

    def _reorder_columns(self, data: pd.DataFrame, column_list: list[str]):
        column_list = [column for column in column_list if column in data.columns]
        return data[column_list]

    def _save_csv(self, data: pd.DataFrame, path: Path, file_name: str):
        get_dagster_logger().debug("Saving data to csv")
        path = Path(path)
        csv_file_path = path / file_name
        data.to_csv(
            csv_file_path,
            index=False,
            header=True,
            sep="\t",
            lineterminator="\n",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        return csv_file_path

    def _get_frame_columns(self, data: pd.DataFrame):
        return data.columns.tolist()

    def _filter_columns(self, data: pd.DataFrame, columns: list[str]):
        return data[columns]

    def _rename_columns(self, data: pd.DataFrame, columns: dict) -> pd.DataFrame:
        return data.rename(columns=columns)


    def _add_identity_columns(self, data: pd.DataFrame, asset_schema: AssetSchema) -> pd.DataFrame:
        ident_cols = asset_schema.get_identity_columns()
        missing_idents = [
            _ for _ in ident_cols if _ not in data.columns
        ]
        for _ in missing_idents:
            data[_] = None
        
        return data