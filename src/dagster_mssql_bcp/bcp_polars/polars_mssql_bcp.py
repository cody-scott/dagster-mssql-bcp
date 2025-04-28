from pathlib import Path

import pendulum

try:
    import polars as pl
    import polars.selectors as cs

    polars_available = 1
except ImportError:
    polars_available = 0

from dagster_mssql_bcp.bcp_core import AssetSchema, BCPCore


class PolarsBCP(BCPCore):
    def _add_meta_columns(
        self,
        data: pl.LazyFrame,
        uuid_value: str,
        add_hash: bool = True,
        add_uuid: bool = True,
        add_datetime: bool = True,
    ):
        """
        Adds the meta columns to the dataframe.
        Calculation of the actual row hash is deferred until loaded into SQL.
        """
        columns_to_add = []
        if add_hash:
            columns_to_add.append(pl.lit(None).alias(self.row_hash_column_name))
        if add_uuid:
            columns_to_add.append(pl.lit(uuid_value).alias(self.load_uuid_column_name))
        if add_datetime:
            columns_to_add.append(
                pl.lit(
                    pendulum.now()
                    .to_iso8601_string()
                    .replace("T", " ")
                    .replace("Z", "+00:00")
                ).alias(self.load_datetime_column_name)
            )

        return data.with_columns(columns_to_add)

    def _replace_values(self, data: pl.LazyFrame, asset_schema: AssetSchema):
        """Replaces values in the DataFrame to ensure they are compatible with BCP."""
        number_columns_that_are_strings = [
            _
            for _ in data.select(cs.by_dtype(pl.String)).collect_schema().names()
            if _ in asset_schema.get_numeric_columns()
        ]

        string_cols = data.select(cs.by_dtype(pl.String)).collect_schema().names()

        if len(string_cols) > 0:
            # calculates only the rows that have replacements
            data = data.with_columns(
                [
                    pl.col(_)
                    .str.contains("(\t)|(\n)")
                    .alias(f"{_}__bcp__has_replacement_values")
                    for _ in string_cols
                ]
            )

            data = data.with_columns(
                pl.any_horizontal(
                    [f"{_}__bcp__has_replacement_values" for _ in string_cols]
                ).alias("should_process_replacements")
            )

            data = data.drop([f"{_}__bcp__has_replacement_values" for _ in string_cols])

        data = data.with_columns(
            [
                pl.col(_)
                .str.replace_all("\t", "__TAB__")
                .str.replace_all("\n", "__NEWLINE__")
                .str.replace_all("^nan$", "")
                .str.replace_all("^NAN$", "")
                .str.replace_all('^""$', "")
                .str.replace_all('^NULL$', "")
                .str.replace_all('^null$', "")
                for _ in string_cols
                if _ not in number_columns_that_are_strings
            ]
            + [
                pl.col(_)
                .str.replace_all(",", "")
                .str.replace_all("^nan$", "")
                .str.replace_all("^NAN$", "")
                .str.replace_all('^""$', "")
                for _ in number_columns_that_are_strings
            ]
            + [
                pl.col(_).cast(pl.Int64)
                for _ in data.select(cs.boolean()).collect_schema().names()
            ]
        )

        data = data.with_columns(pl.col(pl.String).replace("", None))

        return data

    def _process_datetime(
        self, data: pl.LazyFrame, asset_schema: AssetSchema
    ) -> pl.LazyFrame:
        """
        Processes datetime columns in the DataFrame to ensure they are compatible with BCP.

        First determine all columns which are already datetime
        Then convert remaining set to datetime.

        Next, convert any column without a timezone to UTC.

        Finally, convert the data to a string in the format "+00:00" instead of "Z",
        while also replacing "T" with a space.
        This is what BCP expects. 2024-01-01 00:00:00+00:00 from 2024-01-01T00:00:00Z
        """

        dt_columns = (
            data.select(cs.datetime(), cs.date(), cs.time()).collect_schema().names()
        )
        null_columns = data.select(cs.by_dtype(pl.Null)).collect_schema().names()

        data = data.with_columns(
            [
                pl.col(col).str.to_datetime()
                for col in asset_schema.get_datetime_columns()
                if col not in dt_columns and col not in null_columns
            ]
        )

        date_cols = data.select(cs.date()).collect_schema().names()
        data = data.with_columns([pl.col(_) for _ in date_cols])

        # change date/time to datetime
        data = data.with_columns(
            [
                pl.col(_).cast(pl.Datetime)
                for _ in data.select(cs.date()).collect_schema().names()
            ]
        )

        dt_columns_in_tz = (
            data.select(cs.datetime(time_zone="*")).collect_schema().names()
        )
        data = data.with_columns(
            [
                pl.col(_).dt.convert_time_zone("UTC")
                for _ in asset_schema.get_datetime_columns()
                if _ not in dt_columns_in_tz and _ not in null_columns
            ]
        )

        data = data.with_columns(
            [
                pl.col(_)
                .dt.to_string(format="%+")
                .str.replace("Z", "+00:00")
                .str.replace("T", " ")
                for _ in asset_schema.get_datetime_columns()
                if _ not in null_columns
            ]
        )
        return data

    def _reorder_columns(self, data: pl.LazyFrame, column_list: list[str]):
        """Reorder the data frame to match the order of the columns in the SQL table."""
        column_list = [
            column for column in column_list if column in data.collect_schema().names()
        ]
        return data.select(column_list)

    def _save_csv(self, data: pl.LazyFrame, path: Path, file_name: str):
        path = Path(path)
        data.sink_csv(
            path=path / file_name,
            line_terminator="\n",
            separator="\t",
        )

        return path / file_name

    def _get_frame_columns(self, data: pl.LazyFrame):
        return data.collect_schema().names()

    def _filter_columns(self, data: pl.LazyFrame, columns: list[str]):
        return data.select(columns)

    def _rename_columns(self, data: pl.LazyFrame, columns: dict) -> pl.LazyFrame:
        return data.rename(columns)

    def _add_identity_columns(
        self, data: pl.LazyFrame, asset_schema: AssetSchema
    ) -> pl.LazyFrame:
        ident_cols = asset_schema.get_identity_columns()
        missing_idents = [
            _ for _ in ident_cols if _ not in data.collect_schema().names()
        ]
        data = data.with_columns([pl.lit(None).alias(_) for _ in missing_idents])
        return data

    def _pre_processing_start_hook(self, data: pl.DataFrame):
        return data.lazy()

    def _add_replacement_flag_column(self, data: pl.DataFrame):
        data = data.with_columns(pl.lit(0).alias("should_process_replacements"))
        return data
