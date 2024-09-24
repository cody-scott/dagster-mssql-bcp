from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from dagster import (
    get_dagster_logger,
)
from .shell import execute as execute_shell_command
from sqlalchemy import Connection, text

from .asset_schema import AssetSchema
from .bcp_logger import BCPLogger
from .mssql_connection import connect_mssql


class BCPCore(ABC):
    """
    Generalizes the process of loading data into a SQL Server database using BCP.

    Concrete implementations should implement the abstract methods to provide the necessary functionality for the specific data type.
    """

    host: str
    port: str
    database: str

    username: str | None
    password: str | None

    query_props: dict[str, str]

    bcp_arguments: dict[str, str] = {}
    bcp_path: str | None

    driver: str

    process_datetime: bool
    process_replacements: bool

    add_row_hash: bool
    add_load_datetime: bool
    add_load_uuid: bool

    row_hash_column_name: str
    load_uuid_column_name: str
    load_datetime_column_name: str

    _new_line_character: str = "__NEWLINE__"
    _tab_character: str = "__TAB__"

    def __init__(
        self,
        host: str,
        database: str,
        port: str | int = "1433",
        username: str | None = None,
        password: str | None = None,
        bcp_arguments: dict[str, str] = {},
        driver: str = "ODBC Driver 18 for SQL Server",
        query_props: dict[str, str] = {},
        add_row_hash: bool = True,
        add_load_datetime: bool = True,
        add_load_uuid: bool = True,
        bcp_path: str | None = None,
        process_datetime: bool = True,
        process_replacements: bool = True,
        row_hash_column_name: str = "row_hash",
        load_uuid_column_name: str = "load_uuid",
        load_datetime_column_name: str = "load_datetime",
    ):
        """
        Initialize the BCP core configuration.

        Args:
            host (str): The hostname of SQL Server.
            port (str | int): The port number of SQL Server.
            database (str): The name of the database.
            username (str | None, optional): The username for authentication. Defaults to None.
            password (str | None, optional): The password for authentication. Defaults to None.
            bcp_arguments (dict[str, str], optional): Additional arguments for BCP. Defaults to {}.
            driver (str, optional): The ODBC driver to use. Defaults to "ODBC Driver 18 for SQL Server".
            query_props (dict[str, str], optional): Properties for the query dict. Defaults to {}.
            add_row_hash (bool, optional): Whether to add a row hash. Defaults to True.
            add_load_datetime (bool, optional): Whether to add a load timestamp. Defaults to True.
            add_load_uuid (bool, optional): Whether to add a load UUID. Defaults to True.
            bcp_path (str | None, optional): The path to the BCP executable. Defaults to None.
            process_datetime (bool, optional): Whether to process datetime fields. Defaults to True.
            process_replacements (bool, optional): Whether to process replacements. Defaults to True.
        """
        self.host = host
        self.port = str(port)
        self.database = database
        self.username = username
        self.password = password

        self.add_row_hash = add_row_hash
        self.add_load_datetime = add_load_datetime
        self.add_load_uuid = add_load_uuid

        self.driver = driver
        self.query_props = query_props

        self.bcp_arguments = bcp_arguments
        self.bcp_path = bcp_path

        self.process_datetime = process_datetime
        self.process_replacements = process_replacements

        if bcp_path is not None:
            self.bcp_path = self.bcp_path
        else:
            self.bcp_path = "bcp"

        self.row_hash_column_name = row_hash_column_name
        self.load_uuid_column_name = load_uuid_column_name
        self.load_datetime_column_name = load_datetime_column_name

    @property
    def connection_config(self):
        """
        Generates a dictionary containing the configuration parameters for a database connection.
        Returns:
            dict: A dictionary with the following keys:
                - drivername (str): The name of the database driver.
                - username (str): The username for the database connection.
                - password (str): The password for the database connection.
                - host (str): The hostname of the database server.
                - port (int): The port number of the database server.
                - database (str): The name of the database.
                - query (dict): Additional query parameters, including the driver and any other properties.
        """

        return dict(
            drivername="mssql+pyodbc",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={
                "driver": self.driver,
            }
            | self.query_props,
        )

    def load_bcp(
        self,
        data,
        schema: str,
        table: str,
        asset_schema: list[dict] | AssetSchema | None = None,
        add_row_hash: bool = True,
        add_load_datetime: bool = True,
        add_load_uuid: bool = True,
        uuid: str | None = None,
        process_datetime: bool | None = None,
        process_replacements: bool | None = None,
    ):
        """
        Load data into a SQL table using Bulk Copy Program (BCP) with optional metadata and processing.
        Args:
            data: The data to be loaded, typically a DataFrame.
            schema (str): The schema name where the table resides.
            table (str): The table name where data will be loaded.
            asset_schema (list[dict] | AssetSchema | None, optional): Schema definition for the asset. Defaults to None.
            add_row_hash (bool, optional): Whether to add a row hash column. Defaults to True.
            add_load_datetime (bool, optional): Whether to add a load timestamp column. Defaults to True.
            add_load_uuid (bool, optional): Whether to add a load UUID column. Defaults to True.
            uuid (str | None, optional): UUID for the load operation. If None, a new UUID will be generated. Defaults to None.
            process_datetime (bool | None, optional): Whether to process datetime columns. Defaults to None.
            process_replacements (bool | None, optional): Whether to process value replacements. Defaults to None.
        Returns:
            dict: A dictionary containing the UUID of the load, the row count of the new data, and any schema deltas.
        """
        if process_datetime is None:
            process_datetime = self.process_datetime
        if process_replacements is None:
            process_replacements = self.process_replacements

        connection_config_dict = self.connection_config

        asset_schema = self._parse_asset_schema(schema, table, asset_schema)

        if not isinstance(asset_schema, AssetSchema):
            raise ValueError("Invalid Asset Schema provided")

        data = self._rename_columns(data, asset_schema.get_rename_dict())

        if uuid is None:
            uuid = str(uuid4())
        uuid_table = uuid.replace("-", "_")
        staging_table = f"{table}_staging_{uuid_table}"

        self._add_meta_to_asset_schema(
            asset_schema, add_row_hash, add_load_datetime, add_load_uuid
        )

        with connect_mssql(connection_config_dict) as connection:
            self._create_target_tables(
                schema, table, asset_schema, staging_table, connection
            )
            
            data, schema_deltas = self._pre_bcp_stage(
                connection,
                data,
                schema,
                table,
                asset_schema,
                add_row_hash,
                add_load_datetime,
                add_load_uuid,
                uuid,
                process_datetime,
                process_replacements,
            )

        self._bcp_stage(data, schema, staging_table)

        new_line_count = self._post_bcp_stage(
            data,
            schema,
            table,
            staging_table,
            asset_schema,
            add_row_hash,
            process_replacements,
            connection_config_dict,
        )

        return {
            "uuid": uuid,
            "row_count": new_line_count,
            "schema_deltas": schema_deltas,
        }
    
    def _pre_bcp_stage(
        self,
        connection: Connection,
        data,
        schema: str,
        table: str,
        asset_schema: AssetSchema,
        add_row_hash: bool,
        add_load_datetime: bool,
        add_load_uuid: bool,
        uuid: str,
        process_datetime: bool,
        process_replacements: bool,
    ):
        """
        This step is responsible for preparing the data for the BCP stage.
        This includes renaming and reformating columns, adding metadata columns, and validating the schema.
        """
        data = self._add_meta_columns(
            data,
            uuid_value=uuid,
            add_hash=add_row_hash,
            add_datetime=add_load_datetime,
            add_uuid=add_load_uuid,
        )
        data = self._add_identity_columns(data=data, asset_schema=asset_schema)

        sql_structure = self._get_sql_columns(connection, schema, table)
        frame_columns = self._get_frame_columns(data)

        schema_deltas = {}
        if sql_structure is not None:
            schema_deltas = self._validate_columns(
                frame_columns, asset_schema.get_columns(), sql_structure
            )

            # Filter columns that are not in the json schema (evolution)
        data = self._filter_columns(data, asset_schema.get_columns(True))
        sql_structure = sql_structure or frame_columns
        data = self._reorder_columns(data, sql_structure)

        if process_replacements:
            data = self._replace_values(data, asset_schema)
        if process_datetime:
            data = self._process_datetime(data, asset_schema)

        return data, schema_deltas
    
    def _bcp_stage(self, data, schema, staging_table):
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            format_file = temp_dir / f"{staging_table}_format_file.fmt"
            error_file = temp_dir / f"{staging_table}_error_file.err"
            csv_file = self._save_csv(data, temp_dir, f"{staging_table}.csv")

            self._generate_format_file(schema, staging_table, format_file)
            self._insert_with_bcp(
                schema,
                staging_table,
                csv_file,
                format_file,
                error_file,
            )

    def _post_bcp_stage(
        self,
        data,
        schema,
        table,
        staging_table,
        asset_schema,
        add_row_hash,
        process_replacements,
        connection_config_dict,
    ):
        with connect_mssql(connection_config_dict) as con:
            # Validate loads (counts of tables match)
            new_line_count = self._validate_bcp_load(
                con, schema, staging_table, len(data)
            )

            if process_replacements:
                self._replace_temporary_tab_newline(
                    con, schema, staging_table, asset_schema
                )

            if add_row_hash:
                self._calculate_row_hash(
                    con, schema, staging_table, asset_schema.get_hash_columns()
                )

            self._insert_and_drop_bcp_table(
                con, schema, table, staging_table, asset_schema
            )

        return new_line_count

    def _parse_asset_schema(self, schema, table, asset_schema):
        """
        Parses and returns the asset schema for a given table.
        If `asset_schema` is None, it retrieves the schema from the database
        using the provided `schema` and `table` names, excluding specific columns.

        If `asset_schema` is a list, it converts it to an `AssetSchema` object.
        Args:
            schema (str): The schema name of the table.
            table (str): The table name.
            asset_schema (None or list): The asset schema to be parsed. If None,
                         the schema is retrieved from the database.
                         If a list, it is converted to an `AssetSchema` object.
        Returns:
            AssetSchema: The parsed asset schema.
        """
        if asset_schema is None:
            with connect_mssql(self.connection_config) as connection:
                asset_schema = AssetSchema.get_asset_schema_from_db(
                    connection=connection,
                    schema=schema,
                    table=table,
                    exclude_columns=[
                        self.row_hash_column_name,
                        self.load_uuid_column_name,
                        self.load_datetime_column_name,
                    ],
                )
        elif isinstance(asset_schema, list):
            asset_schema = AssetSchema(asset_schema)
        return asset_schema

    # region pre load
    def _add_meta_to_asset_schema(
        self, asset_schema, add_row_hash, add_load_datetime, add_load_uuid
    ) -> AssetSchema:
        """
        Adds metadata columns to the asset schema if specified.
        This method adds columns for row hash, load timestamp, and load UUID to the given
        asset schema based on the provided flags. The columns are only added if they are
        not already present in the asset schema.
        Args:
            asset_schema (AssetSchema): The schema to which metadata columns will be added.
            add_row_hash (bool): Flag indicating whether to add the row hash column.
            add_load_datetime (bool): Flag indicating whether to add the load timestamp column.
            add_load_uuid (bool): Flag indicating whether to add the load UUID column.
        Returns:
            AssetSchema: The updated asset schema with the added metadata columns.
        """
        schema_columns = asset_schema.get_columns()
        if add_row_hash and self.row_hash_column_name not in schema_columns:
            asset_schema.add_column(
                {
                    "name": self.row_hash_column_name,
                    "type": "NVARCHAR",
                    "length": 200,
                    "hash": False,
                }
            )

        if add_load_uuid and self.load_uuid_column_name not in schema_columns:
            asset_schema.add_column(
                {
                    "name": self.load_uuid_column_name,
                    "type": "NVARCHAR",
                    "length": 200,
                    "hash": False,
                }
            )

        if add_load_datetime and self.load_uuid_column_name not in schema_columns:
            asset_schema.add_column(
                {
                    "name": self.load_datetime_column_name,
                    "type": "DATETIME2",
                    "hash": False,
                }
            )
        return asset_schema

    def _create_target_tables(
        self, schema, table, asset_schema: AssetSchema, staging_table, connection
    ):
        self._create_schema(connection, schema)
        self._create_table(
            connection,
            schema,
            table,
            asset_schema.get_sql_columns(),
        )
        connection.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
        # problem is this is creating a table with identity but we have filtered out the identity column
        self._create_table(
            connection,
            schema,
            staging_table,
            asset_schema.get_sql_columns(True),
        )

    @abstractmethod
    def _reorder_columns(
        self,
        data,
        column_list: list[str],
    ):
        """
        Reorders the columns of the given data according to the specified column list.
        Args:
            data: The data whose columns need to be reordered. The type of data can vary
                  (e.g., DataFrame, list of dictionaries, etc.), depending on the implementation.
            column_list (list[str]): A list of column names in the desired order.
        """
        raise NotImplementedError

    @abstractmethod
    def _add_meta_columns(
        self,
        data,
        uuid_value: str,
        add_hash: bool = True,
        add_uuid: bool = True,
        add_datetime: bool = True,
    ):
        """
        Adds metadata columns to the given data.
        This function should add the column to each row of the data.

        ie:
            pandas/polars = new column
            jsonl = new key value pair
        Args:
            data: The data to which metadata columns will be added.
            uuid_value (str): The UUID value to be added to the data.
            add_hash (bool): If True, a hash column will be added to the data. Default is True.
            add_uuid (bool): If True, a UUID column will be added to the data. Default is True.
            add_datetime (bool): If True, a datetime column will be added to the data. Default is True.
        """
        raise NotImplementedError

    @abstractmethod
    def _process_datetime(self, data, asset_schema: AssetSchema):
        """
        Processes datetime data according to the provided asset schema.
        BCP expects the datetime data to be in a specific format, so this method should
        convert the datetime data to the correct format.

        Example format: 2024-12-31 23:59:59.9999999+00:00
        Args:
            data: The datetime data to be processed.
            asset_schema (AssetSchema): The schema defining the structure of the asset.
        """
        raise NotImplementedError

    @abstractmethod
    def _replace_values(self, data, asset_schema: AssetSchema):
        """
        Replace values in the given data according to the provided asset schema.

        This method should replace values in the data to ensure they are compatible with BCP.
        You must replace tab characters with {_tab_character}, newline characters with {_new_line_character}.

        None/Nulls should be replaced with an empty string.
        True/False should be replaced with 1/0.

        Numeric columns that are stored as strings should have commas removed.

        Args:
            data: The data in which values need to be replaced.
            asset_schema (AssetSchema): The schema that defines how values should be replaced.
        """

        raise NotImplementedError

    @abstractmethod
    def _rename_columns(self, data, columns: dict):
        """
        Renames columns in the given DataFrame according to the provided mapping.
        Args:
            data: The data whose columns are to be renamed.
            columns (dict): A dictionary where keys are current column names and values are the new column names.
        """
        raise NotImplementedError

    def _validate_columns(
        self,
        data_columns: list[str],
        asset_schema_columns: list[str],
        sql_columns: list[str] | None = None,
    ) -> dict:
        """
        Validates the columns of a DataFrame against a schema and optionally against SQL columns.
        This method will fail if data is no longer delivered in the new delivery which was defined in the schema.

        Args:
            data_columns (list[str]): List of column names in the data.
            asset_schema_columns (list[str]): List of column names in the schema.
            sql_columns (list[str] | None, optional): List of column names in the SQL table. Defaults to None.
        Returns:
            dict: A dictionary containing the differences between the columns in the data, asset schema, and SQL table.
        Raises:
            ValueError: If there are columns in the asset schema that are not present in the data.
        """
        results = {}

        if sql_columns is not None:
            sql_columns = [column for column in sql_columns]
            results["Columns in SQL that are not in DataFrame"] = (
                self.__get_item_in_first_set_not_in_second_set(
                    sql_columns, data_columns
                )
            )
            results["Columns in SQL that are not in Schema"] = (
                self.__get_item_in_first_set_not_in_second_set(
                    sql_columns, asset_schema_columns
                )
            )
            results["Columns in DataFrame that are not in SQL"] = (
                self.__get_item_in_first_set_not_in_second_set(
                    data_columns,
                    sql_columns,
                )
            )
            results["Columns in Schema that are not in SQL"] = (
                self.__get_item_in_first_set_not_in_second_set(
                    asset_schema_columns, sql_columns
                )
            )

        results["Columns in DataFrame that are not in Schema"] = (
            self.__get_item_in_first_set_not_in_second_set(
                data_columns,
                asset_schema_columns,
            )
        )
        results["Columns in Schema that are not in DataFrame"] = (
            self.__get_item_in_first_set_not_in_second_set(
                asset_schema_columns, data_columns
            )
        )

        # this fails if you said a column should be there, but it no longer appears in the dataframe
        # aka reverse evolution where a column is removed.
        if len(results.get("Columns in Schema that are not in DataFrame", [])) != 0:
            error_message = []
            for _ in results:
                if len(results[_]) > 0:
                    error_lines = "\n".join(results[_])
                    error_message.append(f"{_}:\n{error_lines}")

            raise ValueError("\n\n".join(error_message))

        return {_: results[_] for _ in results if len(results[_]) != 0}

    def __get_item_in_first_set_not_in_second_set(
        self, first_set: list, second_set: list
    ):
        """
        Returns a list of items that are present in the first set but not in the second set.
        Args:
            first_set (list): The first list of items.
            second_set (list): The second list of items.
        Returns:
            list: A list containing items that are in the first set but not in the second set.
        """
        return [item for item in first_set if item not in second_set]

    def _create_schema(self, connection: Connection, schema: str):
        """
        Creates a schema in the database if it does not already exist.
        Args:
            connection (Connection): The database connection object.
            schema (str): The name of the schema to be created.
        """

        sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """
        connection.execute(text(sql))

    def _create_table(
        self,
        connection: Connection,
        schema: str,
        table: str,
        columns: list[str],
    ):
        """
        Creates a table in the specified schema if it does not already exist.
        Args:
            connection (Connection): The database connection object.
            schema (str): The schema in which to create the table.
            table (str): The name of the table to create.
            columns (list[str]): a list of strings containing the SQL type and constraints. NVARCHAR(10), BIGINT, DECIMAL(18,2).
        Returns:
            None
        """
        column_list = columns

        column_list_str = ",\n".join(column_list)
        sql = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}')
        BEGIN
        CREATE TABLE {schema}.{table} (
            {column_list_str}
        )
        END
        """
        connection.execute(text(sql))

    def _get_sql_columns(
        self, connection: Connection, schema: str, table: str
    ) -> list[str] | None:
        """
        Retrieves the column names of a specified table within a given schema from the database.
        Primary reason is BCP returns as the structure of the table.
        This is an easy way of getting back its representation at the cost of a call to SQL.
        Args:
            connection (Connection): The database connection object.
            schema (str): The schema name where the table resides.
            table (str): The name of the table whose columns are to be retrieved.
        Returns:
            list[str] | None: A list of column names if the table exists and has columns,
                              otherwise None if the table has no columns.
        """
        sql = f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        result = [row[0] for row in connection.execute(text(sql))]
        result = None if len(result) == 0 else result
        return result
    @abstractmethod
    def _add_identity_columns(self, data, asset_schema: AssetSchema):
        """
        Adds missing identity columns to the given data.
        This method should add identity columns to the data where they are missing.
        Args:
            data: The data to which identity columns will be added.
            asset_schema (AssetSchema): The schema that defines the identity columns.
        """
        raise NotImplementedError
    
    @abstractmethod
    def _get_frame_columns(self, data) -> list[str]:
        """
        Extracts and returns the column names from the given data.
        Args:
            data: The data from which to extract column names.
        Returns:
            list[str]: A list of column names.
        """
        raise NotImplementedError

    @abstractmethod
    def _filter_columns(self, data, columns: list[str]):
        """
        Filters the given data based on the specified columns.
        This should return the data with only the columns specified in the `columns` list.
        Args:
            data: The data to be filtered.
            columns (list[str]): A list of column names to filter the data by.
        """
        raise NotImplementedError

    @abstractmethod
    def _save_csv(self, data, path: Path, file_name: str):
        """
        Save the given data to a CSV file at the specified path.
        Args:
            data: The data to be saved to the CSV file.
            path (Path): The directory path where the CSV file will be saved.
            file_name (str): The name of the CSV file to be saved.
        """
        raise NotImplementedError

    def _parse_bcp_args(self, additional_args: dict[str, str]) -> dict[str, str]:
        """
        Parses and constructs BCP (Bulk Copy Program) command-line arguments.
        Args:
            additional_args (dict[str, str]): Additional BCP arguments to include.
        Returns:
            dict[str, str]: A dictionary of BCP arguments with their corresponding values.
        """

        bcp_args = {}
        if self.username is not None:
            bcp_args["-U"] = self.username
        if self.password is not None:
            bcp_args["-P"] = self.password

        bcp_args["-d"] = self.database
        bcp_args["-S"] = f'"{self.host},{self.port}"'

        bcp_args = bcp_args | additional_args
        bcp_args = bcp_args | self.bcp_arguments

        bcp_args_str = " ".join([f"{_} {bcp_args[_]}" for _ in bcp_args])
        return bcp_args_str

    def _generate_format_file(self, schema: str, table: str, format_file_path: Path):
        """
        Generates a BCP format file for the specified schema and table.
        Args:
            schema (str): The schema name of the table.
            table (str): The table name for which the format file is generated.
            format_file_path (Path): The file path where the format file will be saved.
        Raises:
            RuntimeError: If the BCP command returns a non-zero exit code.
        """

        get_dagster_logger().debug("Generating BCP format file")
        bcp_args = {
            "-c": "",
            "-t": '"\t"',
            "-r": '"\n"',
            "-f": f'"{str(format_file_path)}"',
        }
        args_str = self._parse_bcp_args(bcp_args)

        # This generates a format file for the bcp command
        format_cmd = f"""{self.bcp_path} [{schema}].[{table}] format nul {args_str}"""
        result = execute_shell_command(format_cmd, "STREAM", BCPLogger("BCP"))
        if result[1] != 0:
            raise RuntimeError("Non-zero exit code for bcp format file generation")

    def _insert_with_bcp(
        self,
        schema: str,
        bcp_table: str,
        csv_file_path: Path,
        format_file_path: Path,
        error_file_path: Path,
    ):
        """
        Inserts data into a specified table using the Bulk Copy Program (BCP).
        Args:
            schema (str): The schema of the table.
            bcp_table (str): The name of the table to insert data into.
            csv_file_path (Path): The path to the CSV file containing the data to be inserted.
            format_file_path (Path): The path to the BCP format file.
            error_file_path (Path): The path to the file where errors will be logged.
        Raises:
            RuntimeError: If BCP fails or returns a non-zero exit code.
        """
        try:
            get_dagster_logger().debug("Inserting data with BCP")
            bcp_args = {
                "-c": "",
                "-t": '"\t"',
                "-r": '"\n"',
                "-f": f'"{str(format_file_path)}"',
                "-e": f'"{error_file_path}"',
                "-m1": "",
                "-F": 2,
                "-b": 5000,
            }
            args_str = self._parse_bcp_args(bcp_args)

            insert_cmd = f"""{self.bcp_path} {schema}.{bcp_table} in "{csv_file_path}" {args_str}"""  # noqa

            results = execute_shell_command(insert_cmd, "STREAM", BCPLogger("BCP"))
            if "Error" in results[0]:
                raise RuntimeError("BCP failure")

            if results[1] != 0:
                raise RuntimeError("Non-zero exit code for bcp")

        except RuntimeError as ae:
            with open(error_file_path, "r") as f:
                error_message = f.read()

            error_message = error_message.split("\n")[0]
            column_regex = r"(?<=Column )\d+"
            column_number = re.search(column_regex, error_message)
            if column_number is not None:
                column_number = int(column_number.group(0))
                error_column = list(
                    csv_file_path.read_text().splitlines()[0].split("\t")
                )[column_number - 1]
                error_message = f"{error_message} - column name: {error_column}"

            get_dagster_logger().error(f"BCP error: {error_message}")
            raise ae

    # endregion

    # region post load
    def _validate_bcp_load(
        self,
        connection: Connection,
        schema: str,
        bcp_table: str,
        row_count: int,
    ):
        """
        Validates the BCP load by comparing the row count in the specified table with the expected row count.
        Args:
            connection (Connection): The database connection object.
            schema (str): The schema name where the table resides.
            bcp_table (str): The name of the table to validate.
            row_count (int): The expected number of rows in the table.
        Returns:
            int: The actual number of rows in the table.
        Raises:
            ValueError: If the validation fails or no result is returned from the query.
        """
        get_dagster_logger().debug("Validating BCP load")
        validate_load_sql = f"SELECT COUNT(*) FROM {schema}.{bcp_table}"
        cursor = connection.execute(
            text(validate_load_sql.format(schema=schema, table=bcp_table))
        )
        result = cursor.fetchone()
        if result is None:
            raise ValueError("No result from validation")
        if result[0] != row_count:
            raise ValueError("Validation failed")
        return result[0]

    def _replace_temporary_tab_newline(
        self, connection: Connection, schema: str, table: str, asset_schema: AssetSchema
    ):
        """
        Reverses temporary replacements of TAB and NEWLINE characters in specified columns of a table.
        This method updates the specified table by replacing temporary representations of
        newline and tab characters with their actual character equivalents (CHAR(10) for newline
        and CHAR(9) for tab) in all text columns defined in the asset schema.
        Args:
            connection (Connection): The database connection object used to execute the update query.
            schema (str): The schema name of the table to be updated.
            table (str): The table name to be updated.
            asset_schema (AssetSchema): An object that provides the schema of the asset, including
                                        methods to retrieve text columns.
        Returns:
            None
        """
        get_dagster_logger().debug("Replacing temporary tab and newline characters")
        new_line = self._new_line_character  # type: ignore
        tab = self._tab_character  # type: ignore
        update_sets = [
            f"""{column} = REPLACE(REPLACE({column}, '{new_line}', CHAR(10)), '{tab}', CHAR(9))"""
            for column in asset_schema.get_text_columns()
        ]

        if len(update_sets) == 0:
            return

        update_sql = """
        UPDATE {schema}.{table}
        SET
        {set_columns}
        """

        update_sql_str = update_sql.format(
            schema=schema,
            table=table,
            set_columns=",\n".join(update_sets),
        )
        connection.execute(text(update_sql_str))

    def _insert_and_drop_bcp_table(
        self,
        connection: Connection,
        schema: str,
        table: str,
        bcp_table: str,
        asset_schema: AssetSchema,
    ):
        """
        Inserts data from a BCP (Bulk Copy Program) table into the main table and then drops the BCP table.
        Args:
            connection (Connection): The database connection object.
            schema (str): The schema name where the tables reside.
            table (str): The name of the main table where data will be inserted.
            bcp_table (str): The name of the BCP table from which data will be selected.
            asset_schema (AssetSchema): An object representing the schema of the asset, used to get column names.
        Returns:
            None
        Raises:
            SQLAlchemyError: If there is an error executing the SQL statements.
        """

        schema_columns = asset_schema.get_columns()
        schema_columns_str = ", ".join(schema_columns)

        schema_with_cast = asset_schema.get_sql_columns_with_cast()
        schema_with_cast_str = ", ".join(schema_with_cast)

        get_dagster_logger().debug("Inserting data and dropping BCP table")
        insert_sql = f"""
        INSERT INTO {schema}.{table} ({schema_columns_str})
        SELECT {schema_with_cast_str} FROM {schema}.{bcp_table}
        """
        connection.execute(
            text(
                insert_sql.format(
                    schema=schema,
                    table=table,
                    bcp_schema=schema,
                    bcp_table=bcp_table,
                )
            )
        )
        connection.execute(text(f"DROP TABLE {schema}.{bcp_table}"))

    def _calculate_row_hash(
        self, connection: Connection, schema: str, table: str, columns: list[str]
    ):
        """
        Calculates and updates the row hash for a given table in the database.
        This method constructs a SQL query to calculate a hash value for each row in the specified
        table based on the provided columns. The hash value is then stored in a column named 'row_hash'.
        Args:
            connection (Connection): The database connection object.
            schema (str): The schema name of the table.
            table (str): The table name.
            columns (list[str]): A list of column names to be included in the hash calculation.
        Returns:
            None
        """
        col_sql = [
            f"COALESCE(CAST({column} AS NVARCHAR(MAX)), '')" for column in columns
        ]
        col_sql_str = " + ".join(col_sql)

        hash_sql = f"""CONVERT(NVARCHAR(32), HASHBYTES('MD5', {col_sql_str}), 2)"""

        update_sql = f"""
        UPDATE {schema}.{table}
        SET {self.row_hash_column_name} = {hash_sql}
        """

        connection.execute(text(update_sql))

    # endregion
