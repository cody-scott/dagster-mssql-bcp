import dagster as dg

from typing import Any

class BCPResource(dg.ConfigurableResource):
    ...
    host: str
    port: str
    database: str

    username: str | None
    password: str | None

    query_props: dict[str, Any] = {}

    bcp_arguments: dict[str, str] = {}
    bcp_path: str | None = None

    driver: str = "ODBC Driver 18 for SQL Server"

    process_datetime: bool = True
    process_replacements: bool = True

    add_row_hash: bool = True
    add_load_datetime: bool = True
    add_load_uuid: bool = True

    row_hash_column_name: str = "row_hash"
    load_uuid_column_name: str = "load_uuid"
    load_datetime_column_name: str = "load_datetime"

    _new_line_character: str = "__NEWLINE__"
    _tab_character: str = "__TAB__"

    staging_database: str | None = None
