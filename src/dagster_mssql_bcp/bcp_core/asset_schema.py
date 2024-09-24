from sqlalchemy import Connection, text

schema_spec = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "pattern": "^[A-Za-z][A-Za-z0-9_]*$",
                "not": {"enum": ["exit"]},
            },
            "alias": {
                "type": "string",
                "pattern": "^[A-Za-z][A-Za-z0-9_]*$",
                "not": {"enum": ["exit"]},
            },
            "type": {
                "type": "string",
                "enum": [
                    "DATETIME2",
                    "DATETIME",
                    "DATETIMEOFFSET",
                    "DATE",
                    "TIME",
                    "VARCHAR",
                    "NVARCHAR",
                    "BIT",
                    "BIGINT",
                    "INT",
                    "SMALLINT",
                    "FLOAT",
                    "NUMERIC",
                    "DECIMAL",
                    "MONEY",
                    "XML",
                ],
            },
            "length": {"type": "integer"},
            "precision": {"type": "integer"},
            "scale": {"type": "integer"},
            "pk": {"type": "boolean"},
            "hash": {"type": "boolean"},
        },
        "required": ["name"],
    },
}


class AssetSchema:
    datetime_column_types = [
        "DATETIME2",
        "DATETIME",
        "DATETIMEOFFSET",
        "DATE",
        "TIME",
    ]
    text_column_types = ["VARCHAR", "NVARCHAR"]
    bool_column_types = ["BIT"]
    int_column_types = ["BIGINT", "INT", "SMALLINT"]
    float_column_types = ["FLOAT"]
    decimal_column_types = ["NUMERIC", "DECIMAL"]
    money_column_types = ["MONEY"]
    xml_column_types = ["XML"]
    binary_column_types = ['BINARY', 'VARBINARY']
    geography_column_types = ['GEOGRAPHY']
    geometry_column_types = ['GEOMETRY']

    number_column_types = (
        int_column_types
        + float_column_types
        + decimal_column_types
        + money_column_types
    )

    allowed_types = (
        text_column_types
        + datetime_column_types
        + bool_column_types
        + int_column_types
        + float_column_types
        + decimal_column_types
        + money_column_types
        + xml_column_types
        + binary_column_types
        + geography_column_types
        + geometry_column_types
    )

    _stage_as_binary = (
        xml_column_types +
        geography_column_types +
        geometry_column_types
    )

    def __init__(self, schema: list[dict]):
        self.schema = schema
        self.validate_asset_schema()

    def __eq__(self, value: "AssetSchema") -> bool:
        for _ in self.schema:
            if _ not in value.schema:
                return False

        for _ in value.schema:
            if _ not in self.schema:
                return False
        return True

    def validate_asset_schema(self):
        """Validates the asset schema for:

        all entries have a name
        all columns have a unique name
        all columns have a valid type
        """
        columns = {}
        for column in self.schema:
            column_name = column.get("name")
            alias_name = column.get("alias")
            if column_name is None:
                raise ValueError(
                    f"Column name not provided for column: {column}")

            if alias_name is not None:
                column_name = alias_name

            column_counter = columns.get(column_name, 0)
            if column_counter > 0:
                msg = f"Duplicate column name: {column['name']}"
                if alias_name:
                    msg += f' alias as {alias_name}'
                raise ValueError(msg)

            columns[column_name] = 1

            column_type = column.get("type", None)
            if column_type is None:
                raise ValueError(
                    f"Column type not provided for column: {column['name']}")

            if column_type not in self.allowed_types:
                raise ValueError(f"Invalid data type: {column['type']}")

    @staticmethod
    def _resolve_name(column: dict):
        """Resolve a columns name as either the alias or name, if no alias provided"""
        return column.get("alias", column.get("name"))

    @staticmethod
    def _resolve_type(column: dict):
        """Returns the SQL type of column"""
        return column["type"].upper()

    def get_source_columns(self):
        """Returns a list of columns by their source names."""
        return [column["name"] for column in self.schema]

    def get_columns(self, include_identity=False):
        """Return the columns from the schema. Default does not include columns flagged as `identity`"""
        result = []
        for column in self.schema:
            if column.get("identity", False) is True and not include_identity:
                continue
            column_name = self._resolve_name(column)
            result.append(f"{column_name}")
        return result

    def get_hash_columns(self):
        results = [
            self._resolve_name(column)
            for column in self.schema
            if column.get("hash", True) is True and column.get('identity', False) is False
        ]
        return results

    def get_text_columns(self):
        return [
            self._resolve_name(column)
            for column in self.schema
            if self._resolve_type(column) in self.text_column_types
        ]

    def get_datetime_columns(self):
        return [
            self._resolve_name(column)
            for column in self.schema
            if self._resolve_type(column) in self.datetime_column_types
        ]

    def get_datetime_columns_as_source(self):
        return [
            column["name"]
            for column in self.schema
            if self._resolve_type(column) in self.datetime_column_types
        ]

    def get_numeric_columns(self) -> list[str]:
        ident_cols = self.get_identity_columns()
        return [
            self._resolve_name(column)
            for column in self.schema
            if self._resolve_type(column) in self.number_column_types
            and self._resolve_name(column) not in ident_cols
        ]

    def get_identity_columns(self) -> list[str]:
        return [
            self._resolve_name(column)
            for column in self.schema
            if column.get("identity", False) is True
        ]

    def get_sql_columns_with_cast(self, include_identity=False) -> list[str]:
        result = []
        for column in self.schema:
            if column.get("identity", False) is True and not include_identity:
                continue
            column_name = self._resolve_name(column)
            column_type = column.get('type')
            if column_type in self.xml_column_types:
                result.append(f"""
                            (
                                CAST(
                                    {self._resolve_name(column)}
                                    AS {column_type}
                                )
                            ) AS {self._resolve_name(column)}
                            """)
                
            elif column_type in self.geography_column_types + self.geometry_column_types:
                srid = column.get('srid', 4326)
                result.append(f"{column_type}::STGeomFromWKB({column_name}, {srid}) AS {column_name}")
            else:
                result.append(f"{column_name} AS {column_name}") 
        return result

    def get_sql_columns(self, staging: bool | None = None) -> list[str]:
        if staging is None:
            staging = False

        columns = []
        for column in self.schema:
            to_add = None

            column_name = self._resolve_name(column)
            data = column
            data_type = self._resolve_type(data)

            if staging and data_type in self._stage_as_binary:
                data_type = "VARBINARY"

            if data_type in (self.text_column_types + self.binary_column_types):
                length = data.get("length", "MAX")
                to_add = f"{column_name} {data_type}({length})"

            elif data_type in self.decimal_column_types:
                precision = data.get("precision", 18)
                scale = data.get("scale", 0)
                to_add = f"{column_name} {data_type}({precision}, {scale})"

            else:
                to_add = f"{column_name} {data_type}"

            if data.get("identity", False):
                to_add += " IDENTITY(1,1)"

            columns.append(to_add)

        return columns

    def get_rename_dict(self) -> dict[str, str]:
        return {column["name"]: self._resolve_name(column) for column in self.schema}

    @staticmethod
    def get_asset_schema_from_db(
        connection: Connection, schema: str, table: str, exclude_columns: list[str] = []
    ) -> "AssetSchema | None":
        sql_logic = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM 
            INFORMATION_SCHEMA.columns
        WHERE table_schema = '{schema}' and TABLE_NAME = '{table}'
        """

        cursor = connection.execute(text(sql_logic))
        result = cursor.fetchall()
        if len(result) == 0:
            return None

        result_schema = []

        for column in result:
            column_name, data_type, str_length, precision, scale = column

            if column_name in exclude_columns:
                continue

            data_type = data_type.upper()
            base_result = {
                "name": column_name,
                "type": data_type.upper(),
            }

            if data_type in AssetSchema.text_column_types:
                if str_length is not None and str_length != -1:
                    base_result["length"] = str_length
            elif data_type in AssetSchema.decimal_column_types:
                if precision is not None:
                    base_result["precision"] = precision
                if scale is not None:
                    base_result["scale"] = scale
            elif data_type in AssetSchema.binary_column_types:
                if str_length is not None:
                    base_result["length"] = str_length
                else:
                    base_result["length"] = 'MAX'

            result_schema.append(base_result)

        return AssetSchema(result_schema)

    def add_column(self, column: dict):
        self.schema.append(column)
        self.validate_asset_schema()
