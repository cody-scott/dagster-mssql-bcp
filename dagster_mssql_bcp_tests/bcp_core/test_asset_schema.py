from dagster_mssql_bcp.bcp_core import asset_schema

import pytest
from contextlib import contextmanager
from sqlalchemy import create_engine, URL, text
import os


class TestAssetSchema:
    @contextmanager
    def connect_mssql(self):
        config = self.get_database_connection()
        connection_url = URL(
            "mssql+pyodbc",
            username=config.get("username"),
            password=config.get("password"),
            host=config.get("host"),
            port=int(config.get("port", "1433")),
            database=config.get("database"),
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
            },  # type: ignore
        )
        with create_engine(
            connection_url, fast_executemany=True, hide_parameters=True
        ).begin() as conn:
            yield conn

    def get_database_connection(self) -> dict[str, str]:
        db_config = dict(
            host=os.getenv("TARGET_DB__HOST", ""),
            port=os.getenv("TARGET_DB__PORT", "1433"),
            database=os.getenv("TARGET_DB__DATABASE", ""),
            username=os.getenv("TARGET_DB__USERNAME", ""),
            password=os.getenv("TARGET_DB__PASSWORD", ""),
        )

        return db_config

    @pytest.fixture
    def asset_schema_fixture(self):
        return [
            {"name": "a", "type": "BIGINT"},
            {"name": "b", "type": "BIGINT"},
            {"name": "c", "type": "NVARCHAR"},
            {"name": "d", "type": "DECIMAL"},
            {"name": "e", "type": "DATETIME"},
            {"name": "f", "type": "DATETIME2"},
            {"name": "g", "type": "NVARCHAR", "length": 10},
            {"name": "h", "type": "NVARCHAR", "length": 10, "hash": False},
        ]

    def test_get_numeric_column(self, asset_schema_fixture):
        schema = asset_schema.AssetSchema(asset_schema_fixture)
        assert schema.get_numeric_columns() == ["a", "b", "d"]
        schema = asset_schema.AssetSchema(
            [
                {"name": "c", "type": "NVARCHAR"},
            ]
        )

        assert schema.get_numeric_columns() == []

    def test_get_datetime_columns(self, asset_schema_fixture):
        schema = asset_schema.AssetSchema(asset_schema_fixture)
        assert schema.get_datetime_columns() == ["e", "f"]

    def test_columns(self, asset_schema_fixture):
        schema = asset_schema.AssetSchema(asset_schema_fixture)

        result = [
            "a BIGINT",
            "b BIGINT",
            "c NVARCHAR(MAX)",
            "d DECIMAL(18, 0)",
            "e DATETIME",
            "f DATETIME2",
            "g NVARCHAR(10)",
            "h NVARCHAR(10)",
        ]

        assert schema.get_sql_columns() == result

        schema.add_column({"name": "i", "type": "INT", "identity": True})
        assert schema.get_sql_columns() == result + ["i INT IDENTITY(1,1)"]

    def test_get_columns(self, asset_schema_fixture):
        schema = asset_schema.AssetSchema(asset_schema_fixture)
        assert schema.get_columns() == ["a", "b", "c", "d", "e", "f", "g", "h"]

        schema = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT", "identity": True},
                {"name": "b", "type": "NVARCHAR", "length": 10},
            ]
        )
        assert schema.get_columns() == ["b"]
        assert schema.get_columns(True) == ["a", "b"]

        schema = asset_schema.AssetSchema(
            [
                {"name": "intersection_id", "type": "NVARCHAR", "length": 200},
                {
                    "name": "timestamp",
                    "alias": "data_timestamp",
                    "type": "DATETIME2",
                },
                {
                    "name": "data_timestamp_with_offset",
                    "type": "DATETIMEOFFSET",
                },
                {
                    "name": "class",
                    "type": "NVARCHAR",
                    "length": 100,
                },
                {
                    "name": "crosswalkSide",
                    "type": "NVARCHAR",
                    "length": 100,
                },
                {"name": "direction", "type": "NVARCHAR", "length": 100},
                {"name": "qty", "type": "BIGINT"},
            ]
        )
        assert schema.get_columns() == [
            "intersection_id",
            "data_timestamp",
            "data_timestamp_with_offset",
            "class",
            "crosswalkSide",
            "direction",
            "qty",
        ]

    def test_get_hash_columns(self, asset_schema_fixture):
        schema = asset_schema.AssetSchema(asset_schema_fixture)
        assert schema.get_hash_columns() == ["a", "b", "c", "d", "e", "f", "g"]

        schema = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "NVARCHAR", "length": 10, "hash": False},
                {"name": "c", "type": "NVARCHAR", "length": 10, "hash": True},
            ]
        )
        assert schema.get_hash_columns() == ["a", "c"]

    def test_resolve_name(self):
        assert (
            asset_schema.AssetSchema._resolve_name(
                {"name": "a"},
            )
            == "a"
        )
        assert (
            asset_schema.AssetSchema._resolve_name(
                {"name": "b", "alias": "c"},
            )
            == "c"
        )

    def test_rename_columns(self):
        schema = asset_schema.AssetSchema(
            [
                {"name": "a", "alias": "b", "type": "NVARCHAR"},
                {"name": "c", "alias": "d", "type": "NVARCHAR"},
                {"name": "e", "type": "NVARCHAR"},
            ]
        )
        result = schema.get_rename_dict()

        assert result == {"a": "b", "c": "d", "e": "e"}

    def test_eq(self):
        a = asset_schema.AssetSchema([{"name": "a", "type": "BIGINT"}])
        b = asset_schema.AssetSchema([{"name": "a", "type": "BIGINT"}])
        assert a == b

        a = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
            ]
        )
        b = asset_schema.AssetSchema(
            [
                {"name": "b", "type": "BIGINT"},
                {"name": "a", "type": "BIGINT"},
            ]
        )
        assert a == b

        a = asset_schema.AssetSchema([{"name": "a", "type": "BIGINT"}])
        b = asset_schema.AssetSchema(
            [
                {"name": "b", "type": "BIGINT"},
                {"name": "a", "type": "BIGINT"},
            ]
        )
        assert a != b

    def test_asset_schema_from_db(self):
        expected_schema = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b_1", "type": "NVARCHAR"},
                {"name": "b_2", "type": "NVARCHAR", "length": 10},
                {"name": "b_3", "type": "VARCHAR"},
                {"name": "b_4", "type": "VARCHAR", "length": 10},
                {"name": "c_1", "type": "DECIMAL", "precision": 18, "scale": 0},
                {"name": "c_2", "type": "DECIMAL", "precision": 20, "scale": 0},
                {"name": "c_3", "type": "DECIMAL", "precision": 18, "scale": 1},
                {"name": "c_4", "type": "DECIMAL", "precision": 20, "scale": 1},
                {"name": "d_1", "type": "DATETIME"},
                {"name": "d_2", "type": "DATETIME2"},
                {"name": "d_3", "type": "DATETIMEOFFSET"},
                {"name": "e", "type": "BIT"},
                {"name": "f_1", "type": "NUMERIC", "precision": 18, "scale": 0},
                {"name": "f_2", "type": "NUMERIC", "precision": 20, "scale": 0},
                {"name": "f_3", "type": "NUMERIC", "precision": 18, "scale": 1},
                {"name": "f_4", "type": "NUMERIC", "precision": 20, "scale": 1},
                {"name": "g", "type": "BIT"},
            ]
        )
        schema = "test_asset_schema"
        table = "test_asset_table"

        with self.connect_mssql() as connection:
            create_schema = f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
            BEGIN
                EXEC('CREATE SCHEMA {schema}')
            END
            """

            drop_sql = f"DROP TABLE IF EXISTS {schema}.{table}"

            create_sql = f"""
            CREATE TABLE {schema}.{table} (
                a BIGINT,
                b_1 NVARCHAR(MAX),
                b_2 NVARCHAR(10),
                b_3 VARCHAR(MAX),
                b_4 VARCHAR(10),
                c_1 DECIMAL(18, 0),
                c_2 DECIMAL(20, 0),
                c_3 DECIMAL(18, 1),
                c_4 DECIMAL(20, 1),
                d_1 DATETIME,
                d_2 DATETIME2,
                d_3 DATETIMEOFFSET,
                e BIT,
                f_1 NUMERIC(18, 0),
                f_2 NUMERIC(20, 0),
                f_3 NUMERIC(18, 1),
                f_4 NUMERIC(20, 1),
                g BIT
            )
            """

            connection.execute(text(create_schema))
            connection.execute(text(drop_sql))
            connection.execute(text(create_sql))

        with self.connect_mssql() as connection:
            asset_schema_obj = asset_schema.AssetSchema.get_asset_schema_from_db(
                connection, schema, table
            )
            drop_sql = f"DROP TABLE IF EXISTS {schema}.{table}"
            connection.execute(text(drop_sql))

        assert asset_schema_obj == expected_schema

    def test_add_column(self):
        schema = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT"},
                {"name": "c", "type": "NVARCHAR"},
                {"name": "d", "type": "DECIMAL"},
                {"name": "e", "type": "DATETIME"},
                {"name": "f", "type": "DATETIME2"},
                {"name": "g", "type": "NVARCHAR", "length": 10},
                {"name": "h", "type": "NVARCHAR", "length": 10, "hash": False},
            ]
        )

        schema.add_column({"name": "i", "type": "BIGINT"})

        result = [
            "a BIGINT",
            "b BIGINT",
            "c NVARCHAR(MAX)",
            "d DECIMAL(18, 0)",
            "e DATETIME",
            "f DATETIME2",
            "g NVARCHAR(10)",
            "h NVARCHAR(10)",
            "i BIGINT",
        ]

        assert schema.get_sql_columns() == result

    def test_get_identity_columns(self):
        schema = asset_schema.AssetSchema(
            [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BIGINT", "identity": True},
            ]
        )

        assert schema.get_identity_columns() == ["b"]

    def test_validate_schema(self):
        good = [
            {"name": "a", "type": "BIGINT"},
            {"name": "b", "type": "BIGINT", "identity": True},
        ]
        asset_schema.AssetSchema(good)

        duplicate_columns = [
            {"name": "a", "type": "BIGINT"},
            {"name": "a", "type": "BIGINT"},
        ]
        with pytest.raises(ValueError) as ae:
            asset_schema.AssetSchema(duplicate_columns)
        assert "Duplicate column name: a" in str(ae.value)

        duplicate_columns = [
            {"name": "a", "type": "BIGINT"},
            {"name": "b", 'alias': 'a', "type": "BIGINT"},
        ]
        with pytest.raises(ValueError) as ae:
            asset_schema.AssetSchema(duplicate_columns)
        assert "Duplicate column name: b alias as a" in str(ae.value)

        missing_name = [
            {"type": "BIGINT"},
        ]
        with pytest.raises(ValueError) as ae:
            asset_schema.AssetSchema(missing_name)
        assert "Column name not provided for column: {'type': 'BIGINT'}" in str(ae.value)

        missing_type = [
            {"name": "a"},
        ]
        with pytest.raises(ValueError) as ae:
            asset_schema.AssetSchema(missing_type)
        assert "Column type not provided for column: a" in str(ae.value)

        invalid_type = [
            {"name": "a", "type": "INVALID"},
        ]
        with pytest.raises(ValueError) as ae:
            asset_schema.AssetSchema(invalid_type)
        assert "Invalid data type: INVALID" in str(ae.value)


    def test_staging_types(self):
        schema = [
            {'name': 'a', 'type': 'NVARCHAR', 'length': 100},
            {'name': 'b', 'type': 'XML'},
        ]

        asset_schema_obj = asset_schema.AssetSchema(schema)

        assert asset_schema_obj.get_sql_columns() == [
            'a NVARCHAR(100)',
            'b XML',
        ]
        assert asset_schema_obj.get_sql_columns(True) == [
            'a NVARCHAR(100)',
            'b VARBINARY(MAX)',
        ]