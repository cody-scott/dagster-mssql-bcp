from dagster_mssql_bcp.bcp_core.bcp_core import BCPConnectionConfig, _convert_query_props_to_string
import pytest
from pydantic import ValidationError


def test_bcp_config():
    cfg = BCPConnectionConfig(
        host='abc', port='123', database='test'
    )
    assert cfg.staging_database == cfg.database

    cfg = BCPConnectionConfig(
        host='abc', port='123', database='test', staging_database='stg'
    )
    assert cfg.staging_database == 'stg'


def test_returns_empty_dict_when_query_props_is_none():
    result = _convert_query_props_to_string(None)

    assert result == {}


def test_converts_multisubnetfailover_boolean_to_title_case_yes_no():
    query_props = {"MultiSubnetFailover": True}

    result = _convert_query_props_to_string(query_props)

    assert result == {"MultiSubnetFailover": "Yes"}


def test_converts_multisubnetfailover_false_to_title_case_no():
    query_props = {"multisubnetfailover": False}

    result = _convert_query_props_to_string(query_props)

    assert result == {"multisubnetfailover": "No"}


def test_converts_other_boolean_values_to_lowercase_yes_no():
    query_props = {
        "Encrypt": True,
        "TrustServerCertificate": False,
    }

    result = _convert_query_props_to_string(query_props)

    assert result == {
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
    }


def test_leaves_non_boolean_values_unchanged():
    query_props = {
        "ApplicationIntent": "ReadOnly",
        "ConnectTimeout": 30,
        "Server": "sql01",
    }

    result = _convert_query_props_to_string(query_props)

    assert result == {
        "ApplicationIntent": "ReadOnly",
        "ConnectTimeout": "30",
        "Server": "sql01",
    }


def test_mixed_values_are_converted_correctly():
    query_props = {
        "MultiSubnetFailover": True,
        "Encrypt": False,
        "ApplicationIntent": "ReadOnly",
        "Port": 1433,
    }

    result = _convert_query_props_to_string(query_props)

    assert result == {
        "MultiSubnetFailover": "Yes",
        "Encrypt": "no",
        "ApplicationIntent": "ReadOnly",
        "Port": "1433",
    }


def test_key_matching_for_multisubnetfailover_is_case_insensitive():
    query_props = {"MuLtIsUbNeTfAiLoVeR": True}

    result = _convert_query_props_to_string(query_props)

    assert result == {"MuLtIsUbNeTfAiLoVeR": "Yes"}


def test_function_mutates_input_dict():
    query_props = {"Encrypt": True}

    result = _convert_query_props_to_string(query_props)

    assert result is query_props
    assert query_props == {"Encrypt": "yes"}


def make_connection(**overrides):
    data = {
        "host": "sql01",
        "port": "1433",
        "database": "DW",
    }
    data.update(overrides)
    return BCPConnectionConfig(**data) # type: ignore


def test_staging_database_defaults_to_database():
    conn = make_connection()
    assert conn.staging_database == "DW"


def test_staging_database_preserves_explicit_value():
    conn = make_connection(staging_database="DW_STAGE")
    assert conn.staging_database == "DW_STAGE"


def test_query_props_converts_multisubnetfailover_to_yes_no():
    conn = make_connection(query_props={"MultiSubnetFailover": True})
    assert conn.query_props == {"MultiSubnetFailover": "Yes"}


def test_query_props_converts_other_booleans_to_lowercase_yes_no():
    conn = make_connection(
        query_props={
            "Encrypt": True,
            "TrustServerCertificate": False,
        }
    )
    assert conn.query_props == {
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
    }


def test_connection_config_builds_expected_dictionary():
    conn = make_connection(
        username="svc_bcp",
        password="secret",
        query_props={
            "Encrypt": True,
            "MultiSubnetFailover": False,
        },
    )

    assert conn.connection_config == {
        "drivername": "mssql+pyodbc",
        "username": "svc_bcp",
        "password": "secret",
        "host": "sql01",
        "port": "1433",
        "database": "DW",
        "query": {
            "driver": "ODBC Driver 18 for SQL Server",
            "Encrypt": "yes",
            "MultiSubnetFailover": "No",
        },
    }


def test_defaults_are_applied():
    conn = make_connection()

    assert conn.username is None
    assert conn.password is None
    assert conn.query_props == {}
    assert conn.bcp_arguments == {}
    assert conn.bcp_path == "bcp"
    assert conn.driver == "ODBC Driver 18 for SQL Server"
    assert conn.process_datetime is True
    assert conn.process_replacements is True
    assert conn.add_row_hash is True
    assert conn.add_load_datetime is True
    assert conn.add_load_uuid is True
    assert conn.add_identity_column is False
    assert conn.row_hash_column_name == "row_hash"
    assert conn.load_uuid_column_name == "load_uuid"
    assert conn.load_datetime_column_name == "load_datetime"
    assert conn.identity_column_name == "id"


def test_required_fields_are_enforced():
    with pytest.raises(ValidationError) as exc_info:
        BCPConnectionConfig() # type: ignore

    missing_fields = {error["loc"][0] for error in exc_info.value.errors()}
    assert {"host", "port", "database"} <= missing_fields
