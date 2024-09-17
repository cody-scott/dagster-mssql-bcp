from contextlib import contextmanager
from typing import Any, Generator

from sqlalchemy import URL, Connection, create_engine


@contextmanager
def connect_mssql(
    config: dict[str, Any], connection_only=False
) -> Generator[Connection, None, None]:
    """
    Connects to a Microsoft SQL Server database using the provided configuration.

    Args:
        config (dict): A dictionary containing the connection configuration parameters.
        connection_only (bool, optional): If True, only a connection object is returned. If False, a transactional connection is returned. Defaults to False.

    Yields:
        Connection: A connection object to the Microsoft SQL Server database.

    """
    connection_url = URL(**config)

    conn = None
    if connection_only:
        with create_engine(connection_url, hide_parameters=True).connect() as conn:
            yield conn
    else:
        with create_engine(
            connection_url, fast_executemany=True, hide_parameters=True
        ).begin() as conn:
            yield conn
