import pytest
import pymysql


@pytest.fixture(scope="session")
def db_connection():
    """
    Session-wide DB connection.
    Adjust host/user/password/database as per your environment.
    """
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Citi@123",
        database="banking_dw_practice",
        autocommit=True,
    )
    yield conn
    conn.close()


@pytest.fixture
def cursor(db_connection):
    """
    Function-level cursor fixture.
    Each test gets a fresh cursor; the DB itself is shared.
    """
    with db_connection.cursor() as cur:
        yield cur
