"""
Pytest configuration and shared fixtures.
"""

import os

import pytest
from testcontainers.postgres import PostgresContainer

from py_load_eurostat.config import DatabaseSettings


@pytest.fixture(scope="session", autouse=True)
def mock_env():
    """
    An autouse session fixture to set a dummy database password.
    This prevents pydantic validation errors when the application modules
    are first imported by the test runner. The value is overridden by the
    db_settings fixture for actual tests.
    """
    os.environ["PY_LOAD_EUROSTAT_DB__PASSWORD"] = "dummy_password_for_import"
    yield
    del os.environ["PY_LOAD_EUROSTAT_DB__PASSWORD"]


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    A session-scoped fixture that starts and stops a PostgreSQL container.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """
    Provides a DatabaseSettings object configured to connect to the
    test container.
    """
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        name=postgres_container.dbname,
    )
