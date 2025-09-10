import os
from pathlib import Path
from unittest.mock import patch

from py_load_eurostat.config import AppSettings, DatabaseType


def test_settings_load_from_env_file(tmp_path: Path):
    """
    Verify that settings are correctly loaded from a .env file.
    """
    # 1. Create a dummy .env file in a temporary directory
    env_content = (
        'PY_LOAD_EUROSTAT_DB_TYPE="sqlite"\n'
        'PY_LOAD_EUROSTAT_DB__NAME="testdb_from_env"\n'
        'PY_LOAD_EUROSTAT_LOG__LEVEL="DEBUG"\n'
    )
    env_file = tmp_path / ".env"
    env_file.write_text(env_content)

    # 2. Instantiate the settings object, passing the path to the .env file directly.
    settings = AppSettings(_env_file=env_file)

    # 3. Assert that the values were loaded correctly
    assert settings.db_type == DatabaseType.SQLITE
    assert settings.db.name == "testdb_from_env"
    assert settings.log.level == "DEBUG"


def test_settings_env_vars_override_env_file(tmp_path: Path, monkeypatch):
    """
    Verify that environment variables take precedence over .env file settings.
    """
    # 1. Create a dummy .env file
    env_content = 'PY_LOAD_EUROSTAT_DB__NAME="name_from_file"\n'
    env_file = tmp_path / ".env"
    env_file.write_text(env_content)

    # 2. Set an environment variable for the same setting
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__NAME", "name_from_env_var")

    # Instantiate the settings object, passing the path to the .env file
    settings = AppSettings(_env_file=env_file)

    # 3. Assert that the environment variable's value was used, overriding the file
    assert settings.db.name == "name_from_env_var"
