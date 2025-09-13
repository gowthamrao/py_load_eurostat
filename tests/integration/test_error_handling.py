from typer.testing import CliRunner

from py_load_eurostat.cli import app


def test_pipeline_network_error(httpserver, monkeypatch, tmp_path):
    """
    Test that the pipeline handles network errors gracefully.
    """
    # 1. Setup a mock server that returns a 500 error
    httpserver.expect_request(
        "/eurostat/api/dissemination/statistics/1.0/data/tps00001"
    ).respond_with_data("Internal Server Error", 500)
    monkeypatch.setenv("PY_LOAD_EUROSTAT_EUROSTAT__BASE_URL", httpserver.url_for("/"))
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__NAME", str(db_path))
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB_TYPE", "sqlite")

    # 2. Run the pipeline
    runner = CliRunner()
    result = runner.invoke(app, ["run", "--dataset-id", "tps00001"])

    # 3. Assert that the pipeline failed
    assert result.exit_code != 0
    assert "Pipeline for tps00001 failed." in result.output


def test_pipeline_db_connection_error(monkeypatch, tmp_path):
    """
    Test that the pipeline handles database connection errors gracefully.
    """
    # 1. Setup invalid database credentials
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB_TYPE", "postgres")
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__HOST", "invalid-host")
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__PASSWORD", "password")

    # 2. Run the pipeline
    runner = CliRunner()
    result = runner.invoke(app, ["run", "--dataset-id", "tps00001"])

    # 3. Assert that the pipeline failed
    assert result.exit_code != 0
    assert "Pipeline for tps00001 failed." in result.output


def test_pipeline_parsing_error(httpserver, monkeypatch, tmp_path):
    """
    Test that the pipeline handles parsing errors gracefully.
    """
    # 1. Setup a mock server that returns a malformed DSD file
    httpserver.expect_request(
        "/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/tps00001"
    ).respond_with_data("malformed xml", 200)
    monkeypatch.setenv("PY_LOAD_EUROSTAT_EUROSTAT__BASE_URL", httpserver.url_for("/"))
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__NAME", str(db_path))
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB_TYPE", "sqlite")

    # 2. Run the pipeline
    runner = CliRunner()
    result = runner.invoke(app, ["run", "--dataset-id", "tps00001"])

    # 3. Assert that the pipeline failed
    assert result.exit_code != 0
    assert "Pipeline for tps00001 failed." in result.output
