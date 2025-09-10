# Py Load Eurostat

`py-load-eurostat` is a high-performance, extensible Python package for downloading, transforming, and bulk-loading statistical data from Eurostat into a relational database. It is designed to handle very large datasets efficiently by leveraging native database bulk-loading capabilities.

This project was built based on the detailed Functional Requirements Document (FRD) provided.

## Key Features

*   **High-Performance Bulk Loading:** Uses native `COPY` command for PostgreSQL to ensure maximum data ingestion speed, avoiding slow row-by-row `INSERT` statements.
*   **Extensible Architecture:** Built with an adapter pattern (`LoaderInterface`) that allows easy extension to support other database backends (e.g., Redshift, Snowflake, BigQuery) without changing the core logic.
*   **Efficient Data Processing:** The entire pipeline, from file download to transformation, is streamed to handle multi-gigabyte datasets with minimal memory consumption.
*   **Modern Tooling:** Developed with modern Python best practices, using `pydantic` for configuration, `typer` for a clean CLI, and `ruff` for formatting and linting.
*   **Robust and Resilient:** Implements automatic retries with exponential backoff for network requests to handle transient API issues.
*   **Comprehensive Testing:** Includes a full suite of unit and integration tests, using `testcontainers` to validate against a real PostgreSQL database.

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/example/py-load-eurostat.git
    cd py-load-eurostat
    ```

2.  Install the package. It is recommended to do this in a virtual environment.
    ```bash
    pip install .
    ```

## Configuration

The application is configured entirely through environment variables. For local development, you can create a `.env` file in the project root to store these variables. The application will automatically load it.

See the `.env.example` file for a complete template.

Environment variables will always take priority over values loaded from a `.env` file.

The following variables are available:

| Environment Variable                  | Description                                            | Default                     |
| ------------------------------------- | ------------------------------------------------------ | --------------------------- |
| `PY_LOAD_EUROSTAT_DB_TYPE`            | The database type (`postgres` or `sqlite`).            | `postgres`                  |
| `PY_LOAD_EUROSTAT_DB__HOST`           | The hostname of the database server.                   | `localhost`                 |
| `PY_LOAD_EUROSTAT_DB__PORT`           | The port of the database server.                       | `5432`                      |
| `PY_LOAD_EUROSTAT_DB__USER`           | The username for the database connection.              | `postgres`                  |
| `PY_LOAD_EUROSTAT_DB__PASSWORD`       | The password for the database connection.              | **Required**                |
| `PY_LOAD_EUROSTAT_DB__NAME`           | The name of the database to connect to.                | `eurostat`                  |
| `PY_LOAD_EUROSTAT_DB__USE_UNLOGGED_TABLES` | Use unlogged tables for staging in PostgreSQL.    | `true`                      |
| `PY_LOAD_EUROSTAT_MANAGED_DATASETS_PATH` | Path to the YAML file listing datasets to manage.      | `managed_datasets.yml`      |
| `PY_LOAD_EUROSTAT_CACHE__PATH`        | Filesystem path for caching downloads.                 | `~/.cache/py-load-eurostat` |
| `PY_LOAD_EUROSTAT_CACHE__ENABLED`     | Set to `false` to disable caching.                     | `true`                      |
| `PY_LOAD_EUROSTAT_LOG__LEVEL`         | The logging level (e.g., `INFO`, `DEBUG`).             | `INFO`                      |
| `PY_LOAD_EUROSTAT_EUROSTAT__BASE_URL` | The base URL for the Eurostat Dissemination API.       | `https://ec.europa.eu/eurostat/api/dissemination` |

## Usage

The primary way to use the loader is via its command-line interface.

```bash
py-load-eurostat run [OPTIONS]
```

**Example:**

To perform a full load of the `nama_10_gdp` dataset, replacing any existing data:

```bash
export PY_LOAD_EUROSTAT_DB__PASSWORD="your_secret_password"

py-load-eurostat run --dataset-id "nama_10_gdp"
```

### CLI Options

*   `-d`, `--dataset-id TEXT`: The Eurostat dataset identifier (e.g., `nama_10_gdp`). **[required]**
*   `-r`, `--representation TEXT`: The data representation: `"Standard"` (coded) or `"Full"` (labeled). [default: Standard]
*   `-s`, `--load-strategy TEXT`: The load strategy: `"Full"` (replaces entire dataset) or `"Delta"` (loads if source is newer). [default: Full]

### Batch Processing (Update All)

For managing multiple datasets efficiently, you can use the `update-all` command. This command will check for updates for a list of datasets you define and only run the ingestion pipeline for those that are new or have been updated.

```bash
py-load-eurostat update-all
```

This command requires a configuration file that lists the datasets to manage.

1.  **Create a `managed_datasets.yml` file:**
    Copy the provided `managed_datasets.yml.example` to `managed_datasets.yml` and edit it to include the dataset IDs you want to track.

    ```yaml
    # managed_datasets.yml
    datasets:
      - nama_10_gdp
      - tps00001
      - env_air_gge
    ```

2.  **Run the command:**
    With your `.env` file configured and `managed_datasets.yml` in place, simply run the command. The application will automatically find the files.

    ```bash
    py-load-eurostat update-all
    ```

The command will log which datasets are skipped, which are updated, and which fail, providing a clear summary at the end.

## Architecture Overview

The application follows a classic, decoupled ETL pipeline design:

1.  **Fetcher**: Handles all communication with Eurostat APIs, including downloading data and metadata, caching, and network retries.
2.  **Parser**: Interprets the raw downloaded files—both the SDMX-ML (XML) for metadata and the unique Eurostat TSV format for data.
3.  **Transformer**: Unpivots the wide-format TSV data into a tidy, long format. It separates observation values from flags and can replace codes with human-readable labels.
4.  **Loader**: The database adapter layer. It prepares the database schema and uses native bulk-loading commands to ingest the transformed data stream efficiently.

## How to Add a New Database Adapter

The system is designed to be easily extended with new database adapters.

1.  **Create a New Loader Class:** Create a new file in `src/py_load_eurostat/loader/` (e.g., `snowflake.py`). In this file, define a new class (e.g., `SnowflakeLoader`).

2.  **Implement the Interface:** Your new class must inherit from `LoaderInterface` (from `src.py_load_eurostat.loader.base`) and implement all its abstract methods:
    *   `prepare_schema(...)`
    *   `manage_codelists(...)`
    *   `bulk_load_staging(...)`
    *   `finalize_load(...)`
    *   `get_ingestion_state(...)`
    *   `save_ingestion_state(...)`
    *   `close_connection(...)`

3.  **Use Native Bulk Loading:** The key requirement for any new loader is to implement `bulk_load_staging` using the target database's most efficient bulk-loading mechanism (e.g., Snowflake's `COPY INTO @...` from an S3 stage).

4.  **Update the Pipeline:** In `pipeline.py`, you would add logic to select the desired loader based on a configuration setting.

Refer to `src/py_load_eurostat/loader/postgresql.py` for the canonical example of a `LoaderInterface` implementation.

## Running Tests

To run the test suite, first install the development dependencies:

```bash
pip install .[dev]
```

Then, run `pytest`:

```bash
pytest
```

The integration tests require Docker to be running, as they will automatically spin up a PostgreSQL container to run against.
