# Eurostat Loader

`eurostat-loader` is a high-performance, extensible Python package for downloading, transforming, and bulk-loading statistical data from Eurostat into a relational database. It is designed to handle very large datasets efficiently by leveraging native database bulk-loading capabilities.

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
    git clone https://github.com/example/eurostat-loader.git
    cd eurostat-loader
    ```

2.  Install the package. It is recommended to do this in a virtual environment.
    ```bash
    pip install .
    ```

## Configuration

The application is configured entirely through environment variables. The following variables are required to connect to the target PostgreSQL database:

| Environment Variable             | Description                                  | Default       |
| -------------------------------- | -------------------------------------------- | ------------- |
| `EUROSTAT_LOADER_DB__HOST`       | The hostname of the database server.         | `localhost`   |
| `EUROSTAT_LOADER_DB__PORT`       | The port of the database server.             | `5432`        |
| `EUROSTAT_LOADER_DB__USER`       | The username for the database connection.    | `postgres`    |
| `EUROSTAT_LOADER_DB__PASSWORD`   | The password for the database connection.    | **Required**  |
| `EUROSTAT_LOADER_DB__NAME`       | The name of the database to connect to.      | `eurostat`    |
| `EUROSTAT_LOADER_CACHE__PATH`    | Filesystem path for caching downloads.       | `~/.cache/eurostat-loader` |
| `EUROSTAT_LOADER_CACHE__ENABLED` | Set to `false` to disable caching.           | `true`        |
| `EUROSTAT_LOADER_LOG__LEVEL`     | The logging level (e.g., `INFO`, `DEBUG`).   | `INFO`        |

## Usage

The primary way to use the loader is via its command-line interface.

```bash
eurostat-loader run [OPTIONS]
```

**Example:**

To perform a full load of the `nama_10_gdp` dataset, replacing any existing data:

```bash
export EUROSTAT_LOADER_DB__PASSWORD="your_secret_password"

eurostat-loader run --dataset-id "nama_10_gdp"
```

### CLI Options

*   `-d`, `--dataset-id TEXT`: The Eurostat dataset identifier (e.g., `nama_10_gdp`). **[required]**
*   `-r`, `--representation TEXT`: The data representation: `"Standard"` (coded) or `"Full"` (labeled). [default: Standard]
*   `-s`, `--load-strategy TEXT`: The load strategy: `"Full"` (replaces entire dataset) or `"Delta"` (loads if source is newer). [default: Full]

## Architecture Overview

The application follows a classic, decoupled ETL pipeline design:

1.  **Fetcher**: Handles all communication with Eurostat APIs, including downloading data and metadata, caching, and network retries.
2.  **Parser**: Interprets the raw downloaded files—both the SDMX-ML (XML) for metadata and the unique Eurostat TSV format for data.
3.  **Transformer**: Unpivots the wide-format TSV data into a tidy, long format. It separates observation values from flags and can replace codes with human-readable labels.
4.  **Loader**: The database adapter layer. It prepares the database schema and uses native bulk-loading commands to ingest the transformed data stream efficiently.

## How to Add a New Database Adapter

The system is designed to be easily extended with new database adapters.

1.  **Create a New Loader Class:** Create a new file in `src/eurostat_loader/loader/` (e.g., `snowflake.py`). In this file, define a new class (e.g., `SnowflakeLoader`).

2.  **Implement the Interface:** Your new class must inherit from `LoaderInterface` (from `src.eurostat_loader.loader.base`) and implement all its abstract methods:
    *   `prepare_schema(...)`
    *   `manage_codelists(...)`
    *   `bulk_load_staging(...)`
    *   `finalize_load(...)`
    *   `get_ingestion_state(...)`
    *   `save_ingestion_state(...)`
    *   `close_connection(...)`

3.  **Use Native Bulk Loading:** The key requirement for any new loader is to implement `bulk_load_staging` using the target database's most efficient bulk-loading mechanism (e.g., Snowflake's `COPY INTO @...` from an S3 stage).

4.  **Update the Pipeline:** In `pipeline.py`, you would add logic to select the desired loader based on a configuration setting.

Refer to `src/eurostat_loader/loader/postgresql.py` for the canonical example of a `LoaderInterface` implementation.

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
