# Functional Requirements Document (FRD) for py_load_eurostat

**Version:** 1.0
**Status:** Initial Draft
**Author:** Jules, Principal Data Architect

---

## 1. Introduction and Scope

### 1.1 Purpose of `py_load_eurostat`

The `py_load_eurostat` package provides a high-performance, extensible, and reliable solution for extracting, loading, and transforming public data from the Eurostat data portal into remote relational databases. Its primary purpose is to enable data scientists, analysts, and engineers to build and maintain a local, queryable replica of large-scale Eurostat datasets for analytical purposes, ensuring the data is fresh, accurate, and efficiently loaded.

### 1.2 Key Objectives

The development of this package is guided by four key objectives:

*   **Performance:** To achieve the highest possible data ingestion speeds by leveraging the native bulk loading capabilities of target databases, bypassing inefficient row-by-row insertion methods. The entire pipeline must be memory-efficient, capable of processing multi-gigabyte datasets on standard hardware by streaming data from source to target.
*   **Extensibility:** To establish a clean, modular architecture where the core data processing logic is decoupled from the database loading implementation. The system must be easily extendable to support various relational database systems (e.g., Redshift, BigQuery, Databricks SQL) via a well-defined adapter pattern, with PostgreSQL as the default implementation.
*   **Delta Accuracy:** To provide a robust and accurate mechanism for incremental (delta) updates. The delta mechanism must be based on source file modification timestamps as published by Eurostat, ensuring that local data mirrors the source's update cycle.
*   **Reproducibility:** To ensure that data loading operations are idempotent and reproducible. The package will use declarative configurations and maintain a detailed ingestion history to track every operation, its parameters, and its outcome.

### 1.3 In-scope and Out-of-scope Features

**In-Scope:**

*   **Data Acquisition from Eurostat Bulk Download Facility:** Prioritizing the new `api/dissemination/files` endpoint for efficient retrieval of compressed TSV data.
*   **Metadata Acquisition from Eurostat SDMX API:** Retrieving Data Structure Definitions (DSDs) and Code Lists via the SDMX REST API.
*   **Full and Delta Load Strategies:** Supporting both complete data refreshes and incremental updates based on source timestamps.
*   **Native Bulk Loading for PostgreSQL:** Default implementation must use the PostgreSQL `COPY` command.
*   **Extensible Database Loader Interface:** An Abstract Base Class defining the contract for all database-specific loaders.
*   **Dual Data Representations:** Support for loading data in both "Standard" (coded) and "Full" (labeled) formats.
*   **Automated Schema Management:** Creation and evolution of relational schemas based on SDMX metadata.
*   **Command-Line Interface (CLI):** Providing a user-friendly CLI for executing and configuring load jobs.

**Out-of-Scope:**

*   **Graphical User Interface (UI):** The package is intended for programmatic and CLI use.
*   **Real-time or Low-Latency Streaming:** The architecture is optimized for bulk/batch operations, not real-time data feeds.
*   **Support for Non-Relational Databases:** The initial design is strictly focused on relational, SQL-based databases.
*   **Row-Level Change Data Capture (CDC):** The delta strategy is file-based, not row-level.
*   **Data Transformation Beyond Normalization:** The package's scope is limited to transforming Eurostat's format into a tidy relational model, not complex business logic transformations.

---

## 2. System Architecture

### 2.1 Architectural Overview

The `py_load_eurostat` package is designed as a modular, streaming pipeline. This design decouples responsibilities into distinct components, each handling a specific stage of the ELT process. Data flows through the pipeline in memory-efficient chunks, allowing the system to process datasets that are much larger than the available system RAM.

The four core components are:
1.  **Fetcher:** Responsible for all communication with external Eurostat APIs. It handles downloading data and metadata, manages a local file cache to prevent redundant downloads, and implements resilient network retry mechanisms.
2.  **Parser:** Responsible for interpreting the raw files provided by Eurostat. It understands the specific formats of Eurostat's compressed TSV data files, the SDMX-ML (XML) metadata files (DSDs, Code Lists), and the Table of Contents (TOC) inventory file.
3.  **Transformer:** Takes the parsed raw data (which is in a wide, pivoted format) and transforms it into a normalized, "tidy" (long) format. It separates observation values from flags and can replace codes with human-readable labels. The output is a clean, standardized stream of data records.
4.  **Loader:** The final component, responsible for ingesting the transformed data stream into the target database. This component is designed around a database abstraction layer to support multiple database backends.

### 2.2 Database Abstraction Layer

To ensure extensibility, the system defines a formal contract for all database loading components through the `LoaderInterface` Abstract Base Class (ABC). This interface mandates the implementation of a set of methods that the core pipeline relies on to manage the loading process.

The `LoaderInterface` requires the following methods:
*   `prepare_schema(...)`: Ensures the target database schema and tables exist, creating or altering them as needed based on the DSD. Must be idempotent.
*   `manage_codelists(...)`: Ingests or updates metadata code lists in the database.
*   `bulk_load_staging(...)`: The core performance method. It must load a stream of data into a temporary/staging table using the database's most efficient native bulk ingestion mechanism.
*   `finalize_load(...)`: Atomically transitions the data from the staging table to the final production table, using either a "swap" or "merge" strategy.
*   `get_ingestion_state(...)`: Retrieves the timestamp and status of the last successful load for a given dataset to support the delta-load logic.
*   `save_ingestion_state(...)`: Records the outcome of a load operation (success or failure) in a dedicated history table.
*   `close_connection(...)`: Handles the graceful closing of database connections.

### 2.3 Extensibility (Adapters)

New database backends can be supported by creating a new "adapter" class that inherits from `LoaderInterface` and provides a concrete implementation for each of its abstract methods.

*   **Default PostgreSQL Adapter:** The default adapter for PostgreSQL utilizes the `psycopg` (v3) library. Its `bulk_load_staging` implementation streams data directly from the Transformer's output generator to the database using the `COPY FROM STDIN` command, which is the fastest ingestion method for PostgreSQL. It also supports using `UNLOGGED` tables for staging to further optimize write performance.
*   **Future Adapter Requirements:** Any future adapter (e.g., for Snowflake, Redshift, or BigQuery) *must* implement `bulk_load_staging` using the corresponding native bulk load mechanism. For example, a Redshift/Snowflake adapter would likely stream data to a file on S3/Blob storage and then issue a `COPY INTO` command. A BigQuery adapter would use the Load Jobs API.

---

## 3. Functional Requirements

### 3.1 Data Acquisition and Caching

*   **3.1.1 Eurostat Bulk Download:** The system must acquire observational data by downloading compressed TSV (`.tsv.gz`) files from the Eurostat Bulk Download facility, as specified in the Eurostat Table of Contents (TOC) or Catalogue API.
*   **3.1.2 Eurostat TSV Parsing:** The system must correctly parse the unique structure of Eurostat TSV files, which includes:
    *   A header row where the first column contains a comma-separated list of dimension identifiers, and subsequent columns represent time periods.
    *   Data rows where the first column contains a comma-separated list of dimension values corresponding to the header.
*   **3.1.3 Eurostat SDMX API:** The system must fetch metadata—specifically Data Structure Definitions (DSDs) and Code Lists—from the Eurostat SDMX REST API.
*   **3.1.4 Configurable Caching:** All downloaded files (TOC, DSDs, Code Lists, TSV data) must be cached on the local filesystem. The caching mechanism must be configurable, allowing the user to specify the cache location and enable/disable it. The system should avoid re-downloading files if a valid cache exists.

### 3.2 Metadata Synchronization (SDMX)

*   **3.2.1 DSD and Code List Parsing:** The system must parse the downloaded SDMX-ML (XML) files to extract a structured representation of the DSD (dimensions, attributes, measures) and all associated Code Lists (code-to-label mappings).
*   **3.2.2 Relational Schema Mapping:** The system must map the parsed SDMX structures to a relational schema.
    *   Each Eurostat dataset shall have its own data table (e.g., `data_nama_10_gdp`).
    *   Each Code List shall have its own metadata table (e.g., `meta_geo`).
    *   An ingestion history table (`_ingestion_history`) must be maintained.
*   **3.2.3 Metadata Versioning:** The version of the DSD used for an ingestion must be recorded in the ingestion history log to track metadata changes over time.

### 3.3 Data Transformation

*   **3.3.1 Data Unpivoting:** The system must transform the wide-format data from the TSV file into a normalized, long format ("Tidy Data"). Each row in the transformed output must represent a single observation. This shall be achieved by "melting" or "unpivoting" the time period columns.
*   **3.3.2 Observation Flag Handling:** The system must parse observation values that contain trailing alphabetic characters (flags). The numeric value and the flag(s) must be separated and stored in distinct columns in the database (e.g., `obs_value` and `obs_flags`).
*   **3.3.3 Data Type Mapping:** The system must map SDMX data types (e.g., `String`, `Double`, `Integer`) to appropriate native database column types during table creation.

### 3.4 Data Loading

*   **3.4.1 Full Load:** In "Full" mode, the system must perform a complete refresh of the dataset. This involves dropping the existing table (if it exists) and replacing it with the newly loaded data. The operation must be atomic to prevent data unavailability.
*   **3.4.2 Delta Load:** In "Delta" mode, the system must first check the `last_update` timestamp for the dataset from the Eurostat TOC. This timestamp is compared against the `source_last_update` timestamp from the last successful entry in the local ingestion history table. The download and load process shall be skipped if the local data is up-to-date or newer. If an update is detected, the system will perform a full replacement or merge of the dataset.
*   **3.4.3 Staging and Finalization:** All data loading must first be performed into a temporary staging table. Once the bulk load is complete, a `finalize_load` process must atomically make the data available.
    *   For `Full` loads, this will be a "swap" operation (e.g., `DROP TABLE old; RENAME TABLE new TO old;`).
    *   For `Delta` loads, this can be a "merge" operation (e.g., `INSERT ... ON CONFLICT DO UPDATE`), upserting new and changed records into the existing target table.
*   **3.4.4 Data Representations:** The system must support two loading representations, configurable by the user:
    *   **Standard (default):** Dimension columns contain the raw codes (e.g., `DE`, `FR`). This is the preferred mode for relational analysis, as it allows joining to the metadata tables.
    *   **Full:** Dimension columns contain the human-readable labels (e.g., `Germany`, `France`). The code-to-label replacement is performed during the transformation step.

---

## 4. Data Structure and Schema

### 4.1 Ingestion History Schema

A table named `_ingestion_history` must be created in a dedicated metadata schema to track all load operations. It shall contain the following columns:
*   `ingestion_id` (Primary Key)
*   `dataset_id`
*   `dsd_version`
*   `load_strategy` (Full/Delta)
*   `representation` (Standard/Full)
*   `status` (Success/Failed/Running)
*   `start_time` (UTC timestamp)
*   `end_time` (UTC timestamp)
*   `rows_loaded`
*   `source_last_update` (UTC timestamp from Eurostat TOC)
*   `error_details`

### 4.2 Eurostat Metadata Schema

All metadata tables shall be stored in a dedicated schema (e.g., `eurostat_meta`). Each Code List will be stored in its own table, named after the codelist ID (e.g., `geo`). The schema will include:
*   `code` (Primary Key)
*   `label_en`
*   `description_en`
*   `parent_code`

### 4.3 Observational Data Schema

Observational data tables will be stored in a dedicated data schema (e.g., `eurostat_data`). The table name will be derived from the dataset ID (e.g., `data_nama_10_gdp`). The columns will be dynamically generated based on the DSD:
*   One column for each dimension (e.g., `geo`, `indic_na`).
*   A `time_period` column.
*   A column for the primary measure (e.g., `obs_value`).
*   A column for observation flags (e.g., `obs_flags`).
*   The primary key will be a composite of all dimension columns plus the `time_period` column.

---

## 5. Non-Functional Requirements

### 5.1 Performance

*   **Memory Efficiency:** The entire pipeline must be designed to stream data in chunks, from initial file read to database loading, keeping memory consumption minimal and constant regardless of dataset size.
*   **Load Speed:** The system must utilize native, set-based database operations for all data and metadata loading to ensure maximum throughput. For PostgreSQL, this requires the use of the `COPY` command.

### 5.2 Reliability and Error Handling

*   **Network Resilience:** All external HTTP requests to Eurostat APIs must be wrapped in a retry mechanism with exponential backoff to handle transient network failures.
*   **Transactional Integrity:** All database modifications during the `finalize_load` step must be performed within a single atomic transaction to ensure the database is never left in an inconsistent state.
*   **Idempotency:** Data loading operations must be idempotent. Re-running a failed job or a completed job should not produce errors or duplicate data.

### 5.3 Configuration and Security

*   **Configuration:** The application shall be configurable via environment variables. This includes database connection details, cache settings, and logging levels.
*   **Security:** Database credentials, especially passwords, must not be hardcoded. The system will read them from environment variables at runtime.

### 5.4 Logging and Monitoring

*   **Structured Logging:** The system must implement structured logging (e.g., JSON format) to facilitate automated log parsing and analysis.
*   **Key Metrics:** Logs must include key metrics for monitoring and debugging, such as files downloaded, time taken for each stage, and number of rows processed.

---

## 6. Development and Maintenance Standards

### 6.1 Stack

*   **Python Version:** Python 3.10+
*   **Key Libraries:** `httpx` (HTTP client), `pandas` (transformation), `psycopg` (PostgreSQL driver), `pysdmx` (SDMX parsing), `pydantic` (data modeling/settings), `tenacity` (retries), `typer` (CLI).
*   **Dependency Management:** Project dependencies and virtual environments must be managed using a modern tool that uses `pyproject.toml`, such as PDM or Poetry.

### 6.2 Project Structure

*   The project must follow the standard `src` layout for source code.
*   Project metadata, dependencies, and scripts must be defined in `pyproject.toml` as per PEP 621.

### 6.3 Quality Assurance

*   **Testing:** The package must have a comprehensive test suite.
    *   **Unit Tests:** To test individual components in isolation.
    *   **Integration Tests:** To validate the end-to-end pipeline against live, containerized services (e.g., a Dockerized PostgreSQL instance).
*   **Continuous Integration (CI):** A CI pipeline (e.g., GitHub Actions) must be configured to automatically run tests, linting, and type checking on every commit.
*   **Linting and Formatting:** Code must be automatically formatted and linted using `Ruff`.
*   **Static Type Checking:** The codebase must have type hints and be validated using `Mypy` or `Pyright` to ensure type safety.

### 6.4 Documentation

*   **User Guide:** A `README.md` file must provide a clear overview, installation instructions, and basic usage examples.
*   **API Reference:** Public modules and functions must have clear docstrings.
*   **Architecture Decision Records (ADRs):** Significant architectural decisions should be documented using ADRs.
*   **Extension Development Guide:** A guide must be provided explaining how to develop and register a new database loader adapter.
---
