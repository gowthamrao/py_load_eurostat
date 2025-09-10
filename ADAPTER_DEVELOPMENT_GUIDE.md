# Extension Development Guide: Creating a New Database Adapter

## 1. Introduction

This guide provides a comprehensive walkthrough for developers who wish to extend `py_load_eurostat` by adding support for a new database engine. The system is designed with a modular loader component, making it straightforward to integrate new databases without altering the core data extraction and transformation logic.

The key to this extensibility is the `LoaderInterface`, an Abstract Base Class (ABC) defined in `src/py_load_eurostat/loader/base.py`. Any new loader must be a concrete implementation of this interface.

## 2. The `LoaderInterface` Contract

The `LoaderInterface` defines the set of methods that the pipeline orchestrator relies on to manage the data loading process. Below is a detailed explanation of each method you must implement.

---

### `prepare_schema(...)`

```python
@abstractmethod
def prepare_schema(
    self,
    dsd: DSD,
    table_name: str,
    schema: str,
    last_ingestion: Optional[IngestionHistory] = None,
) -> None:
```

**Purpose:** To ensure that the target database is ready to receive data. This method must be **idempotent**, meaning it can be run multiple times without causing errors.

**Responsibilities:**
- Create the specified `schema` if it does not already exist (e.g., `CREATE SCHEMA IF NOT EXISTS ...`).
- Create the target data `table_name` if it does not exist. The table's structure should be derived from the provided `dsd` (Data Structure Definition) object. You will need to map SDMX data types from the DSD to your database's native column types.
- **Schema Evolution (Optional but Recommended):** If the target table already exists, compare its structure with the DSD. If the DSD has new columns (dimensions or attributes), the adapter should add them to the table (e.g., `ALTER TABLE ... ADD COLUMN ...`). It should also handle potential data type mismatches.

**Models Used:**
- `DSD`: Contains the dimensions, attributes, and measures that define the columns of the data table.
- `IngestionHistory`: Can be used to check the DSD version of the last load to avoid unnecessary schema checks.

---

### `manage_codelists(...)`

```python
@abstractmethod
def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
```

**Purpose:** To load or update the metadata tables that store SDMX Code Lists.

**Responsibilities:**
- For each `Codelist` in the `codelists` dictionary, create a corresponding table in the database (e.g., `cl_geo`, `cl_freq`).
- The table should typically have columns like `code`, `label_en`, `description_en`, and `parent_code`.
- This operation should be an "upsert" (update if exists, insert if not). The `PostgresLoader` uses a staging table and a `MERGE` (`INSERT ... ON CONFLICT`) command for high efficiency.

**Models Used:**
- `Codelist`: Contains the codes and their human-readable labels and descriptions.

---

### `bulk_load_staging(...)`

```python
@abstractmethod
def bulk_load_staging(
    self,
    table_name: str,
    schema: str,
    data_stream: Generator[Observation, None, None],
    use_unlogged_table: bool = True,
) -> Tuple[str, int]:
```

**Purpose:** This is the core performance method. It must load a stream of data into a **new, temporary staging table**.

**CRITICAL REQUIREMENT:** You **MUST** use the database's native bulk loading utility for this operation. Standard `INSERT` statements are too slow and will not be accepted.

**Examples of Native Bulk Loaders:**
- **PostgreSQL:** `COPY FROM STDIN`
- **Snowflake/Redshift:** `COPY INTO` from an S3/Azure Blob/GCS location. This may require your adapter to first stream the data to a file in cloud storage.
- **BigQuery:** Use the Load Jobs API from a GCS file.
- **SQLite:** Use `executemany` with a large batch size, as it does not have a `COPY`-like command.

**Responsibilities:**
- Create a new, temporary staging table (e.g., `staging_my_data_...`). The structure should match the main data table.
- If the database supports it (like PostgreSQL), create this table as `UNLOGGED` to maximize I/O performance.
- Consume the `data_stream` generator and load all `Observation` objects into the staging table using the native bulk method.
- Return the name of the staging table and the total number of rows loaded.

**Models Used:**
- `Observation`: The data for each row to be loaded.

---

### `finalize_load(...)`

```python
@abstractmethod
def finalize_load(
    self, staging_table: str, target_table: str, schema: str, strategy: str
) -> None:
```

**Purpose:** To atomically move the data from the staging table to the final target table. This operation **MUST** be transactional to prevent data corruption.

**Responsibilities:**
- Implement two strategies:
    - **`swap`:** For full loads. This involves renaming the old table (if it exists), renaming the staging table to the target table's name, and then dropping the old table. This is an extremely fast, near-instantaneous atomic operation.
    - **`merge`:** For delta updates. This involves using a `MERGE` or `INSERT ... ON CONFLICT` statement to upsert records from the staging table into the target table based on the primary key.
- All operations must be wrapped in a single database transaction.

---

### `get_ingestion_state(...)` and `save_ingestion_state(...)`

```python
@abstractmethod
def get_ingestion_state(...) -> Optional[IngestionHistory]:
    pass

@abstractmethod
def save_ingestion_state(...) -> None:
    pass
```

**Purpose:** To manage the `_ingestion_history` table, which is crucial for delta-loading logic.

**Responsibilities:**
- `get_ingestion_state`: Query the history table for the most recent successful load record for a given `dataset_id`.
- `save_ingestion_state`: Create the `_ingestion_history` table if it doesn't exist, and insert a new record for the current pipeline run.

**Models Used:**
- `IngestionHistory`: The data model for a single record in the history table.

---

### `close_connection(...)`

```python
@abstractmethod
def close_connection(self) -> None:
```

**Purpose:** To cleanly close any open database connections.

---

## 3. Registering Your New Adapter

Once you have created your new adapter class (e.g., `MyCoolDBLoader`), you must register it in the factory so the pipeline can use it.

1.  Open `src/py_load_eurostat/loader/factory.py`.
2.  Import your new loader class.
3.  Add it to the `LOADER_MAP` dictionary with a unique key (e.g., `"mycooldb"`).

```python
# src/py_load_eurostat/loader/factory.py

from .base import LoaderInterface
from .postgresql import PostgresLoader
from .sqlite import SQLiteLoader
from .my_cool_db import MyCoolDBLoader # <-- Add your import

LOADER_MAP = {
    "postgres": PostgresLoader,
    "sqlite": SQLiteLoader,
    "mycooldb": MyCoolDBLoader, # <-- Add your loader here
}

def get_loader(settings: AppSettings) -> LoaderInterface:
    # ... (factory logic)
```

That's it! By implementing the `LoaderInterface` and registering your adapter, you can seamlessly extend `py_load_eurostat` to support any relational database.
