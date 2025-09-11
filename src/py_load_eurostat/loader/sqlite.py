"""
SQLite database loader implementation.

This is a fallback loader for simple, file-based database needs or for testing
environments where a full PostgreSQL server is not required. It uses standard
SQL and is not as performant as the PostgreSQL loader for very large datasets.
"""

import logging
import sqlite3
from itertools import islice
from typing import Dict, Generator, Optional, Tuple

import pandas as pd

from ..config import DatabaseSettings
from ..models import DSD, Codelist, IngestionHistory, IngestionStatus, Observation
from .base import LoaderInterface

logger = logging.getLogger(__name__)


class SQLiteLoader(LoaderInterface):
    """
    A loader for SQLite databases.
    It uses the 'name' field from the database settings as the file path.
    """

    def __init__(self, db_settings: DatabaseSettings):
        self.db_path = db_settings.name
        self.conn = self._create_connection()
        self.dsd: Optional[DSD] = None

    def _create_connection(self) -> sqlite3.Connection:
        """Establishes and returns a new database connection."""
        try:
            # We will manage transactions manually.
            conn = sqlite3.connect(self.db_path, isolation_level=None)
            logger.info(f"Successfully connected to SQLite database: '{self.db_path}'")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise

    def _fqn(self, schema: str, table_name: str) -> str:
        """
        Creates a 'fully qualified name' for a table since SQLite doesn't
        support schemas. We use a double underscore to separate them.
        """
        return f"{schema}__{table_name}"

    def _get_required_columns(self, dsd: DSD) -> Dict[str, str]:
        """
        Generates a dictionary of required columns and their SQLite types from a DSD.
        """
        type_map = {
            "String": "TEXT",
            "Text": "TEXT",
            "Double": "REAL",
            "Float": "REAL",
            "Integer": "INTEGER",
            "Long": "INTEGER",
            "Short": "INTEGER",
            "Boolean": "INTEGER",  # 0 or 1
            "Date": "TEXT",
            "Time": "TEXT",
            "DateTime": "TEXT",
            "Year": "INTEGER",
            "Month": "TEXT",
            "Day": "TEXT",
            "TimePeriod": "TEXT",
            "ObservationalTimePeriod": "TEXT",
            "AnyURI": "TEXT",
            "Count": "INTEGER",
            "Decimal": "REAL",
            "BigInteger": "INTEGER",
            "PositiveInteger": "INTEGER",
        }
        columns = {}

        for dim in dsd.dimensions:
            sdmx_type = str(dim.data_type) if dim.data_type else "String"
            sqlite_type = type_map.get(sdmx_type, "TEXT")
            columns[dim.id] = sqlite_type

        primary_measure = next(
            (m for m in dsd.measures if m.id == dsd.primary_measure_id), None
        )
        if primary_measure:
            sdmx_type = (
                str(primary_measure.data_type)
                if primary_measure.data_type
                else "Double"
            )
            sqlite_type = type_map.get(sdmx_type, "REAL")
            columns[primary_measure.id] = sqlite_type
        else:
            columns[dsd.primary_measure_id] = "REAL"

        obs_flag_col_name = next(
            (attr.id for attr in dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )
        columns[obs_flag_col_name] = "TEXT"
        columns["time_period"] = "TEXT"

        return columns

    def _table_exists(self, table_fqn: str, cur: sqlite3.Cursor) -> bool:
        """Checks if a table exists."""
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_fqn,)
        )
        return cur.fetchone() is not None

    def _get_existing_columns(self, table_fqn: str, cur: sqlite3.Cursor) -> set[str]:
        """Retrieves the set of existing column names for a given table."""
        cur.execute(f"PRAGMA table_info({table_fqn});")
        return {row[1] for row in cur.fetchall()}

    def prepare_schema(
        self,
        dsd: DSD,
        table_name: str,
        schema: str,
        representation: str,
        meta_schema: str,
        last_ingestion: Optional[IngestionHistory] = None,
    ) -> None:
        # last_ingestion, representation, and meta_schema are ignored for the
        # simple SQLite loader. They are only here to match the interface.
        self.dsd = dsd
        table_fqn = self._fqn(schema, table_name)
        logger.info(f"Preparing table '{table_fqn}'")

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            required_columns = self._get_required_columns(dsd)

            if not self._table_exists(table_fqn, cur):
                logger.info(f"Table '{table_fqn}' does not exist. Creating...")
                col_defs = [
                    f'"{name}" {dtype}' for name, dtype in required_columns.items()
                ]
                pk_cols = [f'"{dim.id}"' for dim in dsd.dimensions] + ['"time_period"']
                pk_constraint = f"PRIMARY KEY ({', '.join(pk_cols)})"
                col_defs.append(pk_constraint)

                create_sql = f"CREATE TABLE {table_fqn} ({', '.join(col_defs)})"
                cur.execute(create_sql)
                logger.info(f"Table '{table_fqn}' created successfully.")
            else:
                logger.info(
                    f"Table '{table_fqn}' exists. Checking for schema evolution."
                )
                existing_columns = self._get_existing_columns(table_fqn, cur)
                missing_columns = set(required_columns.keys()) - existing_columns

                if missing_columns:
                    for col_name in missing_columns:
                        col_type = required_columns[col_name]
                        logger.info(
                            f"Adding missing column '{col_name}' to '{table_fqn}'."
                        )
                        alter_sql = f'ALTER TABLE {table_fqn} ADD COLUMN "{col_name}" '
                        alter_sql += f"{col_type}"
                        cur.execute(alter_sql)
                    logger.info("Finished adding missing columns.")
                else:
                    logger.info("No missing columns to add. Schema is up-to-date.")

            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
        logger.info(f"Table '{table_fqn}' is ready.")

    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        logger.info(f"Loading {len(codelists)} codelists into schema '{schema}'")
        for cl_id, codelist_obj in codelists.items():
            cl_table_fqn = self._fqn(schema, cl_id.lower())
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN")
                cur.execute(
                    f"""
                CREATE TABLE IF NOT EXISTS {cl_table_fqn} (
                    code TEXT PRIMARY KEY,
                    label_en TEXT,
                    description_en TEXT,
                    parent_code TEXT
                );
                """
                )
                if not codelist_obj.codes:
                    logger.warning(f"Codelist '{cl_id}' has no codes to load.")
                    continue

                rows = [
                    (item.id, item.name, item.description, item.parent_id)
                    for item in codelist_obj.codes.values()
                ]
                cur.executemany(
                    (
                        f"INSERT OR REPLACE INTO {cl_table_fqn} "
                        "(code, label_en, description_en, parent_code) "
                        "VALUES (?, ?, ?, ?)"
                    ),
                    rows,
                )
                logger.info(f"Successfully loaded {len(rows)} codes for '{cl_id}'.")
                cur.execute("COMMIT")
            except sqlite3.Error as e:
                logger.error(f"Error loading codelist '{cl_id}': {e}", exc_info=True)
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()
        logger.info("Codelist loading complete.")

    def bulk_load_staging(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
        use_unlogged_table: bool = True,  # Parameter is unused in SQLite
    ) -> Tuple[str, int]:
        if not self.dsd:
            raise RuntimeError("DSD must be set via prepare_schema before loading.")

        main_table_fqn = self._fqn(schema, table_name)
        staging_table = f"staging_{main_table_fqn}"
        chunk_size = 10000
        total_rows_loaded = 0

        cur = self.conn.cursor()
        try:
            # Setup the staging table. With isolation_level=None, these are
            # autocommitted.
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            res = cur.execute(
                "SELECT sql FROM sqlite_master WHERE name=?", (main_table_fqn,)
            )
            create_sql_tuple = res.fetchone()
            if not create_sql_tuple:
                raise RuntimeError(
                    f"Could not find DDL for main table '{main_table_fqn}'"
                )

            create_sql = create_sql_tuple[0].replace(main_table_fqn, staging_table, 1)
            cur.execute(create_sql)

            # Get column names and order based on the DSD
            dim_order = [
                d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)
            ]
            obs_flag_col_name = next(
                (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()),
                "obs_flags",
            )
            col_names = dim_order + [
                "time_period",
                self.dsd.primary_measure_id,
                obs_flag_col_name,
            ]

            # Process the stream in chunks to avoid loading everything into memory
            while True:
                chunk = list(islice(data_stream, chunk_size))
                if not chunk:
                    break  # End of the stream

                chunk_row_count = len(chunk)
                total_rows_loaded += chunk_row_count
                logger.info(f"Processing a chunk of {chunk_row_count} rows...")

                def data_generator(
                    stream: list[Observation],
                ) -> Generator[tuple, None, None]:
                    """Converts a list of Observation objects into tuples."""
                    for obs in stream:
                        row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                        row_data.extend([obs.time_period, obs.value, obs.flags])
                        yield tuple(row_data)

                df = pd.DataFrame(data_generator(chunk), columns=col_names)

                # Append the chunk to the staging table. pandas handles the transaction.
                df.to_sql(
                    staging_table,
                    self.conn,
                    if_exists="append",
                    index=False,
                )
        finally:
            cur.close()

        logger.info(
            "Finished loading (pandas.to_sql in chunks). "
            f"Loaded {total_rows_loaded} rows."
        )
        return staging_table, total_rows_loaded

    def finalize_load(
        self, staging_table: str, target_table: str, schema: str, strategy: str
    ) -> None:
        if strategy.lower() != "swap":
            raise ValueError(
                f"SQLiteLoader only supports 'swap' strategy, not '{strategy}'"
            )

        target_fqn = self._fqn(schema, target_table)
        logger.info(
            f"Finalizing load from '{staging_table}' to '{target_fqn}' using swap."
        )

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(f"DROP TABLE IF EXISTS {target_fqn}")
            cur.execute(f"ALTER TABLE {staging_table} RENAME TO {target_fqn}")
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
        logger.info("Load finalized successfully.")

    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        history_table_fqn = self._fqn(schema, "_ingestion_history")
        logger.info(f"Querying ingestion state for dataset '{dataset_id}'")

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (history_table_fqn,),
            )
            if not cursor.fetchone():
                return None  # History table doesn't exist yet

            cursor.row_factory = sqlite3.Row  # type: ignore[assignment]
            cursor.execute(
                (
                    f"SELECT * FROM {history_table_fqn} "
                    "WHERE dataset_id = ? AND status = ? "
                    "ORDER BY end_time DESC LIMIT 1"
                ),
                (dataset_id, IngestionStatus.SUCCESS.value),
            )
            row = cursor.fetchone()
            return IngestionHistory.model_validate(row) if row else None
        finally:
            cursor.close()

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        history_table_fqn = self._fqn(schema, "_ingestion_history")
        logger.info(f"Saving ingestion state for dataset '{record.dataset_id}'")

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {history_table_fqn} (
                ingestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL, dsd_version TEXT, load_strategy TEXT,
                representation TEXT, status TEXT, start_time TEXT, end_time TEXT,
                rows_loaded INTEGER, source_last_update TEXT, error_details TEXT
            );
            """)
            record_dict = record.model_dump(mode="json", exclude={"ingestion_id"})
            field_names = ", ".join(record_dict.keys())
            placeholders = ", ".join(["?"] * len(record_dict))
            cur.execute(
                (
                    f"INSERT INTO {history_table_fqn} ({field_names}) "
                    f"VALUES ({placeholders})"
                ),
                list(record_dict.values()),
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()

    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed.")
