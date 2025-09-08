"""
SQLite database loader implementation.

This is a fallback loader for simple, file-based database needs or for testing
environments where a full PostgreSQL server is not required. It uses standard
SQL and is not as performant as the PostgreSQL loader for very large datasets.
"""
import logging
import sqlite3
from typing import Dict, Generator, Optional, Tuple

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
            # Using autocommit mode; transactions are handled explicitly.
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

    def prepare_schema(self, dsd: DSD, table_name: str, schema: str) -> None:
        self.dsd = dsd
        table_fqn = self._fqn(schema, table_name)
        logger.info(f"Preparing table '{table_fqn}'")

        with self.conn:
            self.conn.execute("BEGIN")
            try:
                obs_flag_col_name = next(
                    (attr.id for attr in dsd.attributes if "FLAG" in attr.id.upper()),
                    "obs_flags",
                )
                cols = [f'"{dim.id}" TEXT' for dim in dsd.dimensions]
                cols.append('"time_period" TEXT')
                cols.append(f'"{dsd.primary_measure_id}" REAL')
                cols.append(f'"{obs_flag_col_name}" TEXT')

                pk_cols = [f'"{dim.id}"' for dim in dsd.dimensions] + ['"time_period"']
                cols.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

                self.conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_fqn} ({', '.join(cols)})"
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        logger.info(f"Table '{table_fqn}' is ready.")

    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        logger.info(f"Loading {len(codelists)} codelists into schema '{schema}'")
        try:
            with self.conn:
                for cl_id, codelist_obj in codelists.items():
                    cl_table_fqn = self._fqn(schema, cl_id.lower())
                    self.conn.execute(
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
                        continue
                    rows = [
                        (item.id, item.name, item.description, item.parent_id)
                        for item in codelist_obj.codes.values()
                    ]
                    # Use INSERT OR REPLACE for simple, idempotent upserts.
                    self.conn.executemany(
                        f"INSERT OR REPLACE INTO {cl_table_fqn} VALUES (?, ?, ?, ?)",
                        rows,
                    )
        except sqlite3.Error as e:
            logger.error(f"Error during codelist loading: {e}")
            raise
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

        with self.conn:
            self.conn.execute("BEGIN")
            try:
                # Re-create the staging table each time
                self.conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
                res = self.conn.execute(
                    f"SELECT sql FROM sqlite_master WHERE name='{main_table_fqn}'"
                )
                create_sql = res.fetchone()
                if not create_sql:
                    raise RuntimeError(f"Could not find DDL for main table '{main_table_fqn}'")

                self.conn.execute(create_sql[0].replace(main_table_fqn, staging_table))

                dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]
                obs_flag_col_name = next(
                    (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()), "obs_flags"
                )

                def data_generator(stream: Generator[Observation, None, None]) -> Generator[tuple, None, None]:
                    for obs in stream:
                        row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                        row_data.extend([obs.time_period, obs.value, obs.flags])
                        yield tuple(row_data)

                col_names = dim_order + ["time_period", self.dsd.primary_measure_id, obs_flag_col_name]
                placeholders = ", ".join(["?"] * len(col_names))
                sql = (
                    f"INSERT INTO {staging_table} ({', '.join(f'`{c}`' for c in col_names)})"
                    f" VALUES ({placeholders})"
                )

                cursor = self.conn.executemany(sql, data_generator(data_stream))
                row_count = cursor.rowcount
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        logger.info(f"Finished loading (executemany). Loaded {row_count} rows.")
        return staging_table, row_count

    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        target_fqn = self._fqn(schema, target_table)
        logger.info(f"Finalizing load from '{staging_table}' to '{target_fqn}'.")

        with self.conn:
            self.conn.execute("BEGIN")
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {target_fqn}")
                self.conn.execute(f"ALTER TABLE {staging_table} RENAME TO {target_fqn}")
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        logger.info("Load finalized successfully.")

    def get_ingestion_state(self, dataset_id: str, schema: str) -> Optional[IngestionHistory]:
        history_table_fqn = self._fqn(schema, "_ingestion_history")
        logger.info(f"Querying ingestion state for dataset '{dataset_id}'")

        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{history_table_fqn}'")
            if not cursor.fetchone():
                return None  # History table doesn't exist yet

            cursor.row_factory = sqlite3.Row
            cursor.execute(
                f"SELECT * FROM {history_table_fqn} WHERE dataset_id = ? AND status = ? ORDER BY end_time DESC LIMIT 1",
                (dataset_id, IngestionStatus.SUCCESS.value),
            )
            row = cursor.fetchone()
            return IngestionHistory.model_validate(row) if row else None
        finally:
            cursor.close()

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        history_table_fqn = self._fqn(schema, "_ingestion_history")
        logger.info(f"Saving ingestion state for dataset '{record.dataset_id}'")

        with self.conn:
            self.conn.execute("BEGIN")
            try:
                self.conn.execute(f"""
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
                self.conn.execute(
                    f"INSERT INTO {history_table_fqn} ({field_names}) VALUES ({placeholders})",
                    list(record_dict.values()),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed.")
