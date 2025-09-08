"""
SQLite database loader implementation.

This is a fallback loader for testing environments where PostgreSQL/Docker
is not available. It is not recommended for production use due to its
lower performance compared to the native COPY command in PostgreSQL.
"""
import csv
import logging
import sqlite3
import subprocess
import tempfile
from typing import Dict, Generator, Optional, Tuple

from ..models import DSD, Codelist, IngestionHistory, Observation
from .base import LoaderInterface

logger = logging.getLogger(__name__)

class SqliteLoader(LoaderInterface):
    """
    A loader for SQLite databases. Connects to an in-memory DB by default.
    """

    def __init__(self, db_name: str = ":memory:"):
        self.db_name = db_name
        self.conn = self._create_connection()
        self.dsd: Optional[DSD] = None

    def _create_connection(self) -> sqlite3.Connection:
        try:
            # Set isolation_level to None for autocommit mode, allowing manual
            # transaction control with BEGIN/COMMIT/ROLLBACK.
            conn = sqlite3.connect(self.db_name, isolation_level=None)
            logger.info(f"Successfully connected to SQLite database: '{self.db_name}'")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise

    def prepare_schema(self, dsd: DSD, table_name: str, schema: str) -> None:
        self.dsd = dsd
        # SQLite doesn't have schemas, so we prepend the schema to the table name
        data_table_fqn = f"{schema}_{table_name}"

        with self.conn:
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
                f"CREATE TABLE IF NOT EXISTS {data_table_fqn} ({', '.join(cols)})"
            )
        logger.info(f"Table '{data_table_fqn}' is ready.")


    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        logger.info(f"Loading {len(codelists)} codelists into schema '{schema}'")
        for cl_id, codelist_obj in codelists.items():
            cl_table_fqn = f"{schema}_{cl_id.lower()}"
            staging_cl_table = f"staging_{cl_table_fqn}"

            # Create the main table if it doesn't exist
            self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {cl_table_fqn} (
                code TEXT PRIMARY KEY,
                label_en TEXT,
                description_en TEXT,
                parent_code TEXT
            );
            """)

            # Create a temporary staging table and load data into it
            self.conn.execute(f"DROP TABLE IF EXISTS {staging_cl_table}")
            self.conn.execute(f"CREATE TABLE {staging_cl_table} (code TEXT, label_en TEXT, description_en TEXT, parent_code TEXT)")

            rows = [
                (item.id, item.name, item.description, item.parent_id)
                for item in codelist_obj.codes.values()
            ]
            self.conn.executemany(f"INSERT INTO {staging_cl_table} VALUES (?, ?, ?, ?)", rows)

            # Atomically replace the contents of the main table
            self.conn.execute("BEGIN;")
            try:
                self.conn.execute(f"DELETE FROM {cl_table_fqn};")
                self.conn.execute(f"INSERT INTO {cl_table_fqn} SELECT * FROM {staging_cl_table};")
                self.conn.execute("COMMIT;")
            except Exception:
                self.conn.execute("ROLLBACK;")
                logger.error(f"Failed to update codelist {cl_id}, rolled back transaction.")
                raise
            finally:
                self.conn.execute(f"DROP TABLE {staging_cl_table}")

        logger.info("Codelist loading complete.")


    def bulk_load_staging(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
        use_unlogged_table: bool = True, # Parameter is unused in SQLite
    ) -> Tuple[str, int]:
        if not self.dsd:
            raise RuntimeError("DSD must be set via prepare_schema before loading.")
        if self.db_name == ":memory:":
            logger.warning(
                "SQLite in-memory database does not support bulk loading from "
                "an external process. Falling back to executemany()."
            )
            return self._bulk_load_staging_executemany(table_name, schema, data_stream)

        staging_table = f"staging_{schema}_{table_name}"

        # Re-create the staging table each time
        with self.conn:
            self.conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            main_table_fqn = f"{schema}_{table_name}"
            try:
                res = self.conn.execute(f"SELECT sql FROM sqlite_master WHERE name='{main_table_fqn}'")
                create_sql = res.fetchone()[0]
                self.conn.execute(create_sql.replace(main_table_fqn, staging_table))
            except (TypeError, IndexError):
                raise RuntimeError(
                    f"Could not find main table '{main_table_fqn}' to create staging table. "
                    "Did you run prepare_schema first?"
                )

        dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]

        def data_generator_for_csv(
            stream: Generator[Observation, None, None],
        ) -> Generator[list, None, None]:
            for obs in stream:
                row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                row_data.extend([obs.time_period, obs.value, obs.flags])
                yield row_data

        row_count = 0
        with tempfile.NamedTemporaryFile(mode="w+", newline="", suffix=".csv") as tmp_f:
            writer = csv.writer(tmp_f, delimiter=",")
            for row in data_generator_for_csv(data_stream):
                writer.writerow(row)
                row_count += 1

            # Ensure all data is written to disk before calling subprocess
            tmp_f.flush()

            logger.info(f"Wrote {row_count} rows to temporary CSV file for bulk import.")

            # Use the sqlite3 CLI to perform a fast bulk import
            # We need to close the connection to allow the CLI to access the db file
            self.close_connection()
            try:
                subprocess.run(
                    [
                        "sqlite3",
                        self.db_name,
                        f".mode csv",
                        f".import {tmp_f.name} {staging_table}"
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"SQLite CLI import failed. Stderr: {e.stderr}")
                raise
            finally:
                # Re-establish the connection
                self.conn = self._create_connection()

        logger.info(f"Finished bulk loading. Loaded {row_count} rows into staging table.")
        return staging_table, row_count

    def _bulk_load_staging_executemany(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
    ) -> Tuple[str, int]:
        """Fallback to executemany for in-memory databases."""
        if not self.dsd:
            raise RuntimeError("DSD must be set via prepare_schema before loading.")

        staging_table = f"staging_{schema}_{table_name}"
        with self.conn:
            self.conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            main_table_fqn = f"{schema}_{table_name}"
            res = self.conn.execute(f"SELECT sql FROM sqlite_master WHERE name='{main_table_fqn}'")
            create_sql = res.fetchone()[0]
            self.conn.execute(create_sql.replace(main_table_fqn, staging_table))

        dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]
        obs_flag_col_name = next(
            (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()), "obs_flags"
        )

        def data_generator(stream: Generator[Observation, None, None]) -> Generator[tuple, None, None]:
            for obs in stream:
                row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                row_data.extend([obs.time_period, obs.value, obs.flags])
                yield tuple(row_data)

        col_names = (
            dim_order + ["time_period", self.dsd.primary_measure_id, obs_flag_col_name]
        )
        placeholders = ", ".join(["?"] * len(col_names))
        sql = (
            f"INSERT INTO {staging_table} ({', '.join(f'`{c}`' for c in col_names)})"
            f" VALUES ({placeholders})"
        )

        self.conn.execute("BEGIN;")
        try:
            cursor = self.conn.executemany(sql, data_generator(data_stream))
            row_count = cursor.rowcount
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

        logger.info(f"Finished loading (executemany). Loaded {row_count} rows.")
        return staging_table, row_count


    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        target_fqn = f"{schema}_{target_table}"
        logger.info(f"Finalizing load from '{staging_table}' to '{target_fqn}'.")

        self.conn.execute("BEGIN;")
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {target_fqn};")
            self.conn.execute(
                f"ALTER TABLE {staging_table} RENAME TO {target_fqn};"
            )
            self.conn.execute("COMMIT;")
            logger.info("Load finalized successfully.")
        except Exception:
            logger.error("Failed to finalize load, rolling back transaction.")
            self.conn.execute("ROLLBACK;")
            raise


    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        history_table_fqn = f"{schema}__ingestion_history"
        logger.info(f"Querying ingestion state for dataset '{dataset_id}'")

        # Temporarily set row_factory on the cursor to avoid side effects
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row

        try:
            cursor.execute(
                f"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (history_table_fqn,)
            )
            if not cursor.fetchone():
                logger.warning(f"History table '{history_table_fqn}' does not exist. Returning no state.")
                return None

            cursor.execute(
                f"SELECT * FROM {history_table_fqn} "
                "WHERE dataset_id = ? AND status = 'SUCCESS' "
                "ORDER BY end_time DESC LIMIT 1;",
                (dataset_id,),
            )
            row = cursor.fetchone()

            if row:
                # Pydantic v2 can directly validate dict-like objects like sqlite3.Row
                return IngestionHistory.model_validate(row)
            return None
        finally:
            cursor.close()

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        history_table_fqn = f"{schema}__ingestion_history"
        logger.info(
            f"Saving ingestion state for dataset '{record.dataset_id}' "
            f"with status '{record.status.value}'"
        )

        with self.conn:
            # Idempotently create the history table
            self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {history_table_fqn} (
                ingestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                dsd_version TEXT,
                load_strategy TEXT,
                representation TEXT,
                status TEXT,
                start_time TEXT,
                end_time TEXT,
                rows_loaded INTEGER,
                source_last_update TEXT,
                error_details TEXT
            );
            """)

            # Use Pydantic's JSON-compatible dump to handle Enums, datetimes, etc.
            record_dict = record.model_dump(mode="json", exclude={"ingestion_id"})

            field_names = ", ".join(record_dict.keys())
            placeholders = ", ".join(["?"] * len(record_dict))
            values = list(record_dict.values())

            sql = f"INSERT INTO {history_table_fqn} ({field_names}) VALUES ({placeholders})"
            self.conn.execute(sql, values)

    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed.")
