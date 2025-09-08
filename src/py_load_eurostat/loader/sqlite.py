"""
SQLite database loader implementation.

This is a fallback loader for testing environments where PostgreSQL/Docker
is not available. It is not recommended for production use due to its
lower performance compared to the native COPY command in PostgreSQL.
"""
import logging
import sqlite3
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
            conn = sqlite3.connect(self.db_name)
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
        with self.conn:
            for cl_id, codelist_obj in codelists.items():
                cl_table_fqn = f"{schema}_{cl_id.lower()}"
                self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {cl_table_fqn} (
                    code TEXT PRIMARY KEY,
                    label_en TEXT,
                    description_en TEXT,
                    parent_code TEXT
                );
                """)
                rows = [
                    (item.id, item.name, item.description, item.parent_id)
                    for item in codelist_obj.codes.values()
                ]
                self.conn.execute(f"DELETE FROM {cl_table_fqn};")
                self.conn.executemany(f"INSERT INTO {cl_table_fqn} VALUES (?, ?, ?, ?)", rows)
        logger.info("Codelist loading complete.")


    def bulk_load_staging(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
        use_unlogged_table: bool = True,
    ) -> Tuple[str, int]:
        if not self.dsd:
            raise RuntimeError("DSD must be set via prepare_schema before loading.")

        staging_table = f"staging_{schema}_{table_name}"

        # Re-create the staging table each time
        with self.conn:
             self.conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
             # Create staging table like the main one
             main_table_fqn = f"{schema}_{table_name}"
             res = self.conn.execute(f"SELECT sql FROM sqlite_master WHERE name='{main_table_fqn}'")
             create_sql = res.fetchone()[0]
             self.conn.execute(create_sql.replace(main_table_fqn, staging_table))

        dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]
        obs_flag_col_name = next(
            (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )

        def data_generator(
            stream: Generator[Observation, None, None],
        ) -> Generator[tuple, None, None]:
            for obs in stream:
                row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                row_data.extend([obs.time_period, obs.value, obs.flags])
                yield tuple(row_data)

        with self.conn:
            col_names = (
                dim_order
                + ["time_period", self.dsd.primary_measure_id, obs_flag_col_name]
            )
            placeholders = ", ".join(["?"] * len(col_names))
            sql = (
                f"INSERT INTO {staging_table} ({', '.join(f'`{c}`' for c in col_names)})"
                f" VALUES ({placeholders})"
            )
            cursor = self.conn.executemany(sql, data_generator(data_stream))
            row_count = cursor.rowcount

        logger.info(f"Finished loading. Loaded {row_count} rows into staging table.")
        return staging_table, row_count


    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        target_fqn = f"{schema}_{target_table}"
        with self.conn:
            self.conn.execute(f"DELETE FROM {target_fqn};")
            self.conn.execute(f"INSERT INTO {target_fqn} SELECT * FROM {staging_table};")
            self.conn.execute(f"DROP TABLE {staging_table};")
        logger.info("Load finalized successfully.")


    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        history_table_fqn = f"{schema}__ingestion_history"
        self.conn.row_factory = sqlite3.Row

        with self.conn:
            cursor = self.conn.execute(
                f"SELECT * FROM {history_table_fqn} "
                "WHERE dataset_id = ? AND status = 'SUCCESS' "
                "ORDER BY end_time DESC LIMIT 1;",
                (dataset_id,),
            )
            row = cursor.fetchone()

        # Reset row_factory to default if necessary, or manage contextually
        self.conn.row_factory = None

        if row:
            # Manually convert row to dict to pass to Pydantic model
            return IngestionHistory(**dict(row))
        return None

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        history_table_fqn = f"{schema}__ingestion_history"

        with self.conn:
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

        # Pydantic model converts datetimes to strings automatically
        record_dict = record.model_dump(exclude={"ingestion_id"})

        # Convert Enum to string
        record_dict["status"] = record_dict["status"].value

        field_names = list(record_dict.keys())
        placeholders = ", ".join(["?"] * len(field_names))
        values = list(record_dict.values())

        with self.conn:
            self.conn.execute(
                f"INSERT INTO {history_table_fqn} ({', '.join(field_names)}) "
                f"VALUES ({placeholders})",
                values,
            )

    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed.")
