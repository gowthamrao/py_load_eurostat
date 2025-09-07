"""
PostgreSQL database loader implementation.

This module provides a concrete implementation of the LoaderInterface for
PostgreSQL, leveraging the `psycopg` library and the high-performance
`COPY` command for data ingestion.
"""
import logging
from typing import Dict, Generator, Optional, Tuple

import psycopg
from psycopg.rows import class_row

from .base import LoaderInterface
from ..models import DSD, CodeList, Observation, IngestionHistory
from ..config import DatabaseSettings

logger = logging.getLogger(__name__)

class PostgresLoader(LoaderInterface):
    """
    A loader for PostgreSQL databases.
    """

    def __init__(self, db_settings: DatabaseSettings):
        self.settings = db_settings
        if not self.settings.password:
            raise ValueError("Database password is required but was not provided.")
        self.conn = self._create_connection()
        self.dsd: Optional[DSD] = None

    def _create_connection(self) -> psycopg.Connection:
        """Establishes and returns a new database connection."""
        try:
            conn_info = self.settings.model_dump()
            conn_info['dbname'] = conn_info.pop('name')
            conn = psycopg.connect(**conn_info)
            logger.info(f"Successfully connected to PostgreSQL database '{self.settings.name}' on {self.settings.host}.")
            return conn
        except psycopg.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def prepare_schema(self, dsd: DSD, table_name: str, schema: str) -> None:
        self.dsd = dsd
        logger.info(f"Preparing schema '{schema}' and table '{table_name}'")
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS %s;", (schema,))
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}._ingestion_history (
                ingestion_id SERIAL PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                dsd_version TEXT,
                load_strategy TEXT,
                representation TEXT,
                status TEXT,
                start_time TIMESTAMPTZ,
                end_time TIMESTAMPTZ,
                rows_loaded BIGINT,
                source_last_update TIMESTAMPTZ,
                error_details TEXT
            );
            """)

            obs_flag_col_name = next((attr.id for attr in dsd.attributes if 'FLAG' in attr.id.upper()), 'obs_flags')
            cols = [f'"{dim.id}" TEXT' for dim in dsd.dimensions]
            cols.append('"time_period" TEXT')
            cols.append(f'"{dsd.primary_measure_id}" DOUBLE PRECISION')
            cols.append(f'"{obs_flag_col_name}" TEXT')

            pk_cols = [f'"{dim.id}"' for dim in dsd.dimensions] + ['"time_period"']
            pk_constraint = f"PRIMARY KEY ({', '.join(pk_cols)})"
            cols.append(pk_constraint)

            cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({', '.join(cols)});")
        self.conn.commit()
        logger.info(f"Table '{schema}.{table_name}' is ready.")

    def manage_codelists(self, codelists: Dict[str, CodeList], schema: str) -> None:
        logger.info(f"Preparing {len(codelists)} codelist tables in schema '{schema}'")
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS %s;", (schema,))
            for cl_id in codelists:
                table_name = f"cl_{cl_id.lower()}"
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    code TEXT PRIMARY KEY,
                    label_en TEXT,
                    description_en TEXT,
                    parent_code TEXT
                );
                """)
        self.conn.commit()
        logger.info("Codelist tables are ready.")

    def bulk_load_staging(
        self, table_name: str, schema: str, data_stream: Generator[Observation, None, None], use_unlogged_table: bool = True
    ) -> Tuple[str, int]:
        if not self.dsd:
            raise RuntimeError("DSD must be set via prepare_schema before loading.")

        staging_table = f"staging_{table_name}_{self.dsd.id.lower()}"
        unlogged_str = "UNLOGGED" if use_unlogged_table else ""

        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{staging_table};")
            cur.execute(f"CREATE {unlogged_str} TABLE {schema}.{staging_table} (LIKE {schema}.{table_name} INCLUDING ALL);")
        logger.info(f"Created staging table: {schema}.{staging_table}")

        dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]
        obs_flag_col_name = next((attr.id for attr in self.dsd.attributes if 'FLAG' in attr.id.upper()), 'obs_flags')
        copy_columns = dim_order + ['time_period', self.dsd.primary_measure_id, obs_flag_col_name]

        def data_generator_for_copy(stream: Generator[Observation, None, None]) -> Generator[bytes, None, None]:
            for obs in stream:
                row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                row_data.extend([obs.time_period, obs.value, obs.flags])
                row_str = '\t'.join(str(item) if item is not None else '\\N' for item in row_data) + '\n'
                yield row_str.encode('utf-8')

        with self.conn.cursor() as cur:
            logger.info(f"Starting COPY to {schema}.{staging_table}...")
            copy_gen = data_generator_for_copy(data_stream)

            with cur.copy(f"COPY {schema}.{staging_table} ({', '.join(f'\"{c}\"' for c in copy_columns)}) FROM STDIN") as copy:
                for data_chunk in copy_gen:
                    copy.write(data_chunk)
            row_count = cur.rowcount

        self.conn.commit()
        logger.info(f"Finished COPY. Loaded {row_count} rows into staging table.")
        return staging_table, row_count

    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        logger.info(f"Finalizing load from '{staging_table}' to '{target_table}'.")
        with self.conn.cursor() as cur:
            cur.execute(f"""
            BEGIN;
            TRUNCATE TABLE {schema}.{target_table};
            INSERT INTO {schema}.{target_table} SELECT * FROM {schema}.{staging_table};
            COMMIT;
            """)
            cur.execute(f"DROP TABLE {schema}.{staging_table};")
        self.conn.commit()
        logger.info("Load finalized successfully.")

    def get_ingestion_state(self, dataset_id: str, schema: str) -> Optional[IngestionHistory]:
        logger.info(f"Querying ingestion state for dataset '{dataset_id}'")
        with self.conn.cursor(row_factory=class_row(IngestionHistory)) as cur:
            cur.execute(
                f"SELECT * FROM {schema}._ingestion_history WHERE dataset_id = %s AND status = 'SUCCESS' ORDER BY end_time DESC LIMIT 1;",
                (dataset_id,)
            )
            return cur.fetchone()

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        logger.info(f"Saving ingestion state for dataset '{record.dataset_id}' with status '{record.status}'")
        with self.conn.cursor() as cur:
            field_names = list(IngestionHistory.model_fields.keys())
            columns = ', '.join(f'"{name}"' for name in field_names)
            placeholders = ', '.join(['%s'] * len(field_names))
            values = tuple(getattr(record, name) for name in field_names)

            cur.execute(
                f"INSERT INTO {schema}._ingestion_history ({columns}) VALUES ({placeholders})",
                values
            )
        self.conn.commit()

    def close_connection(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")
