"""
PostgreSQL database loader implementation.

This module provides a concrete implementation of the LoaderInterface for
PostgreSQL, leveraging the `psycopg` library and the high-performance
`COPY` command for data ingestion.
"""
import logging
from typing import Dict, Generator, Optional, Tuple

import psycopg
from psycopg import sql
from psycopg.rows import class_row

from ..config import DatabaseSettings
from ..models import DSD, Codelist, IngestionHistory, Observation
from .base import LoaderInterface

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
            conn_info["dbname"] = conn_info.pop("name")
            conn = psycopg.connect(**conn_info)
            logger.info(
                "Successfully connected to PostgreSQL database "
                f"'{self.settings.name}' on {self.settings.host}."
            )
            return conn
        except psycopg.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def prepare_schema(self, dsd: DSD, table_name: str, schema: str) -> None:
        self.dsd = dsd
        logger.info(f"Preparing schema '{schema}' and table '{table_name}'")
        history_table_name = "_ingestion_history"

        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                schema=sql.Identifier(schema)
            ))
            cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
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
            """).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(history_table_name)
            ))

            obs_flag_col_name = next(
                (attr.id for attr in dsd.attributes if "FLAG" in attr.id.upper()),
                "obs_flags",
            )
            cols = [f'"{dim.id}" TEXT' for dim in dsd.dimensions]
            cols.append('"time_period" TEXT')
            cols.append(f'"{dsd.primary_measure_id}" DOUBLE PRECISION')
            cols.append(f'"{obs_flag_col_name}" TEXT')

            pk_cols = [f'"{dim.id}"' for dim in dsd.dimensions] + ['"time_period"']
            pk_constraint = f"PRIMARY KEY ({', '.join(pk_cols)})"
            cols.append(pk_constraint)

            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols})").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table_name),
                    cols=sql.SQL(", ").join(sql.SQL(c) for c in cols),
                )
            )
        self.conn.commit()
        logger.info(f"Table '{schema}.{table_name}' is ready.")

    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        logger.info(f"Loading {len(codelists)} codelists into schema '{schema}'")
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                schema=sql.Identifier(schema)
            ))

            def codelist_data_generator(
                codelist: Codelist,
            ) -> Generator[bytes, None, None]:
                for item in codelist.codes.values():
                    row = (item.id, item.name, item.description, item.parent_id)
                    row_str = (
                        "\t".join(str(v) if v is not None else "\\N" for v in row)
                        + "\n"
                    )
                    yield row_str.encode("utf-8")

            for cl_id, codelist_obj in codelists.items():
                cl_table_name = cl_id.lower()
                staging_cl_table = f"staging_{cl_table_name}"

                # Create the main table if it doesn't exist
                cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    code TEXT PRIMARY KEY,
                    label_en TEXT,
                    description_en TEXT,
                    parent_code TEXT
                );
                """).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(cl_table_name)
                ))

                # Create a temporary staging table and load data into it
                cur.execute(
                    sql.SQL(
                        "CREATE TEMP TABLE {staging_table} (LIKE {schema}.{table})"
                    ).format(
                        staging_table=sql.Identifier(staging_cl_table),
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(cl_table_name),
                    )
                )

                copy_sql = sql.SQL("COPY {staging_table} FROM STDIN").format(
                    staging_table=sql.Identifier(staging_cl_table)
                )
                with cur.copy(copy_sql) as copy:
                    for data_chunk in codelist_data_generator(codelist_obj):
                        copy.write(data_chunk)

                logger.info(
                    f"Loaded {cur.rowcount} rows into staging table for codelist '{cl_id}'"
                )

                # Atomically replace the contents of the main table
                cur.execute(sql.SQL("""
                BEGIN;
                TRUNCATE {schema}.{table};
                INSERT INTO {schema}.{table} SELECT * FROM {staging_table};
                COMMIT;
                """).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(cl_table_name),
                    staging_table=sql.Identifier(staging_cl_table)
                ))
                cur.execute(
                    sql.SQL("DROP TABLE {staging_table}").format(
                        staging_table=sql.Identifier(staging_cl_table)
                    )
                )

        self.conn.commit()
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

        staging_table = f"staging_{table_name}_{self.dsd.id.lower()}"
        unlogged_str = "UNLOGGED" if use_unlogged_table else ""

        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                schema=sql.Identifier(schema), table=sql.Identifier(staging_table)
            ))
            cur.execute(
                sql.SQL(
                    "CREATE {unlogged} TABLE {schema}.{staging_table} "
                    "(LIKE {schema}.{target_table} INCLUDING ALL)"
                ).format(
                    unlogged=sql.SQL(unlogged_str),
                    schema=sql.Identifier(schema),
                    staging_table=sql.Identifier(staging_table),
                    target_table=sql.Identifier(table_name),
                )
            )
        logger.info(f"Created staging table: {schema}.{staging_table}")

        dim_order = [d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)]
        obs_flag_col_name = next(
            (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )
        copy_columns = (
            dim_order + ["time_period", self.dsd.primary_measure_id, obs_flag_col_name]
        )

        def data_generator_for_copy(
            stream: Generator[Observation, None, None],
        ) -> Generator[bytes, None, None]:
            for obs in stream:
                row_data = [obs.dimensions.get(dim_id) for dim_id in dim_order]
                row_data.extend([obs.time_period, obs.value, obs.flags])
                row_str = (
                    "\t".join(
                        str(item) if item is not None else "\\N" for item in row_data
                    )
                    + "\n"
                )
                yield row_str.encode("utf-8")

        with self.conn.cursor() as cur:
            logger.info(f"Starting COPY to {schema}.{staging_table}...")
            copy_gen = data_generator_for_copy(data_stream)

            copy_sql = sql.SQL("COPY {schema}.{table} ({columns}) FROM STDIN").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(staging_table),
                columns=sql.SQL(',').join(map(sql.Identifier, copy_columns))
            )
            with cur.copy(copy_sql) as copy:
                for data_chunk in copy_gen:
                    copy.write(data_chunk)
            row_count = cur.rowcount

        self.conn.commit()
        logger.info(f"Finished COPY. Loaded {row_count} rows into staging table.")
        return staging_table, row_count

    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        logger.info(f"Finalizing load from '{staging_table}' to '{target_table}'.")
        with self.conn.cursor() as cur:
            # Using sql.SQL to compose the query safely
            truncate_sql = sql.SQL("TRUNCATE TABLE {schema}.{target}").format(
                schema=sql.Identifier(schema), target=sql.Identifier(target_table)
            )
            insert_sql = sql.SQL(
                "INSERT INTO {schema}.{target} SELECT * FROM {schema}.{staging}"
            ).format(
                schema=sql.Identifier(schema),
                target=sql.Identifier(target_table),
                staging=sql.Identifier(staging_table),
            )
            drop_sql = sql.SQL("DROP TABLE {schema}.{staging}").format(
                schema=sql.Identifier(schema), staging=sql.Identifier(staging_table)
            )

            cur.execute(sql.SQL("""
            BEGIN;
            {truncate};
            {insert};
            COMMIT;
            """).format(truncate=truncate_sql, insert=insert_sql))
            cur.execute(drop_sql)
        self.conn.commit()
        logger.info("Load finalized successfully.")

    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        logger.info(f"Querying ingestion state for dataset '{dataset_id}'")
        query = sql.SQL(
            "SELECT * FROM {schema}._ingestion_history "
            "WHERE dataset_id = %s AND status = 'SUCCESS' "
            "ORDER BY end_time DESC LIMIT 1;"
        ).format(schema=sql.Identifier(schema))
        with self.conn.cursor(row_factory=class_row(IngestionHistory)) as cur:
            cur.execute(query, (dataset_id,))
            return cur.fetchone()

    def save_ingestion_state(self, record: IngestionHistory, schema: str) -> None:
        logger.info(
            f"Saving ingestion state for dataset '{record.dataset_id}' "
            f"with status '{record.status}'"
        )
        with self.conn.cursor() as cur:
            # Exclude ingestion_id from the insert, as it's a SERIAL column
            record_dict = record.model_dump(exclude={"ingestion_id"})

            field_names = list(record_dict.keys())
            columns = sql.SQL(", ").join(map(sql.Identifier, field_names))
            placeholders = sql.SQL(", ").join(sql.Placeholder() * len(field_names))
            values = list(record_dict.values())

            query = sql.SQL(
                "INSERT INTO {schema}._ingestion_history ({fields}) VALUES ({placeholders})"
            ).format(
                schema=sql.Identifier(schema),
                fields=columns,
                placeholders=placeholders,
            )
            cur.execute(query, values)
        self.conn.commit()

    def close_connection(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")
