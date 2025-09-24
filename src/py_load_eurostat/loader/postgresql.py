# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


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
            # Remove our custom setting before passing to the database driver
            conn_info.pop("use_unlogged_tables", None)
            conn = psycopg.connect(**conn_info)
            logger.info(
                "Successfully connected to PostgreSQL database "
                f"'{self.settings.name}' on {self.settings.host}."
            )
            return conn
        except psycopg.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _get_required_columns(self, dsd: DSD) -> Dict[str, str]:
        """
        Generates a dictionary of required columns and their SQL types from a DSD,
        dynamically mapping data types.
        """
        # SDMX data types to PostgreSQL type mapping
        type_map = {
            "String": "TEXT",
            "Text": "TEXT",
            "Double": "DOUBLE PRECISION",
            "Float": "DOUBLE PRECISION",
            "Integer": "INTEGER",
            "Long": "BIGINT",
            "Short": "SMALLINT",
            "Boolean": "BOOLEAN",
            "Date": "DATE",
            "Time": "TIME",
            "DateTime": "TIMESTAMPTZ",
            "Year": "INTEGER",
            "Month": "TEXT",
            "Day": "TEXT",
            "TimePeriod": "TEXT",
            "ObservationalTimePeriod": "TEXT",
            "AnyURI": "TEXT",
            "Count": "INTEGER",
            "Decimal": "NUMERIC",
            "BigInteger": "BIGINT",
            "PositiveInteger": "BIGINT",
        }
        columns = {}

        # Process Dimensions
        for dim in dsd.dimensions:
            sdmx_type = str(dim.data_type) if dim.data_type else "String"
            pg_type = type_map.get(sdmx_type, "TEXT")
            columns[dim.id] = pg_type
            logger.debug(
                f"Mapped dimension '{dim.id}' (SDMX type: {sdmx_type}) to "
                f"PostgreSQL type: {pg_type}"
            )

        # Process Primary Measure
        # Find the measure component from the DSD using the ID
        primary_measure = next(
            (m for m in dsd.measures if m.id == dsd.primary_measure_id), None
        )
        if primary_measure:
            sdmx_type = (
                str(primary_measure.data_type)
                if primary_measure.data_type
                else "Double"
            )
            pg_type = type_map.get(sdmx_type, "DOUBLE PRECISION")
            columns[primary_measure.id] = pg_type
            logger.debug(
                f"Mapped primary measure '{primary_measure.id}' "
                f"(SDMX type: {sdmx_type}) to PostgreSQL type: {pg_type}"
            )
        else:
            # Fallback for safety, though this case should ideally not be hit
            logger.warning(
                f"Primary measure '{dsd.primary_measure_id}' not found in DSD "
                "measures list. Defaulting type to DOUBLE PRECISION."
            )
            columns[dsd.primary_measure_id] = "DOUBLE PRECISION"

        # Process Attributes (Flags) - keep as TEXT
        obs_flag_col_name = next(
            (attr.id for attr in dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )
        columns[obs_flag_col_name] = "TEXT"

        # Time Period is a special dimension not always in the DSD component list
        columns["time_period"] = "TEXT"

        logger.info(f"Determined required columns and types: {columns}")
        return columns

    def _table_exists(self, table_name: str, schema: str, cur: psycopg.Cursor) -> bool:
        """Checks if a table exists in the given schema."""
        query = sql.SQL(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """
        )
        cur.execute(query, (schema, table_name))
        result = cur.fetchone()
        return bool(result[0]) if result else False

    def _get_existing_column_types(
        self, table_name: str, schema: str, cur: psycopg.Cursor
    ) -> Dict[str, str]:
        """Retrieves a dict of existing columns and their PostgreSQL types."""
        query = sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """
        )
        cur.execute(query, (schema, table_name))
        return {row[0]: row[1] for row in cur.fetchall()}

    def _normalize_pg_type(self, pg_type: str) -> str:
        """Normalizes a PostgreSQL type string for reliable comparison."""
        pg_type = pg_type.lower()
        if pg_type.startswith("character varying") or pg_type.startswith("char"):
            return "text"
        if pg_type == "float8":
            return "double precision"
        if pg_type == "int8":
            return "bigint"
        if pg_type == "int4":
            return "integer"
        if pg_type == "int2":
            return "smallint"
        if pg_type.startswith("timestamp"):
            return "timestamptz"
        return pg_type

    def prepare_schema(
        self,
        dsd: DSD,
        table_name: str,
        schema: str,
        representation: str,
        meta_schema: str,
        last_ingestion: Optional[IngestionHistory] = None,
    ) -> None:
        self.dsd = dsd
        logger.info(f"Preparing schema '{schema}' and table '{table_name}'")

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema)
                )
            )

            required_columns = self._get_required_columns(dsd)

            if not self._table_exists(table_name, schema, cur):
                logger.info(
                    f"Table '{schema}.{table_name}' does not exist. Creating..."
                )
                col_defs = [
                    sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(dtype))
                    for name, dtype in required_columns.items()
                ]
                pk_cols = [dim.id for dim in dsd.dimensions] + ["time_period"]
                pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, pk_cols))
                )
                col_defs.append(pk_constraint)

                create_sql = sql.SQL("CREATE TABLE {schema}.{table} ({cols})").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table_name),
                    cols=sql.SQL(", ").join(col_defs),
                )
                cur.execute(create_sql)
                logger.info(f"Table '{schema}.{table_name}' created successfully.")
            else:
                logger.info(
                    f"Table '{schema}.{table_name}' already exists. "
                    "Checking for schema evolution."
                )

                if (
                    last_ingestion
                    and last_ingestion.dsd_version
                    and last_ingestion.dsd_version == dsd.version
                ):
                    logger.info(
                        f"DSD version '{dsd.version}' matches the last ingested "
                        "version. Skipping schema evolution check."
                    )
                    self.conn.commit()
                    return

                existing_column_types = self._get_existing_column_types(
                    table_name, schema, cur
                )
                existing_columns = set(existing_column_types.keys())

                # Check for data type mismatches
                for col_name, required_type in required_columns.items():
                    if col_name in existing_column_types:
                        existing_type = existing_column_types[col_name]
                        norm_exist = self._normalize_pg_type(existing_type)
                        norm_req = self._normalize_pg_type(required_type)
                        if norm_exist != norm_req:
                            raise NotImplementedError(
                                f"Data type mismatch for column '{col_name}' in "
                                f"table '{schema}.{table_name}'. Existing type "
                                f"'{existing_type}' is not compatible with required "
                                f"type '{required_type}'. A full reload is required."
                            )

                # Check for missing columns
                missing_columns = set(required_columns.keys()) - existing_columns
                if missing_columns:
                    for col_name in missing_columns:
                        col_type = required_columns[col_name]
                        logger.info(
                            f"Adding missing column '{col_name}' with type "
                            f"'{col_type}' to table '{table_name}'."
                        )
                        alter_sql = sql.SQL(
                            "ALTER TABLE {schema}.{table} "
                            "ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                        ).format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(table_name),
                            col_name=sql.Identifier(col_name),
                            col_type=sql.SQL(col_type),
                        )
                        cur.execute(alter_sql)
                    logger.info("Finished adding missing columns.")
                else:
                    logger.info("No missing columns to add. Schema is up-to-date.")

                # Check for extra columns that are no longer in the DSD
                extra_columns = existing_columns - set(required_columns.keys())
                if extra_columns:
                    logger.warning(
                        f"The following columns exist in the database but are no "
                        f"longer in the DSD for '{table_name}': {extra_columns}. "
                        "These columns will not be dropped automatically."
                    )
            # Add foreign key constraints if this is a 'Standard' representation
            if representation.lower() == "standard":
                logger.info(
                    "Applying foreign key constraints for 'Standard' representation."
                )
                # Ensure the metadata schema exists before trying to reference it
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                        schema=sql.Identifier(meta_schema)
                    )
                )
                for dim in dsd.dimensions:
                    if dim.codelist_id:
                        fk_name = f"fk_{table_name}_{dim.id}"
                        codelist_table = dim.codelist_id.lower()

                        # Check if constraint already exists to ensure idempotency
                        cur.execute(
                            """
                            SELECT 1 FROM information_schema.table_constraints
                            WHERE constraint_type = 'FOREIGN KEY'
                            AND table_name = %s AND constraint_name = %s
                            AND table_schema = %s
                        """,
                            (table_name, fk_name, schema),
                        )
                        if cur.fetchone():
                            logger.debug(f"Foreign key '{fk_name}' already exists.")
                            continue

                        logger.info(
                            f"Adding foreign key '{fk_name}' to table '{table_name}' "
                            f"on column '{dim.id}'."
                        )
                        fk_sql = sql.SQL(
                            """
                            ALTER TABLE {data_schema}.{data_table}
                            ADD CONSTRAINT {fk_name}
                            FOREIGN KEY ({dim_column})
                            REFERENCES {meta_schema}.{codelist_table} (code)
                            ON DELETE RESTRICT ON UPDATE CASCADE
                        """
                        ).format(
                            data_schema=sql.Identifier(schema),
                            data_table=sql.Identifier(table_name),
                            fk_name=sql.Identifier(fk_name),
                            dim_column=sql.Identifier(dim.id),
                            meta_schema=sql.Identifier(meta_schema),
                            codelist_table=sql.Identifier(codelist_table),
                        )
                        cur.execute(fk_sql)

        self.conn.commit()
        logger.info(f"Table '{schema}.{table_name}' is ready.")

    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        logger.info(f"Loading {len(codelists)} codelists into schema '{schema}'")

        def codelist_data_generator(
            codelist: Codelist,
        ) -> Generator[bytes, None, None]:
            for item in codelist.codes.values():
                row = (item.id, item.name, item.description, item.parent_id)
                row_str = (
                    "\t".join(str(v) if v is not None else "\\N" for v in row) + "\n"
                )
                yield row_str.encode("utf-8")

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema)
                )
            )

        for cl_id, codelist_obj in codelists.items():
            cl_table_name = cl_id.lower()
            staging_cl_table = f"staging_{cl_table_name}"

            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    # Create the main table if it doesn't exist
                    cur.execute(
                        sql.SQL(
                            """
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        code TEXT PRIMARY KEY,
                        label_en TEXT,
                        description_en TEXT,
                        parent_code TEXT
                    );
                    """
                        ).format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(cl_table_name),
                        )
                    )

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
                        f"Loaded {cur.rowcount} rows into staging table for "
                        f"codelist '{cl_id}'"
                    )

                    # Use MERGE (INSERT ... ON CONFLICT) for efficient updates
                    merge_sql = sql.SQL(
                        """
                    INSERT INTO {schema}.{table}
                    SELECT * FROM {staging_table}
                    ON CONFLICT (code) DO UPDATE SET
                        label_en = EXCLUDED.label_en,
                        description_en = EXCLUDED.description_en,
                        parent_code = EXCLUDED.parent_code;
                    """
                    ).format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(cl_table_name),
                        staging_table=sql.Identifier(staging_cl_table),
                    )
                    cur.execute(merge_sql)
                    cur.execute(
                        sql.SQL("DROP TABLE {staging_table}").format(
                            staging_table=sql.Identifier(staging_cl_table)
                        )
                    )
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
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                    schema=sql.Identifier(schema), table=sql.Identifier(staging_table)
                )
            )
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

        dim_order = [
            d.id for d in sorted(self.dsd.dimensions, key=lambda x: x.position)
        ]
        obs_flag_col_name = next(
            (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )
        copy_columns = dim_order + [
            "time_period",
            self.dsd.primary_measure_id,
            obs_flag_col_name,
        ]

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
                columns=sql.SQL(",").join(map(sql.Identifier, copy_columns)),
            )
            with cur.copy(copy_sql) as copy:
                for data_chunk in copy_gen:
                    copy.write(data_chunk)
            row_count = cur.rowcount

        self.conn.commit()
        logger.info(f"Finished COPY. Loaded {row_count} rows into staging table.")
        return staging_table, row_count

    def finalize_load(
        self, staging_table: str, target_table: str, schema: str, strategy: str
    ) -> None:
        if strategy.lower() == "swap":
            self._finalize_swap(staging_table, target_table, schema)
        elif strategy.lower() == "merge":
            self._finalize_merge(staging_table, target_table, schema)
        else:
            raise ValueError(f"Unknown finalization strategy: '{strategy}'")

    def _finalize_swap(
        self, staging_table: str, target_table: str, schema: str
    ) -> None:
        logger.info(
            f"Finalizing load from '{staging_table}' to '{target_table}' "
            "using atomic table swap."
        )
        backup_table = f"{target_table}_old"

        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {schema}.{backup} CASCADE").format(
                        schema=sql.Identifier(schema),
                        backup=sql.Identifier(backup_table),
                    )
                )
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE IF EXISTS {schema}.{target} RENAME TO {backup}"
                    ).format(
                        schema=sql.Identifier(schema),
                        target=sql.Identifier(target_table),
                        backup=sql.Identifier(backup_table),
                    )
                )
                cur.execute(
                    sql.SQL("ALTER TABLE {schema}.{staging} RENAME TO {target}").format(
                        schema=sql.Identifier(schema),
                        staging=sql.Identifier(staging_table),
                        target=sql.Identifier(target_table),
                    )
                )
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {schema}.{backup} CASCADE").format(
                        schema=sql.Identifier(schema),
                        backup=sql.Identifier(backup_table),
                    )
                )
        logger.info("Load finalized successfully. Tables swapped.")

    def _finalize_merge(
        self, staging_table: str, target_table: str, schema: str
    ) -> None:
        if not self.dsd:
            raise RuntimeError("DSD must be set to perform a merge.")

        logger.info(
            f"Finalizing load from '{staging_table}' to '{target_table}' using MERGE."
        )

        pk_cols = [dim.id for dim in self.dsd.dimensions] + ["time_period"]
        obs_flag_col = next(
            (attr.id for attr in self.dsd.attributes if "FLAG" in attr.id.upper()),
            "obs_flags",
        )
        update_cols = [self.dsd.primary_measure_id, obs_flag_col]

        set_expressions = sql.SQL(", ").join(
            [
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                for col in update_cols
            ]
        )

        with self.conn.transaction():
            with self.conn.cursor() as cur:
                merge_sql = sql.SQL(
                    """
                    INSERT INTO {schema}.{target}
                    SELECT * FROM {schema}.{staging}
                    ON CONFLICT ({pk_cols}) DO UPDATE SET {set_expressions};
                    """
                ).format(
                    schema=sql.Identifier(schema),
                    target=sql.Identifier(target_table),
                    staging=sql.Identifier(staging_table),
                    pk_cols=sql.SQL(", ").join(map(sql.Identifier, pk_cols)),
                    set_expressions=set_expressions,
                )
                cur.execute(merge_sql)
                row_count = cur.rowcount
                logger.info(f"Merge complete. {row_count} rows inserted or updated.")
                cur.execute(
                    sql.SQL("DROP TABLE {schema}.{staging}").format(
                        schema=sql.Identifier(schema),
                        staging=sql.Identifier(staging_table),
                    )
                )
        logger.info("Load finalized successfully using MERGE strategy.")

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
        history_table_name = "_ingestion_history"

        with self.conn.cursor() as cur:
            # Ensure schema and history table exist before trying to insert
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema)
                )
            )
            cur.execute(
                sql.SQL("""
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
                    table=sql.Identifier(history_table_name),
                )
            )

            # Exclude ingestion_id from the insert, as it's a SERIAL column
            record_dict = record.model_dump(exclude={"ingestion_id"})

            field_names = list(record_dict.keys())
            columns = sql.SQL(", ").join(map(sql.Identifier, field_names))
            placeholders = sql.SQL(", ").join(sql.Placeholder() * len(field_names))
            values = list(record_dict.values())

            query = sql.SQL(
                "INSERT INTO {schema}._ingestion_history ({fields}) "
                "VALUES ({placeholders})"
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
