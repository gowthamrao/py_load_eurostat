"""
The main pipeline orchestration module.

This module brings together all the components (fetcher, parser, transformer,
loader) to execute the end-to-end data ingestion process.
"""
import logging
import logging
from datetime import datetime, timezone
from typing import Optional

from .config import settings
from .fetcher import Fetcher
from .loader.postgresql import PostgresLoader
from .models import IngestionHistory, IngestionStatus
from .parser import SdmxParser, TocParser, TsvParser
from .transformer import Transformer

logger = logging.getLogger(__name__)


def run_pipeline(
    dataset_id: str, representation: str, load_strategy: str
) -> None:
    """
    Orchestrates the end-to-end ingestion pipeline for a given dataset.

    Args:
        dataset_id: The Eurostat dataset ID.
        representation: The data representation ('Standard' or 'Full').
        load_strategy: The load strategy ('Full' or 'Delta').
    """
    loader = None
    history_record: Optional[IngestionHistory] = None
    start_time = datetime.now(timezone.utc)
    data_schema = "eurostat_data"
    meta_schema = "eurostat_meta"

    try:
        # 1. Initialize components
        fetcher = Fetcher(settings)
        sdmx_parser = SdmxParser()
        loader = PostgresLoader(settings.db)

        # 2. Fetch TOC and check for updates
        logger.info("Fetching Table of Contents...")
        toc_path = fetcher.get_toc()
        toc_parser = TocParser(toc_path)
        remote_last_update = toc_parser.get_last_update_timestamp(dataset_id)
        download_url = toc_parser.get_download_url(dataset_id)

        history_record = IngestionHistory(
            dataset_id=dataset_id,
            load_strategy=load_strategy,
            representation=representation,
            status=IngestionStatus.RUNNING,
            start_time=start_time,
        )

        if not remote_last_update or not download_url:
            raise RuntimeError(
                f"Could not find dataset '{dataset_id}' in Eurostat's Table of Contents."
            )

        # Update history record with info we just fetched
        history_record.source_last_update = remote_last_update

        if load_strategy.lower() == "delta":
            last_ingestion = loader.get_ingestion_state(dataset_id, meta_schema)
            if (
                last_ingestion
                and last_ingestion.source_last_update
                and remote_last_update
                and last_ingestion.source_last_update >= remote_last_update
            ):
                logger.info(f"Local data for '{dataset_id}' is up-to-date. Skipping.")
                if history_record:
                    history_record.status = IngestionStatus.SUCCESS
                    history_record.end_time = datetime.now(timezone.utc)
                    history_record.rows_loaded = 0
                return

        # 3. Fetch and parse metadata (DSD, Codelists)
        logger.info("Fetching and parsing metadata...")
        dsd_xml_path = fetcher.get_dsd_xml(dataset_id)
        dsd = sdmx_parser.parse_dsd_from_dataflow(dsd_xml_path)
        history_record.dsd_version = dsd.version

        codelist_ids = [dim.codelist_id for dim in dsd.dimensions if dim.codelist_id]
        codelist_paths = {
            cid: fetcher.get_codelist_xml(cid) for cid in codelist_ids
        }
        codelists = {
            cid: sdmx_parser.parse_codelist(path) for cid, path in codelist_paths.items()
        }

        # 4. Prepare database schema
        table_name = f"data_{dataset_id.lower()}"
        loader.prepare_schema(dsd, table_name, data_schema)
        loader.manage_codelists(codelists, meta_schema)

        # 5. Fetch and Parse main data file
        logger.info(f"Fetching dataset TSV from {download_url}...")
        tsv_path = fetcher.get_dataset_tsv(dataset_id, download_url)
        tsv_parser = TsvParser(tsv_path)
        wide_df_iterator, dim_cols, time_cols = tsv_parser.parse()

        # 6. Transform and Load data
        logger.info("Initializing transformation and loading...")
        transformer = Transformer(dsd, codelists)
        data_stream = transformer.transform(
            wide_df_iterator, dim_cols, time_cols, representation
        )

        staging_table, rows_loaded = loader.bulk_load_staging(
            table_name=table_name, schema=data_schema, data_stream=data_stream
        )
        history_record.rows_loaded = rows_loaded

        # 7. Finalize load
        loader.finalize_load(staging_table, table_name, data_schema)

        # 7. Record successful ingestion
        if history_record:
            history_record.status = IngestionStatus.SUCCESS
            history_record.end_time = datetime.now(timezone.utc)
        logger.info(f"Pipeline completed successfully for dataset {dataset_id}.")

    except Exception as e:
        logger.critical(f"Pipeline failed for dataset {dataset_id}: {e}", exc_info=True)
        if history_record:
            history_record.status = IngestionStatus.FAILED
            history_record.end_time = datetime.now(timezone.utc)
            history_record.error_details = str(e)
        raise

    finally:
        # 8. Save final ingestion state and close connections
        if loader and history_record:
            try:
                loader.save_ingestion_state(history_record, meta_schema)
            except Exception as db_e:
                logger.error(f"CRITICAL: Failed to save final ingestion state: {db_e}")
            loader.close_connection()
        logger.info("Pipeline execution finished. Resources closed.")
