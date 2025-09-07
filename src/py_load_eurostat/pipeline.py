"""
The main pipeline orchestration module.

This module brings together all the components (fetcher, parser, transformer,
loader) to execute the end-to-end data ingestion process.
"""
import logging
from datetime import datetime

from .config import settings
from .fetcher import Fetcher
from .loader.postgresql import PostgresLoader
from .models import IngestionHistory, IngestionStatus
from .parser import SdmxParser, TOCParser
from .transformer import Transformer

logger = logging.getLogger(__name__)


def run_pipeline(dataset_id: str, representation: str, load_strategy: str):
    """
    Orchestrates the end-to-end ingestion pipeline for a given dataset.

    Args:
        dataset_id: The Eurostat dataset ID.
        representation: The data representation ('Standard' or 'Full').
        load_strategy: The load strategy ('Full' or 'Delta').
    """
    loader = None
    history_record = None  # Initialize to None for robust error handling
    start_time = datetime.utcnow()

    try:
        # 1. Initialize components
        fetcher = Fetcher(settings)
        sdmx_parser = SdmxParser()
        toc_parser = TOCParser()
        loader = PostgresLoader(settings.db)
        data_schema = "eurostat_data"
        meta_schema = "eurostat_meta"

        # 2. Delta Load Check
        logger.info(f"Checking for remote updates for dataset '{dataset_id}'...")
        toc_path = fetcher.get_toc_xml()
        remote_last_update = toc_parser.get_last_update_timestamp(toc_path, dataset_id)

        if not remote_last_update:
            raise RuntimeError(f"Could not find dataset '{dataset_id}' in Eurostat's Table of Contents.")

        history_record = IngestionHistory(
            dataset_id=dataset_id,
            dsd_version="N/A",
            load_strategy=load_strategy,
            representation=representation,
            status=IngestionStatus.RUNNING,
            start_time=start_time,
            source_last_update=remote_last_update,
        )

        if load_strategy.lower() == "delta":
            last_ingestion = loader.get_ingestion_state(dataset_id, data_schema)
            if last_ingestion and last_ingestion.source_last_update >= remote_last_update:
                logger.info(
                    f"Local data for '{dataset_id}' is up-to-date (last source update: "
                    f"{last_ingestion.source_last_update}). Skipping ingestion."
                )
                history_record.status = IngestionStatus.SUCCESS
                history_record.end_time = datetime.utcnow()
                history_record.rows_loaded = 0
                return  # Graceful exit, finally block will still run

        # 3. Fetch and parse metadata
        logger.info("Fetching and parsing metadata...")
        dsd_xml_path = fetcher.get_dsd_xml(dataset_id)
        dsd = sdmx_parser.parse_dsd_from_dataflow(dsd_xml_path)
        history_record.dsd_version = dsd.version

        codelist_ids = [dim.codelist_id for dim in dsd.dimensions]
        codelist_paths = {cid: fetcher.get_codelist_xml(cid) for cid in codelist_ids if cid}
        codelists = {cid: sdmx_parser.parse_codelist(path) for cid, path in codelist_paths.items()}

        # 3. Prepare database schema
        table_name = f"data_{dataset_id.lower()}"
        data_schema = "eurostat_data"
        meta_schema = "eurostat_meta"
        loader.prepare_schema(dsd, table_name, data_schema)
        loader.manage_codelists(codelists, meta_schema)

        # 4. Fetch main data file
        logger.info("Fetching dataset TSV file...")
        tsv_path = fetcher.get_dataset_tsv(dataset_id)

        # 5. Transform and Load data
        logger.info("Initializing transformation and loading...")
        transformer = Transformer(dsd, codelists)
        data_stream = transformer.transform(tsv_path, representation)

        staging_table, rows_loaded = loader.bulk_load_staging(
            table_name=table_name,
            schema=data_schema,
            data_stream=data_stream
        )
        history_record.rows_loaded = rows_loaded

        # 6. Finalize load
        loader.finalize_load(staging_table, table_name, data_schema)

        # 7. Record successful ingestion
        history_record.status = IngestionStatus.SUCCESS
        history_record.end_time = datetime.utcnow()
        logger.info(f"Pipeline completed successfully for dataset {dataset_id}.")

    except Exception as e:
        logger.critical(f"Pipeline failed for dataset {dataset_id}: {e}", exc_info=True)
        history_record.status = IngestionStatus.FAILED
        history_record.end_time = datetime.utcnow()
        history_record.error_details = str(e)
        raise

    finally:
        # 8. Save final ingestion state and close connections
        if loader:
            try:
                loader.save_ingestion_state(history_record, data_schema)
            except Exception as db_e:
                logger.error(f"CRITICAL: Failed to save final ingestion state: {db_e}")
            loader.close_connection()
        logger.info("Pipeline execution finished. Resources closed.")
