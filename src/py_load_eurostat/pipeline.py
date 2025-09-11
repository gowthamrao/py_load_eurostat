"""
The main pipeline orchestration module.

This module brings together all the components (fetcher, parser, transformer,
loader) to execute the end-to-end data ingestion process.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from .config import AppSettings
from .fetcher import Fetcher
from .loader.factory import get_loader
from .models import IngestionHistory, IngestionStatus
from .parser import InventoryParser, SdmxParser, TsvParser
from .transformer import Transformer

logger = logging.getLogger(__name__)


def run_batch_update(datasets_file: Path, settings: AppSettings) -> None:
    """
    Runs the pipeline for a batch of datasets defined in a YAML file.

    Args:
        datasets_file: Path to the YAML file with the list of dataset IDs.
        settings: The application settings object.
    """
    if not datasets_file.exists():
        logger.error(f"Managed datasets file not found at: {datasets_file}")
        raise FileNotFoundError(f"No such file: '{datasets_file}'")

    with open(datasets_file, "r") as f:
        managed_datasets = yaml.safe_load(f)

    dataset_ids = managed_datasets.get("datasets", [])
    if not dataset_ids:
        logger.warning("No datasets found in the managed datasets file. Exiting.")
        return

    logger.info(f"Starting batch update for {len(dataset_ids)} managed datasets.")

    # Initialize components that can be shared across the batch
    fetcher = Fetcher(settings)
    loader = get_loader(settings)

    # Fetch the master inventory once
    logger.info("Fetching master data inventory...")
    inventory_path = fetcher.get_toc()
    inventory_parser = InventoryParser(inventory_path)

    summary = {"updated": 0, "skipped": 0, "failed": 0}

    for dataset_id in dataset_ids:
        logger.info(f"--- Processing dataset: {dataset_id} ---")
        try:
            remote_last_update = inventory_parser.get_last_update_timestamp(dataset_id)
            if not remote_last_update:
                logger.warning(
                    f"Dataset '{dataset_id}' not found in remote inventory. Skipping."
                )
                summary["failed"] += 1
                continue

            last_ingestion = loader.get_ingestion_state(dataset_id, "eurostat_meta")

            if (
                last_ingestion
                and last_ingestion.source_last_update
                and remote_last_update
                and last_ingestion.source_last_update >= remote_last_update
            ):
                logger.info(f"Dataset '{dataset_id}' is already up-to-date. Skipping.")
                summary["skipped"] += 1
                continue

            logger.info(f"Update required for '{dataset_id}'. Running pipeline...")
            # For batch updates, we always use the 'Delta' strategy for efficiency
            run_pipeline(
                dataset_id=dataset_id,
                representation="Standard",
                load_strategy="Delta",
                settings=settings,
            )
            summary["updated"] += 1

        except Exception as e:
            logger.error(
                f"An error occurred while processing '{dataset_id}': {e}",
                exc_info=False,
            )
            summary["failed"] += 1

    logger.info("--- Batch Update Summary ---")
    logger.info(f"Updated: {summary['updated']}")
    logger.info(f"Skipped: {summary['skipped']}")
    logger.info(f"Failed:  {summary['failed']}")
    logger.info("Batch update process finished.")


def run_pipeline(
    dataset_id: str, representation: str, load_strategy: str, settings: AppSettings
) -> None:
    """
    Orchestrates the end-to-end ingestion pipeline for a given dataset.

    Args:
        dataset_id: The Eurostat dataset ID.
        representation: The data representation ('Standard' or 'Full').
        load_strategy: The load strategy ('Full' or 'Delta').
        settings: The application settings object.
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
        loader = get_loader(settings)
        logger.info(f"Using '{settings.db_type.value}' database loader.")

        # 2. Fetch inventory and check for updates
        logger.info("Fetching Eurostat data inventory...")
        inventory_path = fetcher.get_toc()
        inventory_parser = InventoryParser(inventory_path)
        remote_last_update = inventory_parser.get_last_update_timestamp(dataset_id)
        download_url = inventory_parser.get_download_url(dataset_id)

        history_record = IngestionHistory(
            dataset_id=dataset_id,
            load_strategy=load_strategy,
            representation=representation,
            status=IngestionStatus.RUNNING,
            start_time=start_time,
        )

        if not remote_last_update or not download_url:
            raise RuntimeError(
                f"Could not find dataset '{dataset_id}' in Eurostat's inventory."
            )

        # Update history record with info we just fetched
        history_record.source_last_update = remote_last_update

        # Delta-load check requires fetching the last ingestion state
        last_ingestion: Optional[IngestionHistory] = None
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
        codelist_paths = {cid: fetcher.get_codelist_xml(cid) for cid in codelist_ids}
        codelists = {
            cid: sdmx_parser.parse_codelist(path)
            for cid, path in codelist_paths.items()
        }

        # 4. Create codelist tables first, as they are needed for FK constraints
        loader.manage_codelists(codelists, meta_schema)

        # 5. Prepare main data table schema
        table_name = f"data_{dataset_id.lower()}"
        loader.prepare_schema(
            dsd,
            table_name,
            data_schema,
            representation,
            meta_schema,
            last_ingestion=last_ingestion,
        )

        # 5. Fetch and Parse main data file
        logger.info(f"Fetching dataset TSV from {download_url}...")
        # The new inventory provides the full URL, so we pass it directly.
        # The dataset_id is also passed to be used for the cache filename.
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
            table_name=table_name,
            schema=data_schema,
            data_stream=data_stream,
            use_unlogged_table=settings.db.use_unlogged_tables,
        )
        history_record.rows_loaded = rows_loaded

        # 7. Finalize load
        finalize_strategy = "merge" if load_strategy.lower() == "delta" else "swap"
        loader.finalize_load(
            staging_table, table_name, data_schema, strategy=finalize_strategy
        )

        # 8. Record successful ingestion
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
