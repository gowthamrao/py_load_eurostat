"""
Fetcher module for downloading data and metadata from Eurostat APIs.

This module provides a Fetcher class that handles:
- Making HTTP requests to the Eurostat SDMX API.
- Caching downloaded files to the filesystem to avoid redundant requests.
- Resiliently retrying failed requests with exponential backoff.
"""
import logging
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from .config import AppSettings

# Configure a logger for this module
logger = logging.getLogger(__name__)

# Base URL for the Eurostat SDMX 2.1 API
EUROSTAT_SDMX_API_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

class Fetcher:
    """
    Handles the acquisition of data and metadata from Eurostat's APIs.
    """

    def __init__(self, settings: AppSettings):
        """
        Initializes the Fetcher with application settings.

        Args:
            settings: An instance of AppSettings containing configuration.
        """
        self.settings = settings
        self.client = httpx.Client(
            headers={"User-Agent": "py-load-eurostat/1.0"},
            follow_redirects=True,
            timeout=60.0,
        )
        self._prepare_cache_dir()

    def _prepare_cache_dir(self) -> None:
        """Ensures the cache directory exists."""
        if self.settings.cache.enabled:
            self.settings.cache.path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache directory prepared at: {self.settings.cache.path}")

    def _get_cache_filepath(self, filename: str) -> Path:
        """
        Constructs the full path for a given cache filename.
        """
        return self.settings.cache.path / filename

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=60),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _download_to_cache(self, url: str, cache_filename: str) -> Path:
        """
        Downloads a file from a URL and saves it to the cache, with retries.
        """
        cache_filepath = self._get_cache_filepath(cache_filename)
        logger.info(f"Downloading from {url} to {cache_filepath}")
        try:
            with self.client.stream("GET", url) as response:
                response.raise_for_status()
                with open(cache_filepath, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
            logger.info(f"Successfully downloaded and cached file: {cache_filename}")
            return cache_filepath
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error while downloading {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while downloading {url}: {e}")
            if cache_filepath.exists():
                cache_filepath.unlink()
            raise

    def _fetch(self, url: str, cache_filename: str) -> Path:
        """
        Generic fetch method with caching logic.
        """
        cache_filepath = self._get_cache_filepath(cache_filename)

        if self.settings.cache.enabled and cache_filepath.exists():
            logger.info(f"Found in cache: {cache_filename}. Skipping download.")
            return cache_filepath

        return self._download_to_cache(url, cache_filename)

    def get_toc_xml(self) -> Path:
        """
        Fetches the Table of Contents (TOC) as an SDMX dataflow listing.
        """
        url = f"{EUROSTAT_SDMX_API_URL}/dataflow/ESTAT/all/latest?detail=full"
        return self._fetch(url, "toc.xml")

    def get_dataset_tsv(self, dataset_id: str) -> Path:
        """
        Fetches a dataset in the compressed TSV format.
        """
        url = f"{EUROSTAT_SDMX_API_URL}/data/{dataset_id}/?format=TSV&compressed=true"
        return self._fetch(url, f"{dataset_id}.tsv.gz")

    def get_dsd_xml(self, dataset_id: str) -> Path:
        """
        Fetches the Data Structure Definition (DSD) for a given dataset.
        """
        url = f"{EUROSTAT_SDMX_API_URL}/dataflow/ESTAT/{dataset_id}/latest?references=datastructure"
        return self._fetch(url, f"dsd_{dataset_id}.xml")

    def get_codelist_xml(self, codelist_id: str) -> Path:
        """
        Fetches a specific Codelist in SDMX-ML format.
        """
        url = f"{EUROSTAT_SDMX_API_URL}/codelist/ESTAT/{codelist_id}/latest"
        return self._fetch(url, f"codelist_{codelist_id}.xml")
