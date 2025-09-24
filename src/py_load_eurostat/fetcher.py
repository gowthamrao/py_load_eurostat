"""
Fetcher module for downloading data and metadata from Eurostat APIs.

This module provides a Fetcher class that handles:
- Making HTTP requests to the Eurostat APIs.
- Caching downloaded files to the filesystem to avoid redundant requests.
- Resiliently retrying failed requests with exponential backoff.
"""

import logging
from pathlib import Path
from typing import cast
from urllib.parse import urljoin

import httpx
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from .config import AppSettings

# Configure a logger for this module
logger = logging.getLogger(__name__)


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
        return cast(Path, self.settings.cache.path / filename)

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

    def get_toc(self) -> Path:
        """
        Fetches the master Table of Contents (TOC), now called the inventory,
        for all bulk data. The response is a tab-separated values (TSV) file.
        """
        # This URL is from the new API documentation for getting the data inventory.
        url = f"{self.settings.eurostat.base_url}/files/inventory?type=data"
        return self._fetch(url, "inventory.tsv")

    def get_dataset_tsv(self, dataset_id: str, download_url: str) -> Path:
        """
        Fetches a dataset in the compressed TSV format using a direct
        download URL from the inventory.

        Args:
            dataset_id: The code of the dataset, used for creating a stable
                        cache filename.
            download_url: The full or relative URL to the .tsv.gz file from the
                          inventory.
        """
        # The cache filename is derived from the dataset_id, not the URL's filename,
        # to ensure consistency.
        cache_filename = f"{dataset_id.lower()}.tsv.gz"
        # Ensure the download URL is absolute
        full_url = urljoin(str(self.settings.eurostat.base_url), download_url)
        return self._fetch(full_url, cache_filename)

    def get_dsd_xml(self, dataset_id: str) -> Path:
        """
        Fetches the Data Structure Definition (DSD) for a given dataset.
        """
        # Constructing the URL based on the new SDMX API guidelines
        sdmx_base = urljoin(str(self.settings.eurostat.base_url), "sdmx/")
        url = (
            f"{sdmx_base}{self.settings.eurostat.sdmx_api_version}/"
            f"dataflow/{self.settings.eurostat.sdmx_agency_id}/{dataset_id.upper()}"
            "/latest?references=datastructure"
        )
        return self._fetch(url, f"dsd_{dataset_id.lower()}.xml")

    def get_codelist_xml(self, codelist_id: str) -> Path:
        """
        Fetches a specific Codelist in SDMX-ML format.
        """
        sdmx_base = urljoin(str(self.settings.eurostat.base_url), "sdmx/")
        url = (
            f"{sdmx_base}{self.settings.eurostat.sdmx_api_version}/"
            f"codelist/{self.settings.eurostat.sdmx_agency_id}/{codelist_id.upper()}"
            "/latest"
        )
        return self._fetch(url, f"codelist_{codelist_id.lower()}.xml")
