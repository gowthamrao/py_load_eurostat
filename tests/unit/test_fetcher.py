from urllib.parse import urljoin

import httpx
import pytest

from py_load_eurostat.config import AppSettings
from py_load_eurostat.fetcher import Fetcher


@pytest.fixture
def app_settings(tmp_path):
    """Fixture for AppSettings with a temporary cache path."""
    cache_path = tmp_path / "cache"
    return AppSettings(cache={"path": cache_path, "enabled": True})


def test_download_to_cache_http_error(app_settings, mocker):
    """Test that an HTTP error during download is handled correctly."""
    fetcher = Fetcher(app_settings)
    mock_logger_error = mocker.patch("py_load_eurostat.fetcher.logger.error")
    mocker.patch(
        "httpx.Client.stream",
        side_effect=httpx.HTTPStatusError(
            "404 Not Found", request=mocker.MagicMock(), response=mocker.MagicMock()
        ),
    )

    with pytest.raises(httpx.HTTPStatusError):
        fetcher._download_to_cache("http://example.com/file", "test_file")

    assert mock_logger_error.call_count > 0
    assert "HTTP error while downloading" in mock_logger_error.call_args[0][0]


def test_download_to_cache_generic_error_deletes_partial_file(app_settings, mocker):
    """Test that a generic error deletes a partially downloaded file."""
    fetcher = Fetcher(app_settings)
    mock_logger_error = mocker.patch("py_load_eurostat.fetcher.logger.error")

    # Create a dummy file to simulate a partial download
    cache_filepath = fetcher._get_cache_filepath("partial_file")
    cache_filepath.touch()

    # Mock unlink on the specific path object
    mock_unlink = mocker.patch("pathlib.Path.unlink")

    mocker.patch("httpx.Client.stream", side_effect=Exception("Unexpected error"))

    with pytest.raises(Exception, match="Unexpected error"):
        fetcher._download_to_cache("http://example.com/partial", "partial_file")

    assert mock_logger_error.call_count > 0
    assert "An unexpected error occurred" in mock_logger_error.call_args[0][0]
    mock_unlink.assert_called()


def test_fetch_uses_cache(app_settings, mocker):
    """Test that an existing file in the cache is used."""
    fetcher = Fetcher(app_settings)
    mock_logger_info = mocker.patch("py_load_eurostat.fetcher.logger.info")
    mock_download = mocker.patch("py_load_eurostat.fetcher.Fetcher._download_to_cache")

    # Create a dummy cached file
    cache_filepath = fetcher._get_cache_filepath("cached_file")
    cache_filepath.touch()

    fetcher._fetch("http://example.com/cached", "cached_file")

    mock_download.assert_not_called()
    assert any(
        "Found in cache" in call[0][0] for call in mock_logger_info.call_args_list
    )


def test_fetch_downloads_if_not_in_cache(app_settings, mocker):
    """Test that a file is downloaded if it's not in the cache."""
    fetcher = Fetcher(app_settings)
    mock_download = mocker.patch("py_load_eurostat.fetcher.Fetcher._download_to_cache")

    fetcher._fetch("http://example.com/not_cached", "not_cached_file")

    mock_download.assert_called_once_with(
        "http://example.com/not_cached", "not_cached_file"
    )


def test_get_toc_url(app_settings, mocker):
    """Test that the get_toc method constructs the correct URL."""
    fetcher = Fetcher(app_settings)
    mock_fetch = mocker.patch("py_load_eurostat.fetcher.Fetcher._fetch")
    fetcher.get_toc()
    expected_url = f"{app_settings.eurostat.base_url}/files/inventory?type=data"
    mock_fetch.assert_called_once_with(expected_url, "inventory.tsv")


def test_get_dataset_tsv_url(app_settings, mocker):
    """Test that get_dataset_tsv constructs the correct absolute URL."""
    fetcher = Fetcher(app_settings)
    mock_fetch = mocker.patch("py_load_eurostat.fetcher.Fetcher._fetch")

    # Test with a relative URL
    relative_url = "some/relative/path.tsv.gz"
    fetcher.get_dataset_tsv("my_dataset", relative_url)
    expected_url = urljoin(str(app_settings.eurostat.base_url), relative_url)
    mock_fetch.assert_called_once_with(
        expected_url,
        "my_dataset.tsv.gz",
    )

    # Test with an absolute URL
    mock_fetch.reset_mock()
    absolute_url = "http://another.site/file.tsv.gz"
    fetcher.get_dataset_tsv("my_dataset_abs", absolute_url)
    mock_fetch.assert_called_once_with(
        absolute_url,
        "my_dataset_abs.tsv.gz",
    )


def test_get_dsd_xml_url(app_settings, mocker):
    """Test that get_dsd_xml constructs the correct URL."""
    fetcher = Fetcher(app_settings)
    mock_fetch = mocker.patch("py_load_eurostat.fetcher.Fetcher._fetch")
    fetcher.get_dsd_xml("nama_10_gdp")
    sdmx_base = urljoin(str(app_settings.eurostat.base_url), "sdmx/")
    expected_url = (
        f"{sdmx_base}{app_settings.eurostat.sdmx_api_version}/"
        f"dataflow/{app_settings.eurostat.sdmx_agency_id}/NAMA_10_GDP"
        "/latest?references=datastructure"
    )
    mock_fetch.assert_called_once_with(expected_url, "dsd_nama_10_gdp.xml")


def test_get_codelist_xml_url(app_settings, mocker):
    """Test that get_codelist_xml constructs the correct URL."""
    fetcher = Fetcher(app_settings)
    mock_fetch = mocker.patch("py_load_eurostat.fetcher.Fetcher._fetch")
    fetcher.get_codelist_xml("CL_GEO")
    sdmx_base = urljoin(str(app_settings.eurostat.base_url), "sdmx/")
    expected_url = (
        f"{sdmx_base}{app_settings.eurostat.sdmx_api_version}/"
        f"codelist/{app_settings.eurostat.sdmx_agency_id}/CL_GEO"
        "/latest"
    )
    mock_fetch.assert_called_once_with(expected_url, "codelist_cl_geo.xml")
