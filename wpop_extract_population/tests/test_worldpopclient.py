from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from worlpopclient import WorldPopClient

# --------------------------------------------------------------------------------------
# Tests for target_tif_filename
# --------------------------------------------------------------------------------------


def test_target_tif_filename_default():  # noqa: D103
    client = WorldPopClient()
    assert client.target_tif_filename("UGA") == "uga_ppp_2020.tif"


def test_target_tif_filename_unadj():  # noqa: D103
    client = WorldPopClient()
    assert client.target_tif_filename("KEN", "2015", un_adj=True) == "ken_ppp_2015_UNadj.tif"


# --------------------------------------------------------------------------------------
# Tests for _build_url
# --------------------------------------------------------------------------------------

def test_build_url():  # noqa: D103
    client = WorldPopClient(url="https://example.com/pop")
    base_url, filename = client._build_url("RWA", "2019", False)
    assert base_url == "https://example.com/pop/2019/RWA/"
    assert filename == "rwa_ppp_2019.tif"


# --------------------------------------------------------------------------------------
# Tests for download_data_for_country
# --------------------------------------------------------------------------------------

def test_download_invalid_iso3_length():  # noqa: D103
    client = WorldPopClient()
    with pytest.raises(ValueError):  # noqa: PT011
        client.download_data_for_country("UG", Path("/tmp"))


def test_download_calls_atomic_download(tmp_path):  # noqa: ANN001, D103
    client = WorldPopClient()

    # Patch the internal atomic download
    with patch.object(client, "_atomic_download") as mock_dl:
        output = client.download_data_for_country("UGA", tmp_path, year="2020")

        expected_file = tmp_path / "uga_ppp_2020.tif"
        assert output == expected_file
        mock_dl.assert_called_once()


# --------------------------------------------------------------------------------------
# Tests for atomic download (success case)
# --------------------------------------------------------------------------------------

def test_atomic_download_success(tmp_path):  # noqa: ANN001
    """Simulate a successful file download."""
    file_path = tmp_path / "test_file.tif"

    fake_response = MagicMock()
    fake_response.iter_content.return_value = [b"abc", b"123"]
    fake_response.__enter__.return_value = fake_response
    fake_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=fake_response):
        WorldPopClient._atomic_download("http://example.com/file", file_path)

    assert file_path.exists()
    assert file_path.read_bytes() == b"abc123"
