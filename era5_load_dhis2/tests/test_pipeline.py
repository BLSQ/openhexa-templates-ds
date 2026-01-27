import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import Period, as_data_values, index_data_dir, read_mapping


def test_index_data_dir() -> None:
    """Test indexing of era5 files in data directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # create empty files to simulate output of era5_sync pipeline
        for var in ("t2m", "d2m", "rh"):
            for agg in ("mean", "min", "max"):
                for period in ("day", "week", "month"):
                    (Path(tmp_dir) / f"{var}_{agg}_{period}.parquet").touch()
        for period in ("day", "week", "month"):
            (Path(tmp_dir) / f"tp_{period}.parquet").touch()

        index = index_data_dir(Path(tmp_dir))

        # sampled variables are aggregated in 3 different ways: mean, min, and max
        # so we expect 3 files per period
        for var in ("t2m", "d2m", "rh"):
            for agg in ("mean", "min", "max"):
                var_name = f"{var}_{agg}"
                assert var_name in index
                assert len(index[var_name]) == 3

        # accumulated variables should only have 1 file per period
        assert "tp" in index
        assert len(index["tp"]) == 3

        f = index["tp"][0]
        assert f["variable"] == "tp"
        assert f["period"]
        assert f["fpath"].exists()


def test_as_data_values() -> None:
    """Test conversion of ERA5 data files to DHIS2 data values."""
    src_dir = Path(__file__).parent / "data" / "src"
    index = index_data_dir(src_dir)
    dv = as_data_values(index["t2m_max"], "t2m_max", Period.MONTH)
    assert len(dv) > 100

    assert (dv["data_element_id"].unique() == "t2m_max").all()
    assert (dv["category_option_combo_id"].unique() == "default").all()
    assert (dv["attribute_option_combo_id"].unique() == "default").all()
    assert (dv["period"].str.len_chars() == 6).all()  # should be YYYYMM
    assert (dv["organisation_unit_id"].str.len_chars() == 11).all()  # should be a DHIS2 UID

    min_value = dv["value"].cast(pl.Float64).min()
    max_value = dv["value"].cast(pl.Float64).max()
    assert isinstance(min_value, float)
    assert isinstance(max_value, float)
    assert 20 < min_value < max_value < 40


def test_read_mapping() -> None:
    """Test reading of mapping file."""
    mapping_file = Path(__file__).parent / "data" / "de_mapping.json"
    mapping = read_mapping(mapping_file)
    assert len(mapping) == 3
    assert mapping["t2m_mean"] == "wqAVMuxnAyk"
    assert mapping["rh_mean"] == "ACei4YDOk4x"
