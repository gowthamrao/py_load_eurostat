import pytest

from py_load_eurostat.models import (
    DSD,
    Attribute,
    Dimension,
)


@pytest.fixture
def sample_dsd():
    """A sample DSD fixture for testing."""
    from py_load_eurostat.models import Measure

    return DSD(
        id="SAMPLE_DSD",
        name="Sample DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, codelist_id="CL_GEO"),
            Dimension(id="freq", name="Frequency", position=1, codelist_id="CL_FREQ"),
        ],
        attributes=[Attribute(id="OBS_FLAG", name="Observation Flag")],
        measures=[Measure(id="OBS_VALUE", name="Observation Value")],
        primary_measure_id="OBS_VALUE",
    )
