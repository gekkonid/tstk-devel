import numpy as np
from datetime import datetime
from .utils import *

SMALL_TIMESTREAMS = dict(
    expect_times=[
        datetime(2001, 2, 1,  9, 14, 15),
        datetime(2001, 2, 1, 10, 14, 15),
        datetime(2001, 2, 1, 11, 14, 15),
        datetime(2001, 2, 1, 12, 14, 15),
        datetime(2001, 2, 1, 13, 14, 15),
        datetime(2001, 2, 2,  9, 14, 15),
        datetime(2001, 2, 2, 10, 14, 15),
        datetime(2001, 2, 2, 11, 14, 15),
        datetime(2001, 2, 2, 12, 14, 15),
        datetime(2001, 2, 2, 13, 14, 15),
    ],
    expect_pixels=np.array([[255, 255]], dtype="u1"),
    timestreams=[
        "timestreams/flat",
        "timestreams/flat.tif.zip",
        "timestreams/flat.tif.tar",
        "timestreams/nested/",
        "timestreams/nested",
        "timestreams/nested.tif.zip",
        "timestreams/nested.tif.tar",
        "timestreams/ziped-day",
    ],
    filenames = [
        "2001_02_01_09_14_15.tif",
        "2001_02_01_10_14_15.tif",
        "2001_02_01_11_14_15.tif",
        "2001_02_01_12_14_15.tif",
        "2001_02_01_13_14_15.tif",
        "2001_02_02_09_14_15.tif",
        "2001_02_02_10_14_15.tif",
        "2001_02_02_11_14_15.tif",
        "2001_02_02_12_14_15.tif",
        "2001_02_02_13_14_15.tif",
    ],
)

GVLIKE_TIMESTREAM = dict(
    expect_datetime=datetime(2001, 2, 1,  9, 14, 15),
    expect_indices=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
    expect_pixels=np.array([[255, 255]], dtype="u1"),
)
