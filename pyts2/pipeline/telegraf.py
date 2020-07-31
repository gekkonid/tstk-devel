from copy import deepcopy
from .base import PipelineStep

import os
from os.path import splitext
from datetime import datetime

from telegraf.client import TelegrafClient
import pytz


class TelegrafRecordStep(PipelineStep):
    """Write each file to output, without changing the file"""

    def __init__(self, metric_name, telegraf_host='localhost', telegraf_port=8092, tags={}, tz=None):
        self.client = TelegrafClient(host=telegraf_host, port=telegraf_port)
        self.metric_name = metric_name
        self.tags = tags
        if tz is None:
            tz = os.environ.get("TSTK_TZ", "Australia/Brisbane")
        self.localtz = pytz.timezone(tz)

    def process_file(self, file):
        fileext = splitext(file.filename)[1].lower().lstrip(".")
        tags = {"InstantIndex": file.instant.index, "FileType": fileext}
        tags.update(self.tags)
        dt = self.localtz.localize(file.instant.datetime)
        utc = dt.astimezone(pytz.utc)
        epoch_ns = int(utc.timestamp() * 1e9) # to NS
        now_ns = int(datetime.utcnow().timestamp() * 1e9)
        report = file.report
        report.update({"CapturedAt": epoch_ns, "ProcessedAt":now_ns})
        self.client.metric(self.metric_name, report, timestamp=epoch_ns, tags=tags)
        return file

