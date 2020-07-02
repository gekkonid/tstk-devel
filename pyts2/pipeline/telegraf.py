from copy import deepcopy
from .base import PipelineStep

from os.path import splitext

from telegraf.client import TelegrafClient


class TelegrafRecordStep(PipelineStep):
    """Write each file to output, without changing the file"""

    def __init__(self, metric_name, telegraf_host='localhost', telegraf_port=8092, tags={}):
        self.client = TelegrafClient(host=telegraf_host, port=telegraf_port)
        self.metric_name = metric_name
        self.tags = {}

    def process_file(self, file):
        fileext = splitext(file.filename)[1].lower().lstrip(".")
        tags = {"InstantIndex": file.instant.index, "FileType": fileext}
        tags.update(self.tags)
        epoch_ns = int(file.instant.datetime.timestamp() * 1e9) # to NS
        self.client.metric(self.metric_name, file.report, timestamp=epoch_ns, tags=tags)
        return file

