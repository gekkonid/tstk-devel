from copy import deepcopy
from sys import stdin, stdout, stderr
from os.path import splitext
import os

from .base import PipelineStep
from ..timestream import FileContentFetcher


class UnsafeNuker(PipelineStep):

    def process_file(self, file):
        if not isinstance(file.fetcher, FileContentFetcher):
            print(f"WARNING: can't delete {file.filename} as it is bundled", file=stderr)
            return file
        os.unlink(file.fetcher.pathondisk)
        file.fetcher = None
        return file

