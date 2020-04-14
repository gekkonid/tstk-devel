from .base import *
from .imageio import *

import shlex
from sys import stderr


class WriteRmScriptStep(PipelineStep):
    def __init__(self, scriptfile, mv_destination=None):
        self.scriptfile = scriptfile
        self.mv_destination = mv_destination
        self.command = "rm -vf"
        if self.mv_destination is not None:
            with open(self.scriptfile, "w") as fh:
                print("mkdir -p", self.mv_destination, file=fh)
            self.command = f"mv -nvt {self.mv_destination}"

    def process_file(self, file):
        if not isinstance(file.fetcher, FileContentFetcher):
            warnings.warn(f"can't delete {file.filename} as it is bundled")
            return file
        with open(self.scriptfile, "a") as fh:
            print(f"{self.command} {shlex.quote(str(file.fetcher.pathondisk))}", file=fh)
        return file
