# Copyright (c) 2018-2020 Kevin Murray <foss@kdmurray.id.au>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
