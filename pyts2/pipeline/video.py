# Copyright (c) 2018-2019 Kevin Murray <foss@kdmurray.id.au>

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .base import PipelineStep
from .imageio import DecodeImageFileStep

import subprocess
from sys import stdin, stdout, stderr


def get_default_ffmpeg_cmd(rate=10, threads=1, scaling=None):
    f = "-f image2pipe -r {rate} -i pipe: -y -safe 0 -r {rate} -threads {threads} -c:v libx264 -pix_fmt yuv420p -profile:v baseline -tune stillimage -preset slow -crf 20 -loglevel panic"
    f = f.format(rate=rate, threads=threads)
    if scaling is not None:
        f += " " + scaling
    return f


class VideoEncoder(PipelineStep):
    def __init__(self, outfile, ffmpeg_args=None, ffmpeg_path="ffmpeg", rate=10, threads=1, scaling=None):
        if ffmpeg_args is None:
            ffmpeg_args = get_default_ffmpeg_cmd(rate=rate, threads=threads, scaling=scaling)
        ffmpeg_base_command = ffmpeg_path + ' ' + ffmpeg_args + " " + outfile
        self.ffmpeg = subprocess.Popen(ffmpeg_base_command, shell=True, stdin=subprocess.PIPE)

    def process_file(self, file):
        if not hasattr(file, "pixels"):
            file = DecodeImageFileStep().process_file(file)
            if file is None:
                return None
        self.ffmpeg.stdin.write(file.content)
        return file

    def finish(self):
        self.ffmpeg.stdin.close()
        self.ffmpeg.wait()
