# Copyright (c) 2018-2019 Kevin Murray <foss@kdmurray.id.au>

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .base import PipelineStep, FatalPipelineError
from .imageio import DecodeImageFileStep

import subprocess
from sys import stdin, stdout, stderr
from os.path import dirname, basename

import numpy as np
import imageio
from PIL import Image, ImageFont, ImageDraw


def get_default_ffmpeg_cmd(rate=10, threads=1, scaling=None):
    f = "-f image2pipe -r {rate} -i pipe: -y -safe 0 -r {rate} -threads {threads} -vf 'pad=ceil(iw/2)*2:ceil(ih/2)*2' -c:v libx264 -pix_fmt yuv420p -profile:v baseline -tune stillimage -preset slow -crf 20 -loglevel error"
    f = f.format(rate=rate, threads=threads)
    if scaling is not None:
        f += " " + scaling
    return f


def get_instant(file):
    return str(file.instant)

class ImageWatermarker(PipelineStep):

    def __init__(self, font=None, font_size=16, textcallback=get_instant):
        self.textcallback = textcallback
        try:
            if font is None:
                font = dirname(dirname(__file__)) + "/data/Monoid-Bold-HalfLoose-0-1-NoCalt.ttf"
            self.font = ImageFont.truetype(font, font_size)
        except Exception as exc:     # load default font in case of any exception
            print("Error: Couldn't locate font", font, ", using default font.\n",
                    exc.__class__.__name__, str(exc), file=stderr)
            self.font = ImageFont.load_default()

    def process_file(self, file):
        if not hasattr(file, "pixels"):
            file = DecodeImageFileStep().process_file(file)
            if file is None:
                return None
        image = file.pil

        text = self.textcallback(file)
        bottom_margin = 3   # bottom margin for text
        text_height = self.font.getsize(text)[1] + bottom_margin
        left, top = (5, image.size[1] - text_height)
        text_width = self.font.getsize(text)[0]
        locus = np.asarray(image.crop((left, top, left + text_width, top + text_height)))
        meancol = tuple(list(locus.mean(axis=(0,1)).astype(int)))
        opposite = (int(locus.mean()) + 96)
        if opposite > 255:
            opposite = (int(locus.mean()) - 96)
        oppositegrey = (opposite, ) * 3
        draw = ImageDraw.Draw(image)
        draw.rectangle((left-3, top-3, left + text_width + 3, top + text_height + 3),
                       fill=meancol)
        draw.text((left, top), text, fill=oppositegrey, font=self.font)
        file.pixels = np.asarray(image)
        return file



class VideoEncoder(PipelineStep):
    def __init__(self, outfile, ffmpeg_args=None, ffmpeg_path="ffmpeg", rate=10, threads=1, scaling=None):
        if ffmpeg_args is None:
            ffmpeg_args = get_default_ffmpeg_cmd(rate=rate, threads=threads, scaling=scaling)
        ffmpeg_base_command = ffmpeg_path + ' ' + ffmpeg_args + " " + outfile
        self.ffmpeg = subprocess.Popen(ffmpeg_base_command, shell=True,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

    def process_file(self, file):
        if not hasattr(file, "pixels"):
            file = DecodeImageFileStep().process_file(file)
            if file is None:
                return None
        try:
            self.ffmpeg.stdin.write(file.content)
        except BrokenPipeError as exc:
            log=self.ffmpeg.stdout.read()
            raise FatalPipelineError(f"FFMPEG failed, log follows\n{log}")
        return file

    def finish(self):
        self.ffmpeg.stdin.close()
        self.ffmpeg.wait()
