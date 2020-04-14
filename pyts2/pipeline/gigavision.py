# Copyright (c) 2018-2019 Kevin Murray <foss@kdmurray.id.au>

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import skimage as ski

from .base import *
from .imageio import *
from pyts2.utils import *

import re
import shlex
from sys import stderr, stdout, stdin


class GigavisionMosaicStep(PipelineStep):
    """Class to piece together composite images"""

    def __init__(self, dims, output, subimgres="200x300", order="colsright", output_format="jpg",
                 centrecrop=None, rm_script=None, mv_destination=None):
        self.superdim = XbyY2XY(dims)
        self.subimgres = XbyY2XY(subimgres)
        self.order = order
        self.output = output
        self.output_format = output_format
        self.image_encoder = EncodeImageFileStep(output_format)
        self.centrecrop = centrecrop
        self.current_pixels = None
        self.current_datetime = None
        self.current_image_files = list()
        self.rm_script = rm_script
        self.mv_destination = mv_destination
        self.rm_command = "rm -vf"
        if self.rm_script is not None and self.mv_destination is not None:
            with open(self.rm_script, "w") as fh:
                print("mkdir -p", self.mv_destination, file=fh)
            self.rm_command = f"mv -nvt {self.mv_destination}"

    def write_current(self):
        if self.current_datetime is not None:
            inst = TSInstant(self.current_datetime)
            composite_img = TimestreamImage(
                filename=f"{str(inst)}.{self.output_format}",
                instant=inst,
                pixels=self.current_pixels,
            )
            composite_img = self.image_encoder.process_file(composite_img)
            self.output.write(composite_img)
            if self.rm_script is not None:
                with open(self.rm_script, "a") as fh:
                    for path in self.current_image_files:
                        print(f"{self.rm_command} {shlex.quote(path)}", file=fh)
            self.current_image_files = list()
            return composite_img

    def process_file(self, file):
        if not hasattr(file, "pixels"):
            file = DecodeImageFileStep().process_file(file)

        if isinstance(file.fetcher, FileContentFetcher):
            self.current_image_files.append(str(file.fetcher.pathondisk))

        composite_img = None
        if self.current_datetime != file.instant.datetime:
            composite_img = self.write_current()
            self.current_datetime = file.instant.datetime
            self.current_pixels = np.zeros(
                (self.superdim[0]*self.subimgres[0],
                 self.superdim[1]*self.subimgres[1], 3),
                dtype=np.float32
            )

        try:
            row, col = index2rowcol(int(file.instant.index)-1, self.superdim[0], self.superdim[1], self.order)
        except (TypeError, ValueError) as exc:
            print(str(exc), file.filename, file.instant.index, file=stderr)
            return None
        top = row * self.subimgres[0]
        bottom = top + self.subimgres[0]
        left = col * self.subimgres[1]
        right = left + self.subimgres[1]

        pixels = file.pixels
        if self.centrecrop is not None:
            h, w, _ = pixels.shape
            s = self.centrecrop * 0.5  # half scale factor
            t, b = int(h*s), int(h*(1-s))
            l, r = int(w*s), int(w*(1-s))
            pixels = pixels[t:b, l:r, :]
        from skimage.transform import resize
        smallpx = resize(pixels, self.subimgres, anti_aliasing=True, mode="constant", order=3)
        self.current_pixels[top:bottom, left:right, ...] = smallpx
        return composite_img

    def finish(self):
        self.write_current()
