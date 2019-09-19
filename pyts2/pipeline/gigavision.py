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


class CompositeImageMakerStep(PipelineStep):
    """Class to piece together composite images"""
    def __init__(self, dims, output, subimgres="200x300", order="colsright",
                 output_format="jpg", centrecrop=None):
        self.superdim = XbyY2XY(dims)
        self.subimgres = XbyY2XY(subimgres)
        self.order = order
        self.current_pixels = None
        self.current_datetime = None
        self.output = output
        self.output_format = output_format
        self.image_encoder = EncodeImageFileStep(output_format)
        self.centrecrop = centrecrop


    def write_current(self):
        if self.current_datetime is not None:
            inst = TSInstant(self.current_datetime)
            composite_img = TimestreamImage(
                filename = f"{str(inst)}.{self.output_format}",
                instant = inst,
                pixels = self.current_pixels,
            )
            composite_img = self.image_encoder.process_file(composite_img)
            self.output.write(composite_img)
            return composite_img

    def process_file(self, file):
        if not hasattr(file, "pixels"):
            file = DecodeImageFileStep().process_file(file)

        composite_img = None
        if self.current_datetime != file.instant.datetime:
            composite_img = self.write_current()
            self.current_datetime = file.instant.datetime
            self.current_pixels = np.zeros(
                    (self.superdim[0]*self.subimgres[0],
                     self.superdim[1]*self.subimgres[1], 3),
                    dtype=np.float32
            )

        row, col = index2rowcol(int(file.instant.index)-1, self.superdim[0], self.superdim[1], self.order)
        top = row * self.subimgres[0]
        bottom = top + self.subimgres[0]
        left = col * self.subimgres[1]
        right = left + self.subimgres[1]

        pixels = file.pixels
        if self.centrecrop is not None:
            h, w, _ = pixels.shape
            s  = self.centrecrop * 0.5  # half scale factor
            t, b = int(h*s), int(h*(1-s))
            l, r = int(w*s), int(w*(1-s))
            pixels = pixels[t:b, l:r, :]
        smallpx = ski.transform.resize(pixels, self.subimgres, anti_aliasing=True,
                                       mode="constant", order=3)
        self.current_pixels[top:bottom, left:right, ...] = smallpx
        return composite_img

    def finish(self):
        self.write_current()

