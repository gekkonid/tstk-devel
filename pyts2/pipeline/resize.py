# Copyright (c) 2018-2019 Kevin Murray <foss@kdmurray.id.au>

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .base import *
from .imageio import *

import numpy as np
import cv2
from skimage.transform import rescale
import re


def geom2rowcol(geom):
    m = re.match(r"^(\d*)[Xx](\d*)$", geom)
    if m is None:
        raise ValueError(f"Invalid image geometry: {geom}")
    rows = int(m[1]) if m[1] else None
    cols = int(m[2]) if m[2] else None
    if rows is None and cols is None:
        raise ValueError(f"Invalid image geometry: {geom}")
    return rows, cols

def ceil2(x):
    """Round up to next even number"""
    return int(np.ceil(x/2)*2)

class GenericDownsizerStep(PipelineStep):
    """Handles boilerplate of chosing sizes etc. for ResizeImageStep/CropCentreStep"""

    def __init__(self, rows=None, cols=None, scale=None, geom=None):
        """Initialise step with size. Use None for one dimension to keep aspect ratio"""
        if geom is not None:
            rows, cols = geom2rowcol(geom)
        if rows is None and cols is None and scale is None:
            raise ValueError("One of rows, cols, or scale must be given")
        if scale is not None and (rows is not None or cols is not None):
            raise ValueError("Either specify rows/cols or scale, not both")
        self.scale = scale
        self.dims = (rows, cols)

    def _new_imagesize(self, imgshape):
        assert len(imgshape) in [2, 3]
        if len(imgshape) == 3:
            orows, ocols, _ = imgshape
        else:
            orows, ocols = imgshape

        if self.scale is not None:
            rows = self.scale * orows
            cols = self.scale * ocols
        else:
            rows, cols = self.dims
            if rows is None:
                rows = orows * (cols / ocols)
            elif cols is None:
                cols = ocols * (rows / orows)
        rows = ceil2(rows)
        assert rows % 2 == 0
        cols = ceil2(cols)
        assert cols % 2 == 0
        if rows > orows:
            rows = orows
        if cols > ocols:
            cols = ocols
        return rows, cols


class ResizeImageStep(GenericDownsizerStep):
    """Pipeline step which resizes an entire image to rows * cols"""

    def process_file(self, file):
        assert hasattr(file, "pixels")  # TODO proper check

        if self.scale is not None:
            newpixels = rescale(file.pixels, self.scale, order=3, anti_aliasing=True)
        else:
            # slower scikit-image method
            # newpixels = ski.transform.resize(file.pixels, np.round((rows, cols)),
            #                                 order=3, anti_aliasing=True)

            # opencv does rows/cols backwards as (width, height)
            rows, cols = self._new_imagesize(file.pixels.shape)
            if len(file.pixels.shape) == 3:
                newpixels = cv2.resize(file.bgr_8, dsize=(cols, rows),
                                       interpolation=cv2.INTER_LANCZOS4)[:, :, ::-1]  # back to rgb
            elif len(file.pixels.shape) == 2:
                # handle greyscale images better
                newpixels = ski.transform.resize(file.pixels, np.round((rows, cols)),
                                                 order=3, anti_aliasing=True)

        return TimestreamImage.from_timestreamfile(file, pixels=newpixels)


class CropCentreStep(GenericDownsizerStep):
    """Pipeline step which resizes an image to rows * cols"""

    def process_file(self, file):
        assert hasattr(file, "pixels")  # TODO proper check

        orow, ocol, _ = file.pixels.shape
        rows, cols = self._new_imagesize(file.pixels.shape)
        left = int((orow - rows) / 2)
        top = int((ocol - cols) / 2)

        newpixels = file.pixels[left:left+rows, top:top+cols, :]

        return TimestreamImage.from_timestreamfile(file, pixels=newpixels)
