# Copyright (c) 2018-2020 Kevin Murray <foss@kdmurray.id.au>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .base import PipelineStep
from .imageio import DecodeImageFileStep
from pyts2.timestream import FileContentFetcher

from os.path import realpath

import click


class VerifyImageStep(PipelineStep):

    def __init__(self, resource_ts, removalist, pixel_distance=None, pixel_distance_logfile=None, only_check_exists=False):
        self.removalist = removalist
        self.resource_ts = resource_ts
        self.pixel_distance = pixel_distance
        self.only_check_exists = only_check_exists

        self.pixel_distance_logfile = pixel_distance_logfile
        if pixel_distance_logfile is not None:
            self.pixel_distance_logfile = open(pixel_distance_logfile, "w")
            print("ephemeral_image\tresource_image\tdistance", file=self.pixel_distance_logfile)

    def finish(self):
        if self.pixel_distance_logfile is not None:
            self.pixel_distance_logfile.close()
        self.removalist._do_delete()

    def process_file(self, file):
        decoder = DecodeImageFileStep()
        try:
            res_img = self.resource_ts.getinstant(file.instant)
            if not isinstance(file.fetcher, FileContentFetcher):
                click.echo(f"WARNING: can't delete {file.filename} as it is bundled", err=True)
                return file
            if self.only_check_exists:
                self.removalist.remove(file.fetcher.pathondisk)
            elif self.pixel_distance is not None:
                eimg = decoder.process_file(file)
                rimg = decoder.process_file(res_img)
                if eimg.pixels.shape != rimg.pixels.shape:
                    if self.pixel_distance_logfile is not None:
                        print(basename(file.filename), basename(res_img.filename), "NA", file=self.pixel_distance_logfile)
                    return file
                dist = np.mean(abs(eimg.pixels - rimg.pixels))
                if self.pixel_distance_logfile is not None:
                    print(basename(file.filename), basename(res_img.filename), dist, file=self.pixel_distance_logfile)
                if dist < self.pixel_distance:
                    self.removalist.remove(realpath(file.fetcher.pathondisk))
            elif file.md5sum == res_img.md5sum:
                self.removalist.remove(realpath(file.fetcher.pathondisk))
        except KeyError:
            tqdm.write(f"{file.instant} not in {resource}")
            if self.pixel_distance_logfile is not None:
                print(basename(file.filename), "", "", file=self.pixel_distance_logfile)
        return file
