# Copyright 2020 Kevin Murray <foss@kdmurray.id.au>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from PIL import Image
import zbarlight
from tqdm import tqdm
import piexif
import rawpy
import numpy as np

import os
from os.path import dirname, basename, splitext, getsize
import io
from sys import stdout, stderr, stdin, exit
from csv import DictWriter
import datetime
import argparse
import multiprocessing as mp
import re
import traceback
from shlex import quote


def tb(do=True):
    if do:
        traceback.print_exc(file=stderr)


def getncpu():
    return int(os.environ.get("PBS_NCPUS", mp.cpu_count()))


def get_exif_time(imgpath):
    exif = piexif.load(imgpath)
    dstr = exif["Exif"][piexif.ExifIFD.DateTimeOriginal].decode("utf-8")
    return datetime.datetime.strptime(dstr, "%Y:%m:%d %H:%M:%S")


def loadimage(imgpath):
    if imgpath.lower().endswith("cr2"):
        with open(imgpath, "rb") as fh, rawpy.imread(fh) as raw:
            try:
                thumb = raw.extract_thumb()
                image = Image.open(io.BytesIO(thumb.data))
                return np.array(image)
            except Exception as exc:
                print(str(exc), file=stderr)
                pixels = raw.postprocess(use_camera_wb=True, auto_bright_thr=0.001, output_bps=8)
                return pixels
    else:
        return np.array(Image.open(imgpath))


def re_extract(string, pattern):
    m = re.search(pattern, string)
    if m is not None:
        return m.group(1)


def re_chamber(string):
    return re_extract(string, r'(GC\d+[LR])')


def re_experiment(string):
    return re_extract(string, r'(BVZ\d+|ATK\d+|TR\d+)')


def re_time(string):
    dstr = re_extract(string, r'(\d\d\d\d_\d\d_\d\d_\d\d_\d\d_\d\d)')
    if dstr is None:
        return None
    return datetime.datetime.strptime(dstr, "%Y_%m_%d_%H_%M_%S")


def find_files(*args):
    for base in args:
        if os.path.exists(base) and os.path.isfile(base):
            yield base
        for root, dirs, files in os.walk(base):
            for file in files:
                yield os.path.join(root, file)


def is_image(path, exts=[".jpg", ".cr2"]):
    base, ext = splitext(path)
    return ext.lower() in exts


def scanimage(imgpath):
    # init all variables
    error = None
    pixel_mean = None
    codes = None
    qr_chamber = None
    qr_experiment = None
    file_size = None

    # Get size
    try:
        file_size = getsize(imgpath)
    except Exception as exc:
        if stderr.isatty():
            print(str(exc), imgpath, file=stderr)
            tb()
        error = "FileSize"

    # Load image
    try:
        pixels = loadimage(imgpath)
        if pixels.dtype == object:
            raise ValueError("Bad image load: see https://github.com/python-pillow/Pillow/issues/3863")
        pixel_mean = np.mean(pixels)
        codes = zbarlight.scan_codes('qrcode', Image.fromarray(pixels))
        if codes is not None:
            codes = ';'.join(sorted(x.decode('utf8') for x in codes))
            qr_chamber = re_chamber(codes)
            qr_experiment = re_experiment(codes)
    except Exception as exc:
        if stderr.isatty():
            print(str(exc), imgpath, file=stderr)
            tb()
        error = "ImageIO"

    # Metadata
    exif_time = None
    try:
        exif_time = get_exif_time(imgpath)
    except KeyError:
        pass
    except Exception as exc:
        if stderr.isatty():
            print(str(exc), imgpath, file=stderr)
            tb()
        error = "EXIF"

    dir_chamber = re_chamber(dirname(imgpath))
    dir_experiment = re_experiment(dirname(imgpath))

    fn_chamber = re_chamber(basename(imgpath))
    fn_experiment = re_experiment(basename(imgpath))
    fn_time = re_time(basename(imgpath))

    return {
        "imgpath": imgpath,
        "qr_chamber": qr_chamber,
        "qr_experiment": qr_experiment,
        "qr_codes": codes,
        "pixel_mean": pixel_mean,
        "exif_time": exif_time,
        "dir_chamber": dir_chamber,
        "dir_experiment": dir_experiment,
        "fn_chamber": fn_chamber,
        "fn_experiment": fn_experiment,
        "fn_time": fn_time,
        "file_size": file_size,
        "error": error,
    }


def timestreamify(dat, dest):
    time = None
    chamber = "Unknown"
    experiment = "Unknown"

    # set above in reverse priority order
    if dat["fn_chamber"] is not None:
        chamber = dat["fn_chamber"]
    if dat["qr_chamber"] is not None:
        chamber = dat["qr_chamber"]
    if dat["dir_chamber"] is not None:
        chamber = dat["dir_chamber"]

    if dat["fn_experiment"] is not None:
        experiment = dat["fn_experiment"]
    if dat["qr_experiment"] is not None:
        experiment = dat["qr_experiment"]
    if dat["dir_experiment"] is not None:
        experiment = dat["dir_experiment"]

    if dat["error"] is not None:
        experiment = f"ERROR_{experiment}"

    if dat["exif_time"] is not None:
        time = dat["exif_time"]
    if dat["fn_time"] is not None:
        time = dat["fn_time"]

    imgpath = dat["imgpath"]
    fn, ext = splitext(basename(imgpath))

    if time is None:
        destpath = f"{dest}/{chamber}/{experiment}/{fn}{ext}"
    else:
        destpath = time.strftime(f"{dest}/{chamber}/{experiment}/%Y/%Y_%m/%Y_%m_%d/%Y_%m_%d_%H/{experiment}~{chamber}_%Y_%m_%d_%H_%M_%S{ext}")
    destdir = dirname(destpath)
    cmd = f"mkdir -p {quote(destdir)} && mv -nv {quote(imgpath)} {quote(destpath)}"
    return cmd


def iso8601ify(dat):
    ret = {}
    for k, v in dat.items():
        if isinstance(v, datetime.datetime):
            ret[k] = v.isoformat()
        else:
            ret[k] = v
    return ret
