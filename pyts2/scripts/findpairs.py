# Copyright (c) 2018-2020 Kevin Murray <foss@kdmurray.id.au>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import os
from os import path as op
import fnmatch
import piexif
from PIL import Image
from sys import stderr
from tqdm import tqdm
import traceback


from pyts2.removalist import Removalist

EXIF_BLACKLIST = [
    ('Exif', piexif.ExifIFD.MakerNote),
    ('Exif', piexif.ExifIFD.InteroperabilityTag),
]

def find_all(basedirs):
    """
    basedirs: list of one or more source directories
    """
    pairs = defaultdict(list)
    for basedir in basedirs:
        for root, dirs, files in os.walk(basedir):
            for file in files:
                bn, ext = op.splitext(op.basename(file))
                if ext.lower() not in [".cr2", ".dng", ".raw", ".rw2", ".orf", ".jpg", ".jpeg"]:
                    continue
                pairs[bn.lower()].append(op.join(root, file))

    for base, files in pairs.items():
        files = list(sorted(files))
        if len(files) == 2:
            yield files


def exif_matches(file1, file2):
    """
    Determine whether the exif data matches between file1 and file2
    Omits checks for exif data listed in EXIF_BLACKLIST
    """
    try:
        ex1 = piexif.load(file1)
        ex2 = piexif.load(file2)
        for group in ['Exif', ]:
            allkeys = set(list(ex1[group].keys()) + list(ex2[group].keys()))
            for key in allkeys:
                if (group, key) in EXIF_BLACKLIST:
                    continue
                try:
                    f1 = ex1[group][key]
                    f2 = ex2[group][key]
                except KeyError:
                    return False

                if f1 != f2:
                    return False
        return True
    except Exception as exc:
        if stderr.isatty():
            traceback.print_exc(file=stderr)
        return False


def findpairs_main(input, rm_script, move_dest, force_delete, jpeg=True):
    """
    input: List of one or more source directories
    rm_script: A path. Calls Removalist to write a bash script that removes files to here
    move_dest: Don't remove, move to here
    force_delete: Delete files without asking
    jpeg: if True, rename '*.jpeg' with '*.jpg'
    """
    with Removalist(rm_script=rm_script, mv_dest=move_dest, force=force_delete) as rmer:
        for (f1, f2) in tqdm(find_all(input)):
            if exif_matches(f1, f2):
                if jpeg:
                    exts = {op.splitext(f)[1].lower().replace("jpeg", "jpg"): f
                            for f in (f1, f2)}
                    if ".jpg" in exts:
                        rmer.remove(exts[".jpg"])
                    else:
                        print("WARNING: no jpg in pair", f1, f2, file=stderr)
                else:
                    print(f1, f2, sep='\t')
