#!/bin/python3
# external deps
from tqdm import tqdm
import pytz
from influxdb import InfluxDBClient
# internal
import sys
import os
import csv
import re
import time
import json
import argparse
import datetime
from hashlib import md5
from collections import deque

BUFFER_SIZE = 10000
timezone = pytz.timezone("Australia/Canberra")
instant_fmt = "%Y_%m_%d_%H_%M_%S"


def exit_if_not(v):
    if not v:
        print("required environment variable not provided")
        exit(3)
    return v


INFLUXDB_URL = exit_if_not(os.environ.get("INFLUXDB_URL", "http://influxdb.traitcapture.org"))
INFLUXDB_USER = exit_if_not(os.environ.get("INFLUXDB_USER", None))
INFLUXDB_PASSWORD = exit_if_not(os.environ.get("INFLUXDB_PASSWORD", None))
INFLUXDB_PORT = int(exit_if_not(os.environ.get("INFLUXDB_PORT", 8086)))
INFLUXDB_DATABASE = exit_if_not(os.environ.get("INFLUXDB_DATABASE", "imaging"))


def value_or_none(d, k):
    v = d.get(k, None)
    if v == "NA":
        return None
    try:
        return float(v)
    except:
        pass
    return v


def add_value(m, row, k, t=None, strip=""):
    v = value_or_none(row, k.replace(strip, ""))
    if v is not None:
        if t is None:
            m['fields'][k] = v
        else:
            m['fields'][k] = t(v)


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", "-c", type=str, required=True,
                    help="Camera tag")
    # ap.add_argument("--camera-type", "-t", type=str, required=True,
    #                help="Camera type tag")
    ap.add_argument("--file-type", "-f", type=str, required=True,
                    help="Image filetype tag")
    ap.add_argument("--audit-file", "-a", type=str, required=True,
                    help="Audit log file (as produced by tstk ingest/audit)")

    args = ap.parse_args()

    client = InfluxDBClient(
        INFLUXDB_URL,
        INFLUXDB_PORT,
        INFLUXDB_USER,
        INFLUXDB_PASSWORD,
        INFLUXDB_DATABASE)

    path = args.audit_file
    total = file_len(path) - 1
    root, _ = os.path.splitext(os.path.basename(path))
    camera_tag = args.camera
    filetype_tag = args.file_type
    if "IPCam" in camera_tag:
        camera_type = "ipcamera"
    elif "Picam" in camera_tag:
        camera_type = "picam"
    elif camera_tag in ("kioloa-hill-GV01", "ARB-GV-ANU-HILL-C01"):
        camera_type = "gigavision"
    else:
        camera_type = "dslr"
    print(camera_tag, camera_type, filetype_tag)

    trname = None
    trlast = None
    buf = deque(maxlen=BUFFER_SIZE)
    x = 0
    n = 0
    retcode = 0
    with open(path) as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        for row in tqdm(reader, total=total):
            m = {
                "measurement": "image_audit",
                "fields": {},
                "tags": {
                    "camera_name": camera_tag,
                    "filetype": filetype_tag,
                    "camera_type": camera_type,
                },
            }

            try:
                add_value(m, row, "ImageMean_Red", t=float)
                add_value(m, row, "ImageMean_Green", t=float)
                add_value(m, row, "ImageMean_Blue", t=float)
                add_value(m, row, "ImageMean_Grey", t=float)
                add_value(m, row, "ImageMean_L", t=float)
                add_value(m, row, "ImageMean_a", t=float)
                add_value(m, row, "ImageMean_b", t=float)
                add_value(m, row, "FileSize", t=int)
                add_value(m, row, "FileName", t=str)

                error = row.get("Errors", None)
                if error is not None and error != "NA":
                    the_hash = md5(bytes(error, 'utf-8')).hexdigest()
                    the_type = error.split(":")[0]
                    m['tags']['error'] = "{}: {}".format(the_type, the_hash)

                instant = row.get("Instant")
                rem = re.match(r'^(\d\d\d\d_\d\d_\d\d_\d\d_\d\d_\d\d)(_00)?(_\w+)?$', instant)
                instant_tag = ""
                if rem is not None:
                    instant, _, instant_tag = rem.groups()
                    if instant_tag is None:
                        instant_tag = ""
                    instant_tag = instant_tag.strip().strip("_")
                m['tags']['instant_extra_value'] = instant_tag
                naive_instant = datetime.datetime.strptime(instant, instant_fmt)
                tzaware_instant = timezone.localize(naive_instant)
                m['time'] = tzaware_instant.isoformat()

                qrcodes = row.get("QRCodes", None)
                if qrcodes is not None and qrcodes != "NA":
                    qrcodes = ";".join(sorted(qrcodes.split(";")))
                    m['fields']['QRCodes'] = qrcodes
                    match = re.search(r'(ATK\d+|GRE\d+|BVZ\d+|TR\d+)', qrcodes)
                    if match is not None:
                        trname = match.group()
                        trlast = tzaware_instant
                        m['tags']['detected_user'] = trname
                        m['tags']['estimated_user'] = trname
                    m['fields']['NumQRCodes'] = int(len(qrcodes.split(";")))
                else:
                    m['fields']['NumQRCodes'] = int(0)
                    if trname is not None:
                        if (tzaware_instant - trlast) < datetime.timedelta(days=1):
                            m['tags']['estimated_user'] = trname
                buf.append(m)
            except Exception as e:
                print(str(e))
                retcode = 1
            x += 1
            if x >= BUFFER_SIZE:
                n += x
                client.write_points(buf)
                x = 0
                time.sleep(0.2)
    client.write_points(buf)
    return retcode


if __name__ == "__main__":
    sys.exit(main())
