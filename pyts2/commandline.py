# Copyright (c) 2018-2020 Kevin Murray <foss@kdmurray.id.au>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
from click import Choice, Path, DateTime
from tqdm import tqdm
import numpy as np

import pyts2
from pyts2 import TimeStream
from pyts2.timestream import FileContentFetcher
from pyts2.time import TimeFilter, parse_date
from pyts2.pipeline import *
from pyts2.pipeline.base import LiveResultRecorder
from pyts2.utils import CatchSignalThenExit
from pyts2.removalist import Removalist


from os.path import dirname, basename, splitext, getsize, realpath
from sys import stdout, stderr, stdin, exit  # noqa
from csv import DictWriter
import datetime
import argparse
import multiprocessing as mp
import re
import traceback
from shlex import quote
import os
import shutil
import sys


def getncpu():
    return int(os.environ.get("PBS_NCPUS", mp.cpu_count()))


def valid_date(s):
    try:
        return parse_date(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Not a valid date in Y-m-d form: '{s}'.")


def valid_time(s):
    try:
        return parse_date(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Not a valid time in H-M-S form: '{s}'.")


@click.group()
def tstk_main():
    pass


@tstk_main.command()
def version():
    print("tstk version", pyts2.__version__)


@tstk_main.command()
@click.option("--force", default=False,
              help="Force writing to an existing stream")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files")
@click.argument("input")
@click.argument("output")
def bundle(force, informat, bundle, input, output):
    input = TimeStream(input, format=informat)
    if os.path.exists(output) and not force:
        click.echo(f"ERROR: output exists: {output}", err=True)
        sys.exit(1)
    output = TimeStream(output, bundle_level=bundle)
    for image in input:
        with CatchSignalThenExit():
            output.write(image)
        click.echo(f"Processing {image}")


@tstk_main.command()
@click.option("--output", "-o", default=None,
              help="Output TSV file name")
@click.option("--ncpus", "-j", default=getncpu(),
              help="Number of parallel workers")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--telegraf-host", default=None,
              help="Telegraf reporting host")
@click.option("--telegraf-port", default=8092,
              help="Telegraf reporting port")
@click.option("--telegraf-metric", default='tstk_audit',
              help="Telegraf reporting metric name")
@click.argument("input")
def audit(input, output, telegraf_host, telegraf_port, telegraf_metric, ncpus=1, informat=None):
    if output is None and telegraf_host is None:
        print("ERROR: must give one of --output or --telegraf-host")
        sys.exit(1)

    pipe = TSPipeline(
        FileStatsStep(),
        CalculateEVStep(),
        DecodeImageFileStep(),
        ImageMeanColourStep(),
        ScanQRCodesStep(),
    )

    if telegraf_host is not None:
        pipe.add_step(TelegrafRecordStep(
            metric_name=telegraf_metric,
            telegraf_host=telegraf_host,
            telegraf_port=telegraf_port,
        ))

    ints = TimeStream(input, format=informat)
    try:
        for image in pipe.process(ints, ncpus=ncpus):
            if output is not None:
                if pipe.n % 1000 == 0:
                    pipe.report.save(output)
    finally:
        if output is not None:
            pipe.report.save(output)
        fmt = "" if informat is None else f":{informat}"
        click.echo(f"Audited {input}{fmt}, found {pipe.n} files")

####################################################################################################
#                                              RESIZE                                              #
####################################################################################################
@tstk_main.command()
@click.option("--output", "-o", required=True,
              help="Output TimeStream")
@click.option("--ncpus", "-j", default=getncpu(),
              help="Number of parallel workers")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--outformat", "-f", default="jpg", type=Choice(("jpg", "png", "tif")),
              help="Output image format")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files.")
@click.option("--mode", "-m", default='resize', type=Choice(('resize', 'centrecrop')),
              help="Either resize whole image to --size, or crop out central " +
                   "--size pixels at original resolution.")
@click.option("--size", "-s", default='720x',
              help="Output size. Use ROWSxCOLS. One of ROWS or COLS can be omitted to keep aspect ratio.")
@click.option("--flat", is_flag=True, default=False,
              help="Output all images to a single directory (flat timestream structure).")
@click.argument("input")
def downsize(input, output, ncpus, informat, outformat, size, bundle, mode, flat):
    if mode == "resize":
        downsizer = ResizeImageStep(geom=size)
    elif mode == "centrecrop" or mode == "crop":
        downsizer = CropCentreStep(geom=size)
    pipe = TSPipeline(
        DecodeImageFileStep(),
        downsizer,
        EncodeImageFileStep(format=outformat),
    )
    ints = TimeStream(input, format=informat)
    outts = TimeStream(output, format=outformat, bundle_level=bundle, add_subsecond_field=True, flat_output=flat)
    try:
        pipe.process_to(ints, outts, ncpus=ncpus)
    finally:
        click.echo(f"{mode} {input}:{informat} to {output}:{outformat}, found {pipe.n} files")


####################################################################################################
#                                              INGEST                                              #
####################################################################################################
@tstk_main.command()
@click.argument("input", type=Path(readable=True, exists=True))
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--output", "-o", required=True, type=Path(writable=True),
              help="Archival bundled TimeStream")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files.")
@click.option("--ncpus", "-j", default=getncpu(),
              help="Number of parallel workers")
@click.option("--downsized-output", "-s", default=None,
              help="Output a downsized copy of the images here")
@click.option("--downsized-size", "-S", default='720x',
              help="Downsized output size. Use ROWSxCOLS. One of ROWS or COLS can be omitted to keep aspect ratio.")
@click.option("--downsized-bundle", "-B", type=Choice(TimeStream.bundle_levels), default="root",
              help="Level at which to bundle downsized images.")
@click.option("--audit-output", "-a", type=Path(writable=True), default=None,
              help="Audit log output TSV. If given, input images will be audited, with the log saved here.")
def ingest(input, informat, output, bundle, ncpus, downsized_output, downsized_size, downsized_bundle, audit_output):
    ints = TimeStream(input, format=informat)
    outts = TimeStream(output, bundle_level=bundle)

    steps = [WriteFileStep(outts)]

    # if downsized_output is not None or audit_output is not None:
    #    steps.append(DecodeImageFileStep())

    if audit_output is not None:
        audit_pipe = TSPipeline(
            FileStatsStep(),
            CalculateEVStep(),
            DecodeImageFileStep(),
            ImageMeanColourStep(),
            ScanQRCodesStep(),
        )
        steps.append(audit_pipe)

    if downsized_output is not None:
        downsized_ts = TimeStream(downsized_output, bundle_level=downsized_bundle, add_subsecond_field=True)
        downsize_pipeline = TSPipeline(
            DecodeImageFileStep(),
            ResizeImageStep(geom=downsized_size),
            EncodeImageFileStep(format="jpg"),
            WriteFileStep(downsized_ts),
        )
        steps.append(downsize_pipeline)

    pipe = TSPipeline(*steps)

    try:
        for image in pipe.process(ints, ncpus=ncpus):
            pass
    finally:
        pipe.finish()
        if audit_output is not None:
            pipe.report.save(audit_output)
        ifmt = f":{informat}" if informat is not None else ""
        click.echo(f"Ingested {input}{ifmt} to {output}, found {pipe.n} files")
        sys.exit(pipe.retcode)

@tstk_main.command()
@click.option("--input", "-i", default=stdin, type=click.File("r"),
              help="file of file names to input (default stdin).")
@click.option("--inotify-watch", "-I", default=None, type=click.Path(writable=True, file_okay=False, dir_okay=True),
              help="watch DIR for changes, ingest the new files.")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--output", "-o", required=True, type=Path(writable=True),
              help="Archival bundled TimeStream")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files.")
@click.option("--downsized-output", "-s", default=None,
              help="Output a downsized copy of the images here")
@click.option("--downsized-size", "-S", default='720x',
              help="Downsized output size. Use ROWSxCOLS. One of ROWS or COLS can be omitted to keep aspect ratio.")
@click.option("--downsized-bundle", "-B", type=Choice(TimeStream.bundle_levels), default="root",
              help="Level at which to bundle downsized images.")
@click.option("--telegraf-host", default="localhost",
              help="Telegraf reporting host")
@click.option("--telegraf-port", default=8092,
              help="Telegraf reporting port")
@click.option("--telegraf-metric", default='tstk_live_ingest',
              help="Telegraf reporting metric name")
@click.option("--NUKE", is_flag=True, default=False,
              help="DELETE file UNSAFELY as it finishes processsing")
def liveingest(input, informat, output, bundle, downsized_output, downsized_size, downsized_bundle, telegraf_host, telegraf_port, telegraf_metric, inotify_watch, nuke):

    ints = TimeStream(format=informat)
    outts = TimeStream(output, bundle_level=bundle)

    pipe = TSPipeline()
    pipe.add_step(WriteFileStep(outts))

    audit_pipe = TSPipeline(
        FileStatsStep(),
        DecodeImageFileStep(),
        ImageMeanColourStep(),
        ScanQRCodesStep(),
        TelegrafRecordStep(
            metric_name=telegraf_metric,
            telegraf_host=telegraf_host,
            telegraf_port=telegraf_port,
        ),
    )
    pipe.add_step(audit_pipe)

    if downsized_output is not None:
        downsized_ts = TimeStream(downsized_output, bundle_level=downsized_bundle, add_subsecond_field=True)
        downsize_pipeline = TSPipeline(
            DecodeImageFileStep(),
            ResizeImageStep(geom=downsized_size),
            EncodeImageFileStep(format="jpg"),
            WriteFileStep(downsized_ts),
        )
        pipe.add_step(TeeStep(downsize_pipeline))

    if nuke:
        pipe.add_step(UnsafeNuker())

    try:
        if inotify_watch is not None:
            instream = ints.from_inotify(inotify_watch)
        else:
            instream = ints.from_fofn(input)
        for image in instream:
            image = pipe.process_file(image)
            click.echo(f"{image.instant} Done")
            pipe.n += 1
    finally:
        pipe.finish()
        ifmt = f":{informat}" if informat is not None else ""
        click.echo(f"Ingested {input.name}{ifmt} to {output}, found {pipe.n} files")


@tstk_main.command()
@click.option("--resource", "-r", required=True, type=Path(readable=True),
              help="Archival bundled TimeStream")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--pixel-distance", "-p", default=None, type=float,
              help="Fuzzily match images based on distance in pixel units. Formula is abs(X - Y)/maxpixelval/npixel, i.e. 0 for no distance and 1 for all white vs all black.")
@click.option("--distance-file", default=None, type=Path(writable=True),
              help="Write log of each ephemeral file's distance to tsv file")
@click.option("--only-check-exists", default=False, is_flag=True,
              help="Only check that a file at the same timepoint exists.")
@click.option("--rm-script", "-s", default=None, type=Path(writable=True),
              help="Write a bash script that removes files to here")
@click.option("--move-dest", "-m", default=None, type=Path(writable=True), metavar="DEST",
              help="Don't remove, move to DEST")
@click.option("--yes", "-y", "force_delete", default=False, is_flag=True,
              help="Delete files without asking")
@click.argument("ephemerals", type=Path(readable=True), nargs=-1)
def verify(ephemerals, resource, informat, force_delete, rm_script, move_dest, pixel_distance, distance_file, only_check_exists):
    """
    Verify images from each of EPHEMERAL, ensuring images are in --resources.
    """
    resource_ts = TimeStream(resource, format=informat)
    decoder = DecodeImageFileStep()
    resource_ts.index()

    if rm_script is not None:
        with open(rm_script, "w") as fh:
            print("# rmscript for", *ephemerals, file=fh)

    with Removalist(rm_script=rm_script, mv_dest=move_dest, force=force_delete) as rmer:
        if distance_file is not None:
            distance_file = open(distance_file, "w")
            print("ephemeral_image\tresource_image\tdistance", file=distance_file)
        for ephemeral in ephemerals:
            click.echo(f"Crawling ephemeral timestream: {ephemeral}")
            ephemeral_ts = TimeStream(ephemeral, format=informat)
            try:
                for image in tqdm(ephemeral_ts, unit=" files"):
                    try:
                        res_img = resource_ts.getinstant(image.instant)
                        if not isinstance(image.fetcher, FileContentFetcher):
                            click.echo(f"WARNING: can't delete {image.filename} as it is bundled", err=True)
                            continue
                        if only_check_exists:
                            rmer.remove(image.fetcher.pathondisk)
                        elif pixel_distance is not None:
                            eimg = decoder.process_file(image)
                            rimg = decoder.process_file(res_img)
                            if eimg.pixels.shape != rimg.pixels.shape:
                                if distance_file is not None:
                                    print(basename(image.filename), basename(res_img.filename), "NA", file=distance_file)
                                continue
                            dist = np.mean(abs(eimg.pixels - rimg.pixels))
                            if distance_file is not None:
                                print(basename(image.filename), basename(res_img.filename), dist, file=distance_file)
                            if dist < pixel_distance:
                                rmer.remove(realpath(image.fetcher.pathondisk))
                        elif image.md5sum == res_img.md5sum:
                            rmer.remove(realpath(image.fetcher.pathondisk))
                    except KeyError:
                        tqdm.write(f"{image.instant} not in {resource}")
                        if distance_file is not None:
                            print(basename(image.filename), "", "", file=distance_file)
                    except Exception as exc:
                        click.echo(f"WARNING: error in resources lookup of {image.filename}: {str(exc)}", err=True)
                        if stderr.isatty():
                            traceback.print_exc(file=stderr)
            except KeyboardInterrupt:
                print("\n\nExiting cleanly", file=stderr)
                break
    if distance_file is not None:
        distance_file.close()


@tstk_main.command()
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--dims", "-d", type=str, required=True,
              help="Dimension of super-image, in units of sub-images, ROWSxCOLS")
@click.option("--order", "-O", default="colsright",
              type=Choice(["colsright", "colsleft", "rowsdown", "rowsup"]),
              help="Order in which images are taken (cols or rows, left orright)")
# time
@click.option("--truncate-time", type=str, default=None, metavar="TIME",
              help="Truncate time to TIME")
# audit
@click.option("--audit-output", "-a", type=Path(writable=True), default=None,
              help="Audit log output TSV. If given, input images will be audited, with the log saved here.")
# composite/mosaicing
@click.option("--composite-size", "-s", type=str, default="200x300",
              help="Size of each sub-image in a composite, ROWSxCOLS")
@click.option("--composite-format", "-f", type=str, default="jpg",
              help="File format of composite output images")
@click.option("--composite-output", "-o", type=str, default=None,
              help="Output timestream for composite images")
@click.option("--composite-bundling", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle composite image output")
@click.option("--composite-centrecrop", "-S", type=float, default=0.5, metavar="PROPORTION",
              help="Crop centre of each image. takes centre PROPORTION h x w from each image")
# verbatim bundling
@click.option("--bundle-output", "--bo", type=str, default=None,
              help="Output timestream for verbtaim import images")
@click.option("--bundle-level", "--bb", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle verbatim images")
# recoding
@click.option("--recoded-output", "--ro", type=str, default=None,
              help="Output timestream for recoded import images")
@click.option("--recoded-format", "--rf", type=str, default=None,
              help="File format of  images")
@click.option("--recoded-bundling", "--rb", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle recoded images")
# source removal
@click.option("--rm-script", "-x", type=Path(writable=True), metavar="FILE",
              help="Write a script which deletes input files to FILE.")
@click.option("--mv-destination", type=Path(), metavar="DIR",
              help="Instead of deleting input files, move files to DIR. (see --rm-script)")
@click.argument("input")
def gvmosaic(input, informat, dims, order, audit_output, composite_bundling,
             composite_format, composite_size, composite_output, composite_centrecrop,
             bundle_output, bundle_level, recoded_output, recoded_format,
             recoded_bundling, rm_script, mv_destination, truncate_time):

    from pyts2.pipeline.gigavision import GigavisionMosaicStep

    ints = TimeStream(input, format=informat)

    composite_ts = TimeStream(composite_output, bundle_level=composite_bundling, add_subsecond_field=True)
    steps = []

    if truncate_time is not None:
        steps.append(TruncateTimeStep(truncate_time))

    if bundle_output is not None:
        verbatim_ts = TimeStream(bundle_output, bundle_level=bundle_level)
        steps.append(WriteFileStep(verbatim_ts))

    # decode image
    steps.append(DecodeImageFileStep())

    # run audit pipeline
    if audit_output is not None:
        audit_pipe = TSPipeline(
            FileStatsStep(),
            ImageMeanColourStep(),
            ScanQRCodesStep(),
        )
        steps.append(audit_pipe)

    if recoded_output is not None:
        # run recode pipeline
        recoded_ts = TimeStream(recoded_output, bundle_level=recoded_bundling)
        recoded_pipe = TSPipeline(
            EncodeImageFileStep(format=recoded_format),
            WriteFileStep(recoded_ts),
        )
        steps.append(TeeStep(recoded_pipe))

    # do mosaicing
    steps.append(
        TSPipeline(GigavisionMosaicStep(
            dims, composite_ts, subimgres=composite_size, order=order,
            output_format=composite_format, centrecrop=composite_centrecrop,
            rm_script=rm_script, mv_destination=mv_destination,
        ))
    )

    # assemble total pipeline
    pipe = TSPipeline(*steps)

    # run pipeline
    try:
        for image in pipe.process(ints):
            pass
    finally:
        pipe.finish()
        if audit_output is not None:
            pipe.report.save(audit_output)
        sys.exit(pipe.retcode)


@tstk_main.command()
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files")
@click.option("--start-time", "-S",
              help="Start time of day (inclusive)")
@click.option("--end-time", "-E",
              help="End time of day (inclusive)")
@click.option("--interval", "-i", type=int,
              help="Interval in minutes")
@click.option("--start-date", "-s",
              help="Start time of day (inclusive)")
@click.option("--end-date", "-e",
              help="End time of day (inclusive)")
@click.argument("input")
@click.argument("output")
def cp(informat, bundle, input, output, start_time, start_date, end_time, end_date, interval):
    tfilter = TimeFilter(start_date, end_date, start_time, end_time)
    if interval is not None:
        raise NotImplementedError("haven't done interval restriction yet")
    output = TimeStream(output, bundle_level=bundle)
    for image in tqdm(TimeStream(input, format=informat, timefilter=tfilter)):
        with CatchSignalThenExit():
            output.write(image)

@tstk_main.command()
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--start-time", "-S",
              help="Start time of day (inclusive)")
@click.option("--end-time", "-E",
              help="End time of day (inclusive)")
@click.option("--interval", "-i", type=int,
              help="Interval in minutes")
@click.option("--start-date", "-s",
              help="Start time of day (inclusive)")
@click.option("--end-date", "-e",
              help="End time of day (inclusive)")
@click.argument("input")
def ls(informat, input, start_time, start_date, end_time, end_date, interval):
    tfilter = TimeFilter(start_date, end_date, start_time, end_time)
    if interval is not None:
        raise NotImplementedError("haven't done interval restriction yet")
    for image in TimeStream(input, format=informat, timefilter=tfilter):
        print(image.instant)


@tstk_main.command()
@click.option("--output", "-o", type=click.File("w"), default=stdout,
              help="Output TSV file name")
@click.option("--ncpus", "-j", default=getncpu(),
              help="Number of parallel workers")
@click.option("--timestreamify-script", "-t", type=click.File("w"),
              help="Write script to sort images to FILE.")
@click.option("--timestreamify-destination", "-d", type=str,
              help="-t script moves files to DIR")
@click.argument("input", nargs=-1)
def imgscan(input, timestreamify_script, timestreamify_destination, output, ncpus):
    from pyts2.scripts.imgscan import find_files, is_image, iso8601ify, scanimage, timestreamify
    files = [x for x in tqdm(find_files(*input), desc="Find images", unit=" files") if is_image(x)]
    print(f"Found {len(files)} files.", file=stderr)

    # set up tsv
    hdr = ["imgpath", "qr_chamber", "qr_experiment", "qr_codes", "pixel_mean", "exif_time", "file_size",
           "dir_chamber", "dir_experiment", "fn_chamber", "fn_experiment", "fn_time", "error"]
    out = DictWriter(output, fieldnames=hdr, dialect="excel-tab")
    out.writeheader()

    pool = mp.Pool(ncpus)
    err = 0
    for i, res in enumerate(tqdm(pool.imap_unordered(scanimage, files), total=len(files), desc="Scan images", unit=" images")):
        out.writerow(iso8601ify(res))
        if timestreamify_script is not None:
            print(timestreamify(res, timestreamify_destination), file=timestreamify_script)
        if res["error"] is not None:
            err += 1
        if i % 100 == 0:
            output.flush()
            if timestreamify_script is not None:
                timestreamify_script.flush()
    pool.close()
    print(f"Finished: {len(files)} images, {err} errors.", file=stderr)


@tstk_main.command()
@click.option("--rm-script", "-s", default=None, type=Path(writable=True),
              help="Write a bash script that removes files to here")
@click.option("--move-dest", "-m", default=None, type=Path(writable=True), metavar="DEST",
              help="Don't remove, move to DEST")
@click.option("--yes", "-y", "force_delete", default=False, is_flag=True,
              help="Delete files without asking")
@click.argument("input", nargs=-1)
def findpairs(input, rm_script, move_dest, force_delete):
    """Finds pairs of XXXXXX.{jpg,cr2} or similar with identical metadata & filename."""
    from pyts2.scripts.findpairs import findpairs_main
    findpairs_main(input, rm_script, move_dest, force_delete)


@tstk_main.command()
@click.option("--output", "-o", type=Path(writable=True),
              help="Output video file name")
@click.option("--ffmpeg-path", "-b", default="ffmpeg",
              help="ffmpeg command/path")
@click.option("--ffmpeg-args", default=None,
              help="ffmpeg custom commandline")
@click.option("--framerate", "-r", default=10,
              help="Video frame rate")
# FIXME: make this take the typical tstk 720x-style arg, and convert to ffmpeg -f:v scale.... arg
@click.option("--scaling", "-s", default=None,
              help="FFMpeg args for scaling, default scales up so sides are even length")
@click.option("--ncpus", "-j", default=getncpu(),
              help="Number of threads for ffmpeg to use")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.argument("input")
def video(input, output, ffmpeg_path, ffmpeg_args, framerate, scaling, ncpus, informat):
    """Creates a standard timelapse video from timestream"""

    from pyts2.pipeline.video import VideoEncoder, ImageWatermarker

    # ffmpeg_command = ffmpeg_path+' '+ffmpeg_arg
    ints = TimeStream(input, format=informat)
    pipe = TSPipeline(
        DecodeImageFileStep(),
        ImageWatermarker(),
        EncodeImageFileStep(format="jpg"),
        VideoEncoder(output, ffmpeg_args=ffmpeg_args, ffmpeg_path=ffmpeg_path,
            rate=framerate, threads=ncpus, scaling=scaling),
    )
    try:
        for image in pipe.process(ints, ncpus=1):
            pass
    finally:
        pipe.finish()
        sys.exit(pipe.retcode)

if __name__ == "__main__":
    tstk_main()
