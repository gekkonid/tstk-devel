# Copyright (c) 2018-2019 Kevin Murray <foss@kdmurray.id.au>

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
from click import Choice, Path, DateTime
from tqdm import tqdm

from pyts2 import TimeStream
from pyts2.timestream import FileContentFetcher
from pyts2.time import TimeFilter
from pyts2.pipeline import *
from pyts2.utils import CatchSignalThenExit

import argparse as ap
import os
from os.path import realpath
import shutil
import sys
import datetime


def valid_date(s):
    try:
        if isinstance(s, datetime.datetime) or isinstance(s, datetime.date):
            return s
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date in Y-m-d form: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def valid_time(s):
    try:
        if isinstance(s, datetime.datetime) or isinstance(s, datetime.time):
            return s
        return datetime.time.strptime(s, "%H:%M:%S")
    except ValueError:
        msg = "Not a valid date in Y-m-d form: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


@click.group()
def tstk_main():
    pass


@tstk_main.command()
@click.option("--force", default=False,
              help="Force writing to an existing stream")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files")
@click.argument("input")
                #help="Input files in timestream format of any form but msgpack")
@click.argument("output")
                #help="Output file or directory")
def bundle(force, informat, bundle, input, output):
    input = TimeStream(input, format=informat)
    if os.path.exists(output) and not force:
        click.echo(f"ERROR: output exists: {output}", err=True)
        sys.exit(1)
    output =  TimeStream(output, bundle_level=bundle)
    for image in input:
        with CatchSignalThenExit():
            output.write(image)
        click.echo(f"Processing {image}")


@tstk_main.command()
@click.option("--output", "-o", required=True,
              help="Output TSV file name")
@click.option("--ncpus", "-j", default=1,
              help="Number of parallel workers")
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.argument("input")
def audit(output, input, ncpus=1, informat=None):
    pipe = TSPipeline(
        FileStatsStep(),
        DecodeImageFileStep(),
        ImageMeanColourStep(),
        ScanQRCodesStep(),
    )

    ints = TimeStream(input, format=informat)
    try:
        for image in pipe.process(ints, ncpus=ncpus):
            if pipe.n % 1000 == 0:
                pipe.report.save(output)
    finally:
        pipe.report.save(output)
        click.echo(f"Audited {input}:{informat}, found {pipe.n} files")


####################################################################################################
#                                              RESIZE                                              #
####################################################################################################
@tstk_main.command()
@click.option("--output", "-o", required=True,
              help="Output TimeStream")
@click.option("--ncpus", "-j", default=1,
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
@click.option("--ncpus", "-j", default=1,
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

    #if downsized_output is not None or audit_output is not None:
    #    steps.append(DecodeImageFileStep())

    if audit_output is not None:
        audit_pipe = TSPipeline(
            FileStatsStep(),
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
        click.echo(f"Ingested {input}:{informat} to {output}, found {pipe.n} files")


@tstk_main.command()
@click.option("--ephemeral", "-e", type=Path(readable=True), required=True,
        help="Ephemeral image source location")
@click.option("--resource", "-r", required=True, type=Path(readable=True),
        help="Archival bundled TimeStream")
@click.option("--informat", "-F", default=None,
        help="Input image format (use extension as lower case for raw formats)")
@click.option("--rm-script", "-s", default=None, type=Path(writable=True),
        help="Write a bash script that removes files to here")
@click.option("--move-dest", "-m", default=None, type=Path(writable=True), metavar="DEST",
        help="Don't remove, move to DEST")
@click.option("--yes", "-y", "force_delete", default=False, is_flag=True,
        help="Delete files without asking")
def verify(ephemeral, resource, informat, force_delete, rm_script, move_dest):
    ephemeral_ts = TimeStream(ephemeral, format=informat)
    resource_ts = TimeStream(resource, format=informat)
    to_delete = []
    resource_images = resource_ts.instants
    try:
        for image in tqdm(ephemeral_ts, unit=" files"):
            try:
                res_img = resource_images[image.instant]
                if image.md5sum == res_img.md5sum:
                    if not isinstance(image.fetcher, FileContentFetcher):
                        click.echo(f"WARNING: can't delete {image.filename} as it is bundled", err=True)
                    to_delete.append(image.fetcher.pathondisk)
                res_img.clear_content()
            except KeyError:
                continue
            except Exception as exc:
                click.echo(f"WARNING: error in resources lookup of {image.filename}: {str(exc)}", err=True)
    finally:
        if rm_script is not None:
            with open(rm_script, "w") as fh:
                cmd = "rm -vf" if move_dest is None else f"mv -n -t {move_dest}"
                for f in to_delete:
                    print(cmd, realpath(f), file=fh)
        else:
            if move_dest is None:
                click.echo("Will delete the following files:")
            else:
                click.echo(f"Will move the following files to {move_dest}:")
            for f in to_delete:
                click.echo("\t{}".format(f))
            if force_delete or click.confirm("Is that OK?"):
                for f in to_delete:
                    if move_dest is None:
                        os.unlink(f)
                    else:
                        os.makedirs(move_dest, exist_ok=True)
                        shutil.move(f, move_dest)


@tstk_main.command()
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--dims", "-d", type=str, required=True,
              help="Dimension of super-image, in units of sub-images, ROWSxCOLS")
@click.option("--order", "-O", default="colsright",
              type=Choice(["colsright", "colsleft", "rowsdown", "rowsup"]),
              help="Order in which images are taken (cols or rows, left orright)")
@click.option("--audit-output", "-a", type=Path(writable=True), default=None,
              help="Audit log output TSV. If given, input images will be audited, with the log saved here.")
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
@click.option("--recoded-output", "--ro", type=str, default=None,
              help="Output timestream for recoded import images")
@click.option("--recoded-format", "--rf", type=str, default=None,
              help="File format of  images")
@click.option("--recoded-bundling", "--rb", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle recoded images")
@click.option("--rm-script", "-x", type=Path(writable=True), metavar="FILE",
              help="Write a script which deletes input files to FILE.")
@click.option("--mv-destination", type=Path(), metavar="DIR",
              help="Instead of deleting input files, move files to DIR. (see --rm-script)")
@click.argument("input")
def gvmosaic(input, informat, dims, order, audit_output, composite_bundling,
             composite_format, composite_size, composite_output, composite_centrecrop,
             recoded_output, recoded_format, recoded_bundling, rm_script, mv_destination):

    from pyts2.pipeline.gigavision import GigavisionMosaicStep

    ints = TimeStream(input, format=informat)

    composite_ts = TimeStream(composite_output, bundle_level=composite_bundling, add_subsecond_field=True)
    steps = []

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

    # run recode pipeline
    if recoded_output is not None:
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


@tstk_main.command()
@click.option("--informat", "-F", default=None,
              help="Input image format (use extension as lower case for raw formats)")
@click.option("--bundle", "-b", type=Choice(TimeStream.bundle_levels), default="none",
              help="Level at which to bundle files")
@click.option("--start-time", "-S", type=valid_time,
              help="Start time of day (inclusive)")
@click.option("--end-time", "-E", type=valid_time,
              help="End time of day (inclusive)")
@click.option("--interval", "-i", type=int,
              help="Interval in minutes")
@click.option("--start-date", "-s", type=valid_date,
              help="Start time of day (inclusive)")
@click.option("--end-date", "-e", type=valid_date,
              help="End time of day (inclusive)")
@click.argument("input")
@click.argument("output")
def cp(force, informat, bundle, input, output, start_time, start_date, end_time, end_date, interval):
    tfilter = TimeFilter(start_date, end_date, start_time, end_time)
    if interval is not None:
        raise NotImplementedError("haven't done interval restriction yet")
    output =  TimeStream(output, bundle_level=bundle)
    for image in TimeStream(input, format=informat, timefilter=tfilter):
        with CatchSignalThenExit():
            output.write(image)
        click.echo(f"Processing {image}")


if __name__ == "__main__":
    tstk_main()
