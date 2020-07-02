# Copyright (c) 2018 Kevin Murray <kdmfoss@gmail.com>
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from tqdm import tqdm
import msgpack

import copy
from collections import defaultdict
import csv
from os import path as op
import re
from sys import stderr, stdout, stdin
import traceback
import warnings

csv.register_dialect('tsv',
                     delimiter='\t',
                     doublequote=False,
                     escapechar='\\',
                     lineterminator='\n',
                     quotechar='"',
                     quoting=csv.QUOTE_NONNUMERIC)


class FatalPipelineError(Exception):
    pass

class TSPipeline(object):
    def __init__(self, *args, reporter=None):
        self.retcode = 0 
        self.n = 0
        self.steps = []
        for step in args:
            self.add_step(step)
        if reporter is None:
            reporter = ResultRecorder()
        self.report = reporter

    def add_step(self, step):
        if not hasattr(step, "process_file"):
            raise ValueError(f"step doesn't seem to be a pipeline step: {step}")
        self.steps.append(step)
        return self  # so one can chain calls

    def process_file(self, file):
        # This should mirror PipelineStep, so an entire pipeline can function
        # as a pipeline step
        for step in self.steps:
            file.report["Errors"] = None
            try:
                file = step.process_file(file)
            except FatalPipelineError as exc:
                if file is not None:
                    path = file.filename
                    if hasattr(file.fetcher, "pathondisk"):
                        path = file.fetcher.pathondisk
                    print(f"\n{exc.__class__.__name__}: {str(exc)} while processing '{path}'\n", file=stderr)
                    if stderr.isatty():
                        traceback.print_exc(file=stderr)
                    file.report["Errors"] = f"{exc.__class__.__name__}: {str(exc)}"
                    self.report.record(file.instant, **file.report)
                raise
            except Exception as exc:
                if file is not None:
                    path = file.filename
                    if hasattr(file.fetcher, "pathondisk"):
                        path = file.fetcher.pathondisk
                    print(f"\n{exc.__class__.__name__}: {str(exc)} while processing '{path}'\n", file=stderr)
                    if stderr.isatty():
                        traceback.print_exc(file=stderr)
                    file.report["Errors"] = f"{exc.__class__.__name__}: {str(exc)}"
                break
        if file is not None:
            self.report.record(file.instant, **file.report)
        return file

    def process(self, input_stream, ncpus=1, progress=True):
        try:
            from concurrent.futures import as_completed, ThreadPoolExecutor, ProcessPoolExecutor
            if ncpus > 1:
                with ProcessPoolExecutor(max_workers=ncpus) as executor:
                        for file in tqdm(executor.map(self.process_file, input_stream), unit=" files"):
                            if file is None:
                                continue
                            self.report.record(file.instant, **file.report)
                            self.n += 1
                            yield file
            else:
                for file in tqdm(input_stream, unit=" files"):
                    file = self.process_file(file)
                    if file is None:
                        continue
                    self.report.record(file.instant, **file.report)
                    self.n += 1
                    yield file
        except FatalPipelineError as exc:
            print(f"Apologies, we encountered a fatal pipeline error, and are stopping processing. The error is:\n{str(exc)}", file=stderr)
            self.retcode=1

    def __call__(self, *args, **kwargs):
        yield from self.process(*args, **kwargs)

    def process_to(self, input_stream, output, ncpus=1):
        for done in self.process(input_stream, ncpus=ncpus):
            output.write(done)

    def write(self, file):
        # TODO needed so that pipelines can be used as files
        pass

    def read(self, file):
        # TODO needed so that pipelines can be used as files
        pass

    def finish(self):
        for step in self.steps:
            step.finish()
            if hasattr(step, "report") and isinstance(step.report, ResultRecorder):
                self.report.merge(step.report)
                step.report.close()
        self.report.close()


class ResultRecorder(object):

    def __init__(self):
        self.fields = []
        self.data = defaultdict(dict)

    def record(self, instant, **kwargs):
        for key, val in kwargs.items():
            if key not in self.fields:
                self.fields.append(key)
            self.data[repr(instant)].update(kwargs.copy())

    def merge(self, reporter):
        for inst, data in reporter.data.items():
            for key in data:
                if key not in self.fields:
                    self.fields.append(key)
            self.data[inst].update(data)

    def save(self, outpath, delim="\t"):
        if len(self.data) < 1:
            # No data, don't make file
            return
        with open(outpath, "w") as fh:
            tsvw = csv.writer(fh, dialect='tsv')
            tsvw.writerow(["Instant"] + self.fields)
            for instant, record in sorted(self.data.items()):
                line = [instant, ]
                for field in self.fields:
                    val = record.get(field, None)
                    if val is None:
                        val = "NA"
                    if isinstance(val, str):
                        val = re.sub(r"\s+", " ", val, re.IGNORECASE | re.MULTILINE)
                    line.append(val)
                tsvw.writerow(line)

    def close(self):
        pass

class LiveResultRecorder(ResultRecorder):

    def __init__(self, fileorpath):
        if hasattr(fileorpath, "write"):
            self.file = fileorpath
        else:
            self.file = open(fileorpath)

    def record(self, instant, **kwargs):
        dat = {"instant": repr(instant)}
        dat.update(kwargs)
        self.file.write(msgpack.packb(dat))

    def merge(self, reporter):
        for inst, data in reporter.data.items():
            self.record(inst, **data)

    def close(self):
        self.file.close()

    def save(self):
        pass


##########################################################################################
#                                     Pipeline steps                                     #
##########################################################################################


class PipelineStep(object):
    """A generic base class for pipeline steps.

    All pipeline steps should implement a method called `process_file` that accepts one
    argument `file`, and returns either TimestreamFile or a subclass of it.
    """

    def process_file(self, file):
        return file

    def finish(self):
        pass


class ResultRecorderStep(PipelineStep):

    def __init__(self, output_file):
        self.n = 0
        self.output_file = output_file
        self.report = ResultRecorder()
        self.write_interval = 1000  # write results every write_interval images

    def process_file(self, file):
        self.report.record(file.instant, **file.report)
        self.n += 1
        if self.n % self.write_interval == 0:
            self.report.save(self.output_file)

    def finish(self):
        self.report.save(self.output_file)


class CopyStep(PipelineStep):
    """Does Nothing"""
    pass


class TeeStep(PipelineStep):
    """Execute another step or pipeline with no side effects on each `file`"""

    def __init__(self, other_pipeline):
        self.pipe = other_pipeline

    def process_file(self, file):
        self.pipe.process_file(copy.deepcopy(file))
        return file


class WriteFileStep(PipelineStep):
    """Write each file to output, without changing the file"""

    def __init__(self, output):
        self.output = output

    def process_file(self, file):
        self.output.write(file)
        return file


class FileStatsStep(PipelineStep):
    def process_file(self, file):
        file.report.update({"FileName": op.basename(file.filename),
                            "FileSize": len(file.content)})
        return file
