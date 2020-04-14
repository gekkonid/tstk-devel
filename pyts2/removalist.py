import os
import shutil
from sys import stdout, stderr, stdin, exit  # noqa
from os.path import dirname, basename, splitext, getsize, realpath
import click
from tqdm import tqdm
import traceback


class Removalist(object):
    def __init__(self, rm_script=None, mv_dest=None, force=False):
        self.force = force
        self.mv_dest = mv_dest
        self.to_delete = []

        self.rm_script = rm_script
        if self.rm_script is not None:
            with open(self.rm_script, "w") as fh:
                print("# rmscript", file=fh)

    def _do_delete(self):
        if self.rm_script is not None:
            with open(self.rm_script, "a") as fh:
                cmd = "rm -vf" if self.mv_dest is None else f"mv -n -t {self.mv_dest}"
                for f in self.to_delete:
                    print(cmd, realpath(f), file=fh)
        else:
            if not self.force:
                if self.mv_dest is not None:
                    click.echo(f"Will move the following files to {self.mv_dest}:")
                else:
                    click.echo("Will delete the following files:")
                for f in self.to_delete:
                    click.echo(f"\t{f}")
            if self.force or click.confirm("Is that OK?"):
                for f in self.to_delete:
                    try:
                        if self.mv_dest is None:
                            os.unlink(f)
                        else:
                            os.makedirs(self.mv_dest, exist_ok=True)
                            shutil.move(f, self.mv_dest)
                    except Exception as exc:
                        tqdm.write(f"Error deleting {f}: {str(exc)}")
                        if stderr.isatty():
                            traceback.print_exc(file=stderr)
                tqdm.write(f"Deleted {len(self.to_delete)} files")
        self.to_delete = []

    def remove(self, filepath):
        if len(self.to_delete) > 1000:
            self._do_delete()
        self.to_delete.append(filepath)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._do_delete()
