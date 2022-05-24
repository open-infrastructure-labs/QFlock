#!/usr/bin/python3

import os
import subprocess
import argparse
from argparse import RawTextHelpFormatter
from glob import glob


class CompareFiles:
    """Application for comparing output files."""

    exception_list = ["2", "13", "24a", "24b", "43", "48", "59"]
    # exception_list = []

    def __init__(self):
        self._args = None

    def get_parser(self):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="App to compare directories.\n")

        parser.add_argument("--debug", "-D", action="store_true",
                            help="enable debug output")
        parser.add_argument("--verbose", "-V", action="store_true",
                            help="Show all skipped and mismatched files")
        parser.add_argument("--meld", action="store_true",
                            help="run meld on diff")
        parser.add_argument("--dir1", default=None,
                            help="first directory")
        parser.add_argument("--dir2", default=None,
                            help="second directory to compare")
        return parser

    def get_files_dict(self, path):
        directories = glob(os.path.join(path, "*"))
        files = {}
        for d in directories:
            key = os.path.split(d)
            query = key[1].split(".sql")[0]
            if os.path.isdir(d) and query not in CompareFiles.exception_list:
                files[key[1]] = glob(os.path.join(d, "*.csv"))
        return files

    def compare(self):
        match_count = 0
        mismatch_count = 0
        missing_count = 0
        files_dict1 = self.get_files_dict(self._args.dir1)
        files_dict2 = self.get_files_dict(self._args.dir2)
        for d in sorted(files_dict1.keys()):
            if d in files_dict2 and len(files_dict2[d]) and len(files_dict1[d]):
                # print(f"{d} {files_dict1[d][0]} {files_dict2[d][0]}")
                rc = subprocess.call("/usr/bin/diff -q {} {}".format(files_dict1[d][0],
                                                                     files_dict2[d][0]),
                                     shell=True, stdout=subprocess.DEVNULL)
                if rc != 0:
                    if self._args.debug or self._args.verbose:
                        print(f"{files_dict1[d][0]}  {files_dict2[d][0]} differ")
                    if self._args.meld:
                        subprocess.call("meld {} {}".format(files_dict1[d][0], files_dict2[d][0]), shell=True)
                    mismatch_count += 1
                else:
                    if self._args.debug:
                        print(f"{files_dict1[d][0]}  {files_dict2[d][0]} files match")
                    match_count += 1
            else:
                if len(files_dict1[d]):
                    missing_count += 1
                    if self._args.debug or self._args.verbose:
                        print(f"{files_dict1[d][0]} not found in dest")
        print(f"success/failure/missing: {match_count}/{mismatch_count}/{missing_count}")

    def run(self):
        self._args = self.get_parser().parse_args()

        if self._args.dir1 and self._args.dir2:
            self.compare()


if __name__ == "__main__":
    c = CompareFiles()
    c.run()
