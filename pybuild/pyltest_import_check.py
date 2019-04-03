# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for checking the import of pyltest source code
"""
import sys

# Local libs
from pylcommon import clog


def check_import(log, fpath):
    """
    Check the import of file path
    """
    with open(fpath, "r") as fd:
        lines = fd.readlines()

    for line in lines:
        if line.startswith("from pyclownfish import"):
            log.cl_error("file [%s] imports library from pyclownfish, which "
                         "is not allowed", fpath)
            return -1
    return 0


def main():
    """
    Check the source code files
    """
    log = clog.get_log()
    for arg in sys.argv[1:]:
        log.cl_info("checking file [%s]", arg)
        ret = check_import(log, arg)
        if ret:
            log.cl_error("file [%s] imported wrong library", arg)
            sys.exit(-1)
    sys.exit(0)
