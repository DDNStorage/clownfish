# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for defining a command written in python
"""
import sys
import os
import shutil
import traceback
import getopt
import filelock

from pylustre import clog
from pylustre import time_util
from pylustre import utils


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s [--config|-c <config>] [--logdir|-d <logdir>] [--help|-h]\n"
                 "        logdir: the dir path to save logs\n"
                 "        config: config file path"
                 % sys.argv[0])


def main(default_config_fpath, default_log_parent, main_func):
    """
    The main function of a command
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    reload(sys)
    sys.setdefaultencoding("utf-8")

    options, args = getopt.getopt(sys.argv[1:],
                                  "c:i:h",
                                  ["config=",
                                   "help",
                                   "logdir="])

    config_fpath = None
    workspace = None
    for opt, arg in options:
        if opt == '-c' or opt == "--config" or opt == "-config":
            config_fpath = arg
        elif opt == '-l' or opt == "--logdir" or opt == "-logdir":
            workspace = arg
        elif opt == '-h' or opt == "--help" or opt == "-help":
            usage()
            sys.exit(0)
        else:
            usage()
            sys.exit(1)

    if len(args) != 0:
        usage()
        sys.exit(1)

    if workspace is None:
        identity = time_util.local_strftime(time_util.utcnow(),
                                            "%Y-%m-%d-%H_%M_%S")
        workspace = default_log_parent + "/" + identity
    if config_fpath is None:
        config_fpath = default_config_fpath

    command = "mkdir -p %s" % workspace
    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        utils.eprint("failed to run command [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]" %
                     (command,
                      retval.cr_exit_status,
                      retval.cr_stdout,
                      retval.cr_stderr))
        sys.exit(-1)

    log = clog.get_log(resultsdir=workspace, exclusive=False)
    log.cl_info("starting to run [%s] using config [%s], "
                "please check [%s] for more log" %
                (main_func.__name__, config_fpath, workspace))

    if not os.path.exists(config_fpath):
        log.cl_error("config [%s] doesn't exist, using empty config",
                     config_fpath)
        ret = main_func(log, workspace, None)
        sys.exit(ret)
    elif not os.path.isfile(config_fpath):
        log.cl_error("config [%s] is not a file", config_fpath)
        sys.exit(-1)

    config_fname = os.path.basename(config_fpath)
    save_fpath = workspace + "/" + config_fname
    log.cl_debug("copying config file from [%s] to [%s]",
                 config_fpath, save_fpath)
    if config_fpath != save_fpath:
        shutil.copyfile(config_fpath, save_fpath)

    lock_file = config_fpath + ".lock"
    lock = filelock.FileLock(lock_file)
    try:
        with lock.acquire(timeout=0):
            try:
                ret = main_func(log, workspace, config_fpath)
            except:
                ret = -1
                log.cl_error("exception: %s", traceback.format_exc())
            lock.release()
    except filelock.Timeout:
        ret = -1
        log.cl_error("someone else is holding lock of file [%s], aborting "
                     "to prevent conflicts", lock_file)
    sys.exit(ret)
