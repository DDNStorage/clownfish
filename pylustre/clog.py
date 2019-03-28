# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Logging library
Rerfer to /usr/lib64/python2.7/logging for more information

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import logging
import os
import threading
import inspect

from pylustre import utils

LOG_DEBUG_FNAME = "debug.log"
LOG_INFO_FNAME = "info.log"
LOG_WARNING_FNAME = "warning.log"
LOG_ERROR_FNAME = "error.log"

#
# _CLOG_SRC_FILE is used when walking the stack to check when we've got the
# first caller stack frame.
#
if __file__[-4:].lower() in ['.pyc', '.pyo']:
    _CLOG_SRC_FILE = __file__[:-4] + '.py'
else:
    _CLOG_SRC_FILE = __file__
_CLOG_SRC_FILE = os.path.normcase(_CLOG_SRC_FILE)


# pylint: disable=pointless-statement
try:
    unicode
    _UNICODE = True
except NameError:
    _UNICODE = False


def find_caller(src_file):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    frame = inspect.currentframe()
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if frame is not None:
        frame = frame.f_back
    ret = "(unknown file)", 0, "(unknown function)"
    while hasattr(frame, "f_code"):
        code_object = frame.f_code
        filename = os.path.normcase(code_object.co_filename)
        # src_file is always absolute path, but the filename might not be
        # absolute path, e.g. pyclownfish/xxx
        if filename.startswith("/"):
            if filename == src_file:
                frame = frame.f_back
                continue
        else:
            if src_file.endswith(filename):
                frame = frame.f_back
                continue
        ret = (code_object.co_filename, frame.f_lineno, code_object.co_name)
        break
    return ret


class CommandLogs(object):
    """
    Global log object to track what logs have been allocated
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        self.cls_condition = threading.Condition()
        self.cls_logs = {}
        self.cls_root_log = None

    def cls_log_add_or_get(self, log):
        """
        Add a new log
        """
        name = log.cl_name
        old_log = None
        self.cls_condition.acquire()
        if name is None:
            if self.cls_root_log is not None:
                old_log = self.cls_root_log
                self.cls_condition.release()
                return old_log
            self.cls_root_log = log
        else:
            if name in self.cls_logs:
                old_log = self.cls_logs[name]
                self.cls_condition.release()
                return old_log
            self.cls_logs[name] = log
        self.cls_condition.release()
        return log

    def cls_log_fini(self, log):
        """
        Cleanup a log
        """
        name = log.cl_name
        self.cls_condition.acquire()
        if self.cls_root_log is log:
            self.cls_root_log = None
        elif name in self.cls_logs:
            del self.cls_logs[name]
        else:
            log.cl_warning("log [%s] doesn't exist when cleaning up, ignoring",
                           name)
        self.cls_condition.release()


GLOBAL_LOGS = CommandLogs()


def get_message(msg, args):
    """
    Return the message.

    Please check LogRecord.getMessage of logging for more info
    """
    if not _UNICODE:  # if no unicode support...
        msg = str(msg)
    else:
        if not isinstance(msg, basestring):
            try:
                msg = str(msg)
            except UnicodeError:
                msg = msg      # Defer encoding till later
    if args:
        msg = msg % args
    return msg


class ClogRecord(object):
    """
    The log record
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, record, is_stdout=False, is_stderr=False):
        self.clr_record = record
        self.clr_is_stdout = is_stdout
        self.clr_is_stderr = is_stderr


class CommandLog(object):
    """
    Log the ouput of a command
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, name=None, resultsdir=None, simple_console=False,
                 record_consumer=False):
        self.cl_name = name
        self.cl_result = utils.CommandResult()
        # Whether the command is about to abort
        self.cl_abort = False
        self.cl_resultsdir = resultsdir
        self.cl_simple_console = simple_console
        self.cl_logger = None
        self.cl_records = []
        self.cl_condition = threading.Condition()
        # Whether there is any consumer of the record
        # If no consumer, then the stdout and stderror will be saved into
        # cl_result
        self.cl_record_consumer = record_consumer
        self.cl_debug_handler = None
        self.cl_info_handler = None
        self.cl_warning_handler = None
        self.cl_error_handler = None
        self.cl_console_handler = None

    def cl_set_propaget(self):
        """
        Whether log events to this logger to higher level loggers
        """
        self.cl_logger.propagate = True

    def cl_clear_propaget(self):
        """
        Whether log events to this logger to higher level loggers
        """
        self.cl_logger.propagate = False

    def cl_get_child(self, name, resultsdir=None, simple_console=False,
                     exclusive=True, record_consumer=False):
        """
        Get a child log
        If exclusive, the name should not be used for twice
        """
        # pylint: disable=too-many-arguments
        if self.cl_name is not None:
            name = self.cl_name + "." + name
        return get_log(name, resultsdir=resultsdir,
                       simple_console=simple_console,
                       exclusive=exclusive,
                       record_consumer=record_consumer)

    def cl_config(self):
        """
        Config the log
        """
        resultsdir = self.cl_resultsdir
        name = self.cl_name
        simple_console = self.cl_simple_console

        default_formatter = logging.Formatter("[%(asctime)s] [%(name)s] "
                                              "[%(levelname)s] "
                                              "[%(filename)s:%(lineno)s] "
                                              "%(message)s",
                                              "%Y/%m/%d-%H:%M:%S")

        if resultsdir is not None:
            fpath = resultsdir + "/" + LOG_DEBUG_FNAME
            debug_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                 maxBytes=10485760,
                                                                 backupCount=10)
            debug_handler.setLevel(logging.DEBUG)
            debug_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_INFO_FNAME
            info_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                maxBytes=10485760,
                                                                backupCount=10)
            info_handler.setLevel(logging.INFO)
            info_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_WARNING_FNAME
            warning_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                   maxBytes=10485760,
                                                                   backupCount=10)
            warning_handler.setLevel(logging.WARNING)
            warning_handler.setFormatter(default_formatter)

            fpath = resultsdir + "/" + LOG_ERROR_FNAME
            error_handler = logging.handlers.RotatingFileHandler(fpath,
                                                                 maxBytes=10485760,
                                                                 backupCount=10)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(default_formatter)

        if name is None:
            logger = logging.getLogger()
        else:
            logger = logging.getLogger(name=name)

        logger.handlers = []
        logger.setLevel(logging.DEBUG)

        if name is None:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            if not simple_console:
                console_handler.setFormatter(default_formatter)
            logger.addHandler(console_handler)
            self.cl_console_handler = console_handler

        if resultsdir is not None:
            logger.addHandler(debug_handler)
            logger.addHandler(info_handler)
            logger.addHandler(warning_handler)
            logger.addHandler(error_handler)
            self.cl_debug_handler = debug_handler
            self.cl_info_handler = info_handler
            self.cl_warning_handler = warning_handler
            self.cl_error_handler = error_handler
        self.cl_logger = logger

    def cl_change_config(self, simple_console=False, resultsdir=None):
        """
        Change the config of the log
        """
        self.cl_resultsdir = resultsdir
        self.cl_simple_console = simple_console
        self.cl_config()

    def cl_emit(self, name, level, filename, lineno, func, message,
                created_time=None, is_stdout=False, is_stderr=False):
        """
        Emit a log
        """
        # pylint: disable=too-many-arguments
        record_args = None
        exc_info = None
        extra = None
        record = self.cl_logger.makeRecord(name, level, filename, lineno,
                                           message, record_args, exc_info,
                                           func, extra)
        if created_time is not None:
            # Fields like relativeCreated, msecs should be updated ideally. But
            # since they are not used, it doesn't matter
            record.created = created_time
        self.cl_logger.handle(record)

        if self.cl_record_consumer:
            log_record = ClogRecord(record, is_stdout=is_stdout,
                                    is_stderr=is_stderr)
            self.cl_condition.acquire()
            self.cl_records.append(log_record)
            self.cl_condition.release()
        elif is_stdout:
            self.cl_result.cr_stdout += message + "\n"
        elif is_stderr:
            self.cl_result.cr_stderr += message + "\n"

    def _cl_log(self, is_stdout, is_stderr, level, msg, *args):
        """
        Save the log
        """
        message = get_message(msg, args)

        try:
            filename, lineno, func = find_caller(_CLOG_SRC_FILE)
        except ValueError:
            filename, lineno, func = "(unknown file)", 0, "(unknown function)"

        name = self.cl_name
        self.cl_emit(name, level, filename, lineno, func, message,
                     is_stdout=is_stdout, is_stderr=is_stderr)

    def cl_consume(self):
        """
        Consume the log record
        """
        self.cl_condition.acquire()
        records = self.cl_records
        self.cl_records = []
        self.cl_condition.release()
        return records

    def cl_debug(self, msg, *args):
        """
        Print the log to debug log, but not stdout/stderr
        """
        self._cl_log(False, False, logging.DEBUG, msg, *args)

    def cl_info(self, msg, *args):
        """
        Print the log to info log, but not stdout/stderr
        """
        self._cl_log(False, False, logging.INFO, msg, *args)

    def cl_warning(self, msg, *args):
        """
        Print the log to warning log, but not stdout/stderr
        """
        self._cl_log(False, False, logging.WARNING, msg, *args)

    def cl_error(self, msg, *args):
        """
        Print the log to error log, but not stdout/stderr
        """
        self._cl_log(False, False, logging.ERROR, msg, *args)

    def cl_stdout(self, msg, *args):
        """
        Print the log to stdout
        """
        self._cl_log(True, False, logging.INFO, msg, *args)

    def cl_stderr(self, msg, *args):
        """
        Print the log to stdout
        """
        self._cl_log(False, True, logging.ERROR, msg, *args)

    def cl_fini(self):
        """
        Cleanup this log
        """
        return fini_log(self)


def get_log(name=None, resultsdir=None, simple_console=False,
            exclusive=True, record_consumer=False):
    """
    Get the log class
    If exclusive, the name should not be used for twice
    """
    log = CommandLog(name=name, resultsdir=resultsdir,
                     simple_console=simple_console,
                     record_consumer=record_consumer)
    old_log = GLOBAL_LOGS.cls_log_add_or_get(log)
    if old_log is log:
        # Newly added, config it
        old_log.cl_config()
    else:
        if exclusive:
            reason = ("log with name [%s] already exists" % name)
            raise Exception(reason)
        # If the config is not the same, config it
        if (old_log.cl_resultsdir != resultsdir or
                old_log.cl_simple_console != simple_console):
            old_log.cl_change_config(simple_console=simple_console,
                                     resultsdir=resultsdir)
    return old_log


def fini_log(log):
    """
    Cleanup the log so the name can be re-used again
    """
    return GLOBAL_LOGS.cls_log_fini(log)
