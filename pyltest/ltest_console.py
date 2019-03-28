# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Console that manages the scheduler
"""
import xmlrpclib
import readline
import logging
import getopt
import sys
import traceback

# local libs
from pylustre import clog
from pylustre import utils
from pylustre import time_util
from pyltest import ltest_scheduler


LTEST_CONSOLE_LOG_DIR = "/var/log/ltest_console"


class TestConsoleCompleter(object):
    """
    Completer of command
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, options):
        self.ltcc_options = options
        self.ltcc_current_candidates = []
        return

    def ltcc_complete(self, text, state):
        # pylint: disable=unused-argument,too-many-nested-blocks
        """
        The complete function of the completer
        """
        response = None
        if state == 0:
            # This is the first time for this text,
            # so build a match list.
            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]
            words = origline.split()
            if not words:
                self.ltcc_current_candidates = sorted(self.ltcc_options.keys())
            else:
                try:
                    if begin == 0:
                        # first word
                        candidates = self.ltcc_options.keys()
                    else:
                        # later word
                        first = words[0]
                        candidates = self.ltcc_options[first]
                    if being_completed:
                        # match options with portion of input
                        # being completed
                        self.ltcc_current_candidates = []
                        for candidate in candidates:
                            if not candidate.startswith(being_completed):
                                continue
                            self.ltcc_current_candidates.append(candidate)
                    else:
                        # matching empty string so use all candidates
                        self.ltcc_current_candidates = candidates
                except (KeyError, IndexError):
                    self.ltcc_current_candidates = []
        try:
            response = self.ltcc_current_candidates[state]
        except IndexError:
            response = None
        return response


def tconsole_input_init():
    """
    Initialize the input completer
    """
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set editing-mode vi")
    # Register our completer function
    completer = TestConsoleCompleter({"help": [],
                                      "host_cleanup": [],
                                      "host_list": [],
                                      "ip_list": [],
                                      "ip_cleanup": [],
                                      "job_list": [],
                                      "job_kill": []})
    readline.set_completer(completer.ltcc_complete)


def tconsole_input_fini():
    """
    Stop the input completer
    """
    readline.set_completer(None)


def tconsole_command_help(proxy, arg_string):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    logging.info("help: show help messages")
    return 0


def tconsole_command_host_list(proxy, arg_string):
    # pylint: disable=unused-variable
    """
    List the hosts
    """
    error = False
    args = arg_string.split()
    options, remainder = getopt.getopt(args,
                                       "he",
                                       ["--error",
                                        "--help"])
    for opt, arg in options:
        if opt in ("-e", "--error"):
            error = True
        elif opt in ("-h", "--help"):
            print """Usage: host_list [-e|--error]
    -e: print hosts that have cleanup error
    -h: print this string"""
            sys.exit(0)

    output = proxy.ts_host_list(error)
    print "%s" % output
    return 0


def tconsole_command_ip_list(proxy, arg_string):
    # pylint: disable=unused-variable
    """
    List the IP addreses
    """
    error = False
    args = arg_string.split()
    options, remainder = getopt.getopt(args,
                                       "he",
                                       ["--error",
                                        "--help"])
    for opt, arg in options:
        if opt in ("-e", "--error"):
            error = True
        elif opt in ("-h", "--help"):
            print """Usage: host_list [-e|--error]
    -e: print hosts that have cleanup error
    -h: print this string"""
            sys.exit(0)

    output = proxy.ts_ip_address_list(error)
    print "%s" % output
    return 0


def tconsole_command_job_list(proxy, arg_string):
    """
    List all the active jobs on the scheduler
    """
    # pylint: disable=unused-argument
    jobs = proxy.ts_job_list()
    print "%s" % jobs
    return 0


def tconsole_command_job_kill(proxy, arg_string):
    """
    Kill a job
    """
    jobid = arg_string
    scheduler_id = proxy.ts_get_id()
    ret = proxy.ts_job_stop(scheduler_id, jobid)
    return ret


def tconsole_command_host_cleanup(proxy, arg_string):
    """
    Fix the host
    """
    arg_string = arg_string.strip()
    args = arg_string.split()
    if len(args) == 1:
        hostname = args[0]
    else:
        logging.error("""Usage: host_cleanup <hostname>""")
        return -1
    ret = proxy.ts_host_cleanup(hostname)
    return ret


def tconsole_command_ip_cleanup(proxy, arg_string):
    """
    Cleanup the IP address
    """
    arg_string = arg_string.strip()
    args = arg_string.split()
    if len(args) == 1:
        ip_address = args[0]
    else:
        logging.error("""Usage: ip_cleanup <ip_address>""")
        return -1
    ret = proxy.ts_ip_cleanup(ip_address)
    return ret


def tconsole_command(proxy, line):
    """
    Run a command in the console
    """
    # pylint: disable=broad-except
    functions = {"help": tconsole_command_help,
                 "host_cleanup": tconsole_command_host_cleanup,
                 "host_list": tconsole_command_host_list,
                 "ip_list": tconsole_command_ip_list,
                 "ip_cleanup": tconsole_command_ip_cleanup,
                 "job_list": tconsole_command_job_list,
                 "job_kill": tconsole_command_job_kill}
    if " " in line:
        command, arg_string = line.split(' ', 1)
    else:
        command = line
        arg_string = ""

    try:
        func = functions[command]
    except (KeyError, IndexError), err:
        func = None

    # Run system command
    if func is not None:
        try:
            ret = func(proxy, arg_string)
        except Exception, err:
            logging.error("failed to run command [%s %s] %s, %s",
                          command, arg_string, err,
                          traceback.format_exc())
            return -1
    else:
        logging.error("no command: %s\n", line)
        ret = -1
    return ret


def tconsole_input_loop(proxy):
    """
    Loop and excute the command
    """
    while True:
        line = raw_input('> ("q" to quit): ')
        if line == 'q' or line == 'quit':
            break
        tconsole_command(proxy, line)


def usage():
    """
    Print the usage of the command
    """
    command = sys.argv[0]
    utils.eprint("Usage: %s <server>\n"
                 "    server: the server address\n"
                 "\n"
                 "examples:\n"
                 "%s    --> use http://localhost:1234 as the server address\n"
                 "%s -s localhost\n"
                 "%s -s localhost\n"
                 "%s -s http://localhost:1234\n"
                 "%s -s http://10.0.0.10:1234"
                 % (command, command, command, command, command, command))


def main():
    """
    Run the console
    """
    # pylint: disable=unused-variable
    now = time_util.utcnow()
    workspace = (LTEST_CONSOLE_LOG_DIR + "/" +
                 time_util.local_strftime(now, ('%Y-%m-%d-%H:%M:%S')))
    ret = utils.run("mkdir -p %s" % workspace)
    if ret.cr_exit_status != 0:
        logging.error("failed to create directory [%s]", workspace)
        sys.exit(1)

    log = clog.get_log(resultsdir=workspace)

    argc = len(sys.argv)
    if argc == 1:
        server = "http://localhost:1234"
    elif argc == 2:
        arg = sys.argv[1]
        if arg == "-h" or arg == "--help":
            usage()
            sys.exit(0)
        server = arg
        if not server.startswith("http://"):
            server = "http://" + server
        if server.count(":") != 2:
            server = server + ":" + str(ltest_scheduler.TEST_SCHEDULER_PORT)

    log.cl_info("connecting to server [%s]", server)
    proxy = xmlrpclib.ServerProxy(server, allow_none=True)

    tconsole_input_init()
    tconsole_input_loop(proxy)
    tconsole_input_fini()
