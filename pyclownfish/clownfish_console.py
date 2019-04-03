# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Deamon Library for clownfish
Clownfish is an automatic management system for Lustre
"""
import sys
import os
import time
import threading
import readline
import zmq

# Local libs
from pylcommon import utils
from pylcommon import clog
from pylcommon import time_util
from pylcommon import constants
from pyclownfish import clownfish_pb2
from pyclownfish import clownfish

CLOWNFISH_CONSOLE_QUERY_INTERVAL = 1
CLOWNFISH_CONSOLE_PING_INTERVAL = 1
CLOWNFISH_CONSOLE_PING_TIMEOUT = 10
CLOWNFISH_CONSOLE_POLL_TIMEOUT = 1
CLOWNFISH_CONSOLE_TIMEOUT = 10
CLOWNFISH_CONSOLE_CONNECT_TIMEOUT = 10

CLOWNFISH_CONSOLE_LOG_DIR = "/var/log/clownfish_console"


class ClownfishConsoleCommand(object):
    """
    Config command
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, command, arguments, need_child):
        self.ccc_command = command
        self.ccc_arguments = arguments
        self.ccc_need_child = need_child


class ClownfishConsoleMessage(object):
    """
    Each message has a object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, client_uuid, request_type, reply_type):
        self.ccm_request = clownfish_pb2.ClownfishMessage()
        self.ccm_request.cm_protocol_version = clownfish_pb2.ClownfishMessage.CPV_ZERO
        self.ccm_request.cm_client_uuid = client_uuid
        self.ccm_request.cm_type = request_type
        self.ccm_request.cm_errno = clownfish_pb2.ClownfishMessage.CE_NO_ERROR
        self.ccm_reply = clownfish_pb2.ClownfishMessage()
        self.ccm_reply_type = reply_type

    def ccm_communicate(self, log, poll, socket_client, timeout):
        """
        Send the request and wait for the reply
        """
        # pylint: disable=too-many-return-statements
        # If communicate failed because of un-recoverable error, return
        # negative value. If times out, return 1.
        log.cl_debug("communicating to server")
        request_string = self.ccm_request.SerializeToString()
        socket_client.send(request_string)
        received = False
        time_start = time.time()
        log.cl_debug("sent request to server")
        while not received:
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed >= timeout:
                log.cl_error("timeout after waiting for [%d] seconds when "
                             "communcating to server", timeout)
                return -1

            events = dict(poll.poll(CLOWNFISH_CONSOLE_POLL_TIMEOUT * 1000))
            for socket, event in events.iteritems():
                if socket != socket_client:
                    log.cl_error("found a event which doesn't belong to this "
                                 "socket, ignoring")
                    continue
                if event == zmq.POLLIN:
                    log.cl_debug("received the reply successfully")
                    received = True

        log.cl_debug("received the reply from server")
        reply_string = socket_client.recv()
        if not reply_string:
            log.cl_error("got POLLIN event, but no message received")
            return -1

        log.cl_debug("parsing the reply from server")
        self.ccm_reply.ParseFromString(reply_string)
        if (self.ccm_reply.cm_protocol_version !=
                clownfish_pb2.ClownfishMessage.CPV_ZERO):
            log.cl_error("wrong reply protocol version [%d], expected [%d]",
                         self.ccm_reply.cm_protocol_version,
                         clownfish_pb2.ClownfishMessage.CPV_ZERO)
            return -1

        if self.ccm_reply.cm_type != self.ccm_reply_type:
            log.cl_error("wrong reply type [%d], expected [%d]",
                         self.ccm_reply.cm_type,
                         self.ccm_reply_type)
            return -1

        if self.ccm_reply.cm_errno != clownfish_pb2.ClownfishMessage.CE_NO_ERROR:
            log.cl_error("server side error [%d]", self.ccm_reply.cm_errno)
            return -1

        if self.ccm_reply.cm_type == clownfish_pb2.ClownfishMessage.CMT_CONNECT_REPLY:
            log.cl_debug("got connect reply from server")
            return 0

        if (self.ccm_reply.cm_client_uuid !=
                self.ccm_request.cm_client_uuid):
            log.cl_error("wrong client UUID [%d] in reply, expected [%d]",
                         self.ccm_reply.cm_client_uuid,
                         self.ccm_request.cm_client_uuid)
            return -1
        log.cl_debug("communicated successfully with server")
        return 0


class ClownfishClient(object):
    """
    Client of Clownfish server
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, log, workspace, server_url):
        self.cc_workspace = workspace
        self.cc_context = zmq.Context(1)
        self.cc_poll = zmq.Poller()
        self.cc_server_url = server_url
        self.cc_candidates = []
        self.cc_cstr_candidates = []
        self.cc_client = self.cc_context.socket(zmq.REQ)
        self.cc_client.connect(self.cc_server_url)
        self.cc_poll.register(self.cc_client, zmq.POLLIN)
        self.cc_running = True
        self.cc_uuid = None
        self.cc_log = log
        self.cc_abort_event = threading.Event()
        # used to notify the stop running
        self.cc_condition = threading.Condition()

    def cc_ping_thread(self):
        """
        Ping the server constantly
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        name = "thread_ping"
        thread_workspace = self.cc_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.cc_log.cl_error("failed to create directory [%s]",
                                     thread_workspace)
                sys.exit(-1)
        elif not os.path.isdir(thread_workspace):
            self.cc_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.cc_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_debug("starting ping thread")
        server_url = self.cc_server_url
        ret = 0
        time_start = time.time()
        poll = zmq.Poller()
        context = zmq.Context(1)
        client = None
        while self.cc_running and ret == 0:
            if client is not None:
                client.setsockopt(zmq.LINGER, 0)
                client.close()
                poll.unregister(client)
            client = context.socket(zmq.REQ)
            client.connect(server_url)
            poll.register(client, zmq.POLLIN)
            while self.cc_running and ret == 0:
                time_now = time.time()
                elapsed = time_now - time_start
                if elapsed >= CLOWNFISH_CONSOLE_PING_TIMEOUT:
                    log.cl_error("timeout when pinging [%s]", server_url)
                    ret = -1
                    break

                message = ClownfishConsoleMessage(self.cc_uuid,
                                                  clownfish_pb2.ClownfishMessage.CMT_PING_REQUEST,
                                                  clownfish_pb2.ClownfishMessage.CMT_PING_REPLY)
                log.cl_debug("pinging [%s]", server_url)
                ret = message.ccm_communicate(log, poll, client,
                                              CLOWNFISH_CONSOLE_PING_TIMEOUT)
                if ret < 0:
                    log.cl_error("failed to ping server")
                    break
                elif ret > 0:
                    log.cl_debug("no response from server, retrying")
                    ret = 0
                    continue

                log.cl_debug("server replied successfully")
                time_start = time.time()
                self.cc_condition.acquire()
                self.cc_condition.wait(CLOWNFISH_CONSOLE_PING_INTERVAL)
                self.cc_condition.release()
        if client is not None:
            client.setsockopt(zmq.LINGER, 0)
            client.close()
        if ret:
            log.cl_debug("ping thread stoped because of connection error")
            self.cc_running = False
        else:
            assert not self.cc_running
            log.cl_debug("ping thread stoped because the console is exiting")

        log.cl_debug("terminating ZMQ context of pinging thread")
        context.term()
        log.cl_debug("terminated ZMQ context of pinging thread")
        return ret

    def cc_children(self):
        """
        Return the children under this directory
        """
        log = self.cc_log

        children = []
        request_type = clownfish_pb2.ClownfishMessage.CMT_COMMAND_CHILDREN_REQUEST
        reply_type = clownfish_pb2.ClownfishMessage.CMT_COMMAND_CHILDREN_REPLY
        message = ClownfishConsoleMessage(self.cc_uuid, request_type,
                                          reply_type)
        log.cl_debug("getting the command children from server")
        ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                      CLOWNFISH_CONSOLE_TIMEOUT)
        if ret:
            log.cl_error("failed to get command children from server")
            return children

        for name in message.ccm_reply.cm_command_children_reply.cccry_children:
            escaped_name = clownfish.clownfish_entry_escape(name)
            children.append(escaped_name)
        return children

    def cc_command_dict(self):
        """
        Return the command dict with type ClownfishConsoleCommand
        """
        log = self.cc_log

        command_dict = {}
        message = ClownfishConsoleMessage(self.cc_uuid,
                                          clownfish_pb2.ClownfishMessage.CMT_COMMAND_DICT_REQUEST,
                                          clownfish_pb2.ClownfishMessage.CMT_COMMAND_DICT_REPLY)
        log.cl_debug("getting the command dictionary from server")
        ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                      CLOWNFISH_CONSOLE_TIMEOUT)
        if ret:
            log.cl_error("failed to get command dictionary from server")
            return command_dict

        for item in message.ccm_reply.cm_command_dict_reply.ccdry_items:
            command = item.cci_command
            need_child = item.cci_need_child
            arguments = []
            for argument in item.cci_arguments:
                arguments.append(argument)
            ccommand = ClownfishConsoleCommand(command, arguments, need_child)
            command_dict[command] = ccommand

        return command_dict

    def cc_completer(self, text, state):
        # pylint: disable=too-many-branches,unused-argument
        # pylint: disable=too-many-nested-blocks
        """
        The complete function of the input completer
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
                self.cc_candidates = sorted(self.cc_command_dict().keys())
            else:
                try:
                    if begin == 0:
                        # first word
                        candidates = self.cc_command_dict().keys()
                    else:
                        # later word
                        first = words[0]
                        config_command = self.cc_command_dict()[first]
                        candidates = list(config_command.ccc_arguments)

                        if config_command.ccc_need_child:
                            subdirs = self.cc_children()
                            if subdirs is not None:
                                candidates += subdirs

                    if being_completed:
                        # match options with portion of input
                        # being completed
                        self.cc_candidates = []
                        for candidate in candidates:
                            if not candidate.startswith(being_completed):
                                continue
                            self.cc_candidates.append(candidate)
                    else:
                        # matching empty string so use all candidates
                        self.cc_candidates = candidates
                except (KeyError, IndexError):
                    self.cc_candidates = []
        try:
            response = self.cc_candidates[state]
        except IndexError:
            response = None
        return response

    def cc_pwd(self):
        """
        Return the PWD
        """
        log = self.cc_log

        message = ClownfishConsoleMessage(self.cc_uuid,
                                          clownfish_pb2.ClownfishMessage.CMT_PWD_REQUEST,
                                          clownfish_pb2.ClownfishMessage.CMT_PWD_REPLY)
        log.cl_debug("getting the pwd from server")
        ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                      CLOWNFISH_CONSOLE_TIMEOUT)
        if ret:
            log.cl_error("failed to get pwd from server")
            return "UNKOWN"

        return message.ccm_reply.cm_pwd_reply.cpry_pwd

    def cc_command(self, log, cmd_line):
        """
        Run a command in the console
        """
        abort_event = self.cc_abort_event
        log.cl_result.cr_clear()
        # The abort flag is set by server
        log.cl_abort = False
        message = ClownfishConsoleMessage(self.cc_uuid,
                                          clownfish_pb2.ClownfishMessage.CMT_COMMAND_REQUEST,
                                          clownfish_pb2.ClownfishMessage.CMT_COMMAND_REPLY)
        log.cl_debug("running the command [%s] on server", cmd_line)
        message.ccm_request.cm_command_request.ccrt_cmd_line = cmd_line
        ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                      CLOWNFISH_CONSOLE_TIMEOUT)
        if ret:
            log.cl_stderr("failed to run command [%s] on server", cmd_line)
            log.cl_result.cr_exit_status = -1
            return

        while True:
            command_reply = message.ccm_reply.cm_command_reply
            for record in command_reply.ccry_logs:
                log.cl_emit(record.clr_name, record.clr_levelno,
                            record.clr_pathname, record.clr_lineno,
                            record.clr_funcname, record.clr_msg,
                            created_time=record.clr_created_time,
                            is_stdout=record.clr_is_stdout,
                            is_stderr=record.clr_is_stderr)
            if command_reply.ccry_is_final:
                final = command_reply.ccry_final
                ret = final.ccfr_exit_status
                log.cl_abort = final.ccfr_quit
                log.cl_debug("ran command [%s], ret = [%d], quiting = [%s]",
                             cmd_line,
                             final.ccfr_exit_status,
                             final.ccfr_quit)
                break
            else:
                clownfish_message = clownfish_pb2.ClownfishMessage
                request_type = clownfish_message.CMT_COMMAND_PARTWAY_QUERY
                reply_type = clownfish_message.CMT_COMMAND_REPLY
                message = ClownfishConsoleMessage(self.cc_uuid, request_type,
                                                  reply_type)
                query = message.ccm_request.cm_command_partway_query
                query.ccpq_abort = abort_event.is_set()
                log.cl_debug("partway querying of the command [%s] on server", cmd_line)
                ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                              CLOWNFISH_CONSOLE_TIMEOUT)
                if ret:
                    log.cl_stderr("failed to query command [%s] on server", cmd_line)
                    break
                time.sleep(CLOWNFISH_CONSOLE_QUERY_INTERVAL)
        log.cl_result.cr_exit_status = ret
        return

    def cc_loop(self, cmdline=None):
        """
        Loop and execute the command
        """
        # pylint: disable=unused-variable
        log = self.cc_log

        readline.parse_and_bind("tab: complete")
        readline.parse_and_bind("set editing-mode vi")
        # This enables completer of options with prefix "-" or "--"
        # becase "-" is one of the delimiters by default
        readline.set_completer_delims(" \t\n")
        readline.set_completer(self.cc_completer)
        while self.cc_running:
            if cmdline is None:
                try:
                    prompt = '[%s]$ (h for help): ' % self.cc_pwd()
                    log.cl_debug(prompt)
                    cmd_line = raw_input(prompt)
                except (KeyboardInterrupt, EOFError):
                    log.cl_debug("keryboard interrupt recieved")
                    log.cl_info("")
                    log.cl_info("Type q to exit")
                    continue
                log.cl_debug("input: %s", cmd_line)
                cmd_line = cmd_line.strip()
                if len(cmd_line) == 0:
                    continue
            else:
                cmd_line = cmdline

            self.cc_abort_event.clear()
            command_thread = utils.thread_start(self.cc_command,
                                                (log, cmd_line))
            while command_thread.is_alive():
                try:
                    command_thread.join(CLOWNFISH_CONSOLE_QUERY_INTERVAL)
                except (KeyboardInterrupt, EOFError):
                    log.cl_debug("keryboard interrupt recieved")
                    log.cl_stderr("aborting command [%s]", cmd_line)
                    self.cc_abort_event.set()
                    continue

            # The server told us to quit the connection
            if log.cl_abort:
                break
            # not interactive mode
            if cmdline is not None:
                break

        readline.set_completer(None)

    def cc_fini(self):
        """
        Finish the connection to the server
        """
        log = self.cc_log

        self.cc_running = False
        self.cc_condition.acquire()
        self.cc_condition.notifyAll()
        self.cc_condition.release()
        self.cc_client.setsockopt(zmq.LINGER, 0)
        self.cc_client.close()
        self.cc_poll.unregister(self.cc_client)
        log.cl_debug("terminating ZMQ context")
        self.cc_context.term()
        log.cl_debug("terminated ZMQ context")

    def cc_init(self):
        """
        Init the connection to server
        """
        log = self.cc_log

        client = self.cc_client
        server_url = self.cc_server_url

        sequence = 0

        message = ClownfishConsoleMessage(0,
                                          clownfish_pb2.ClownfishMessage.CMT_CONNECT_REQUEST,
                                          clownfish_pb2.ClownfishMessage.CMT_CONNECT_REPLY)
        message.ccm_request.cm_connect_request.ccrt_client_hash = sequence
        log.cl_debug("connecting to server [%s]", server_url)
        ret = message.ccm_communicate(log, self.cc_poll, client,
                                      CLOWNFISH_CONSOLE_CONNECT_TIMEOUT)
        if ret:
            log.cl_error("failed to connect to server [%s]",
                         server_url)
            return -1
        elif ret == 1:
            log.cl_error("failed to connect to server [%s]: no response",
                         server_url)
            return -1

        if message.ccm_reply.cm_connect_reply.ccry_client_hash != sequence:
            log.cl_error("wrong client hash [%d] in reply, expected [%d]",
                         message.ccm_reply.cm_connect_reply.ccry_client_hash,
                         sequence)
            return -1

        self.cc_uuid = message.ccm_reply.cm_client_uuid
        log.cl_debug("connected to server [%s] successfully, UUID is [%s]",
                     server_url, self.cc_uuid)
        utils.thread_start(self.cc_ping_thread, ())
        return 0


def clownfish_console_loop(log, workspace, server_url, cmdline=None):
    """
    Start to run console
    """
    console_client = ClownfishClient(log, workspace, server_url)
    ret = console_client.cc_init()
    if ret == 0:
        ret = console_client.cc_loop(cmdline=cmdline)
    else:
        log.cl_error("failed to init connection to [%s]", server_url)
    console_client.cc_fini()
    return ret


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s [-P port] host [command]" %
                 sys.argv[0])
    utils.eprint("Examples:")
    utils.eprint("%s localhost" % sys.argv[0])
    utils.eprint("%s -P %s localhost" %
                 (sys.argv[0], constants.CLOWNFISH_DEFAULT_SERVER_PORT))
    utils.eprint("%s -P %s 192.168.1.2" %
                 (sys.argv[0], constants.CLOWNFISH_DEFAULT_SERVER_PORT))
    utils.eprint("%s -P %s 192.168.1.2 ls -R" %
                 (sys.argv[0], constants.CLOWNFISH_DEFAULT_SERVER_PORT))


def main():
    """
    Start clownfish
    """
    # pylint: disable=unused-variable,too-many-statements,too-many-branches
    reload(sys)
    sys.setdefaultencoding("utf-8")

    argc = len(sys.argv)
    if argc < 2:
        utils.eprint("too few arguments")
        usage()
        sys.exit(-1)
    elif argc == 2:
        # clownfish_console host
        # clownfish_console -h
        # clownfish_console --help
        if sys.argv[1] == "-h" or sys.argv[1] == "--help":
            usage()
            sys.exit(0)
        server_url = ("tcp://%s:%s" %
                      (sys.argv[1], constants.CLOWNFISH_DEFAULT_SERVER_PORT))
        cmdline = None
    elif argc == 3:
        # clownfish_console host cmdline
        server_url = ("tcp://%s:%s" %
                      (sys.argv[1], constants.CLOWNFISH_DEFAULT_SERVER_PORT))
        cmdline = sys.argv[2]
    elif argc == 4:
        # clownfish_console -P 3002 host
        # clownfish_console host cmdline1 cmdline2
        if sys.argv[1] == "-P":
            port_string = sys.argv[2]
            host = sys.argv[3]
            cmdline = None
        else:
            host = sys.argv[1]
            port_string = str(constants.CLOWNFISH_DEFAULT_SERVER_PORT)
            cmdline = sys.argv[2] + " " + sys.argv[3]
        server_url = "tcp://%s:%s" % (host, port_string)
    elif argc >= 5:
        # clownfish_console -P 3002 host cmdline...
        # clownfish_console host cmdline1 cmdline2 cmdline3...
        if sys.argv[1] == "-P":
            port_string = sys.argv[2]
            host = sys.argv[3]
            cmdline_start = 4
        else:
            host = sys.argv[1]
            port_string = str(constants.CLOWNFISH_DEFAULT_SERVER_PORT)
            cmdline_start = 2
        cmdline = ""
        for arg_index in range(cmdline_start, argc):
            if cmdline != "":
                cmdline += " "
            cmdline += sys.argv[arg_index]
        server_url = "tcp://%s:%s" % (host, port_string)

    identity = time_util.local_strftime(time_util.utcnow(), "%Y-%m-%d-%H_%M_%S")
    workspace = CLOWNFISH_CONSOLE_LOG_DIR + "/" + identity

    if not os.path.exists(CLOWNFISH_CONSOLE_LOG_DIR):
        ret = utils.mkdir(CLOWNFISH_CONSOLE_LOG_DIR)
        if ret:
            sys.stderr.write("failed to create directory [%s]" % CLOWNFISH_CONSOLE_LOG_DIR)
            sys.exit(-1)
    elif not os.path.isdir(CLOWNFISH_CONSOLE_LOG_DIR):
        sys.stderr.write("[%s] is not a directory" % CLOWNFISH_CONSOLE_LOG_DIR)
        sys.exit(-1)

    if not os.path.exists(workspace):
        ret = utils.mkdir(workspace)
        if ret:
            sys.stderr.write("failed to create directory [%s]" % workspace)
            sys.exit(-1)
    elif not os.path.isdir(workspace):
        sys.stderr.write("[%s] is not a directory" % workspace)
        sys.exit(-1)

    if cmdline is None:
        print("Starting Clownfish console to server [%s], "
              "please check [%s] for more log" %
              (server_url, workspace))

    log = clog.get_log(resultsdir=workspace, simple_console=True)

    ret = clownfish_console_loop(log, workspace, server_url, cmdline=cmdline)
    if ret:
        log.cl_error("Clownfish console exited with failure, please check [%s] for "
                     "more log\n", workspace)
        sys.exit(ret)
    if cmdline is None:
        log.cl_info("Clownfish console exited, please check [%s] for more log",
                    workspace)
    sys.exit(0)
