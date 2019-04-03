# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Server Library for clownfish
Clownfish is an automatic management system for Lustre
"""
import threading
import traceback
import sys
import os
import time
import yaml
import zmq

# Local libs
from pylcommon import utils
from pylcommon import cstr
from pylcommon import cmd_general
from pylcommon import constants
from pyclownfish import clownfish_pb2
from pyclownfish import clownfish

CLOWNFISH_WORKER_NUMBER = 10
CLOWNFISH_CONNECTION_TIMEOUT = 30

CLOWNFISH_SERVER_LOG_DIR = "/var/log/clownfish_server"


def remove_tailing_newline(log, output):
    """
    Prepare the command reply
    """
    if output != "":
        if output[-1] != "\n":
            log.cl_error("unexpected output [%s], no tailing newline is "
                         "found", output)
        else:
            output = output[0:-1]
    return output


class ClownfishConnection(object):
    """
    Each connection from a client has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, parent_log, client_hash, sequence, instance):
        self.cc_client_hash = client_hash
        self.cc_sequence = sequence
        self.cc_atime = time.time()
        self.cc_walk = clownfish.ClownfishWalk(instance)
        self.cc_connection_name = "connection_%s" % sequence
        self.cc_workspace = instance.ci_workspace + "/" + self.cc_connection_name
        ret = utils.mkdir(self.cc_workspace)
        if ret:
            reason = ("failed to create directory [%s] on local host" %
                      (self.cc_workspace))
            parent_log.cl_error(reason)
            raise Exception(reason)
        self.cc_command_log = parent_log.cl_get_child(self.cc_connection_name,
                                                      resultsdir=self.cc_workspace,
                                                      record_consumer=True)
        self.cc_last_retval = None
        self.cc_quit = False
        # used to notify the finish of command
        self.cc_condition = threading.Condition()

    def cc_update_atime(self):
        """
        Whenever the connection has a message, udpate the atime
        """
        self.cc_atime = time.time()

    def cc_cmdline_finish(self):
        """
        notify that the cmdline thread finished
        """
        self.cc_condition.acquire()
        self.cc_condition.notifyAll()
        self.cc_condition.release()

    def cc_cmdline_thread(self, cmdline):
        """
        Thread to run a command line
        """
        # pylint: disable=broad-except,too-many-branches,too-many-statements
        log = self.cc_command_log
        args = cmdline.split()
        argc = len(args)
        if argc == 0:
            log.cl_stderr("empty command line [%s]", cmdline)
            log.cl_result.cr_exit_status = -1
            self.cc_cmdline_finish()
            return

        operation = clownfish.CLOWNFISH_DELIMITER_AND
        retval = 0
        while operation != "":
            if ((operation == clownfish.CLOWNFISH_DELIMITER_AND and retval != 0) or
                    (operation == clownfish.CLOWNFISH_DELIMITER_OR and retval == 0)):
                log.cl_debug("finish cmdline because delimiter [%s] and "
                             "retval [%d]", operation, retval)
                break

            argc = len(args)
            assert argc > 0

            operation = ""
            for argc_index in range(argc):
                arg = args[argc_index]
                if arg == clownfish.CLOWNFISH_DELIMITER_AND:
                    operation = clownfish.CLOWNFISH_DELIMITER_AND
                elif arg == clownfish.CLOWNFISH_DELIMITER_OR:
                    operation = clownfish.CLOWNFISH_DELIMITER_OR
                elif arg == clownfish.CLOWNFISH_DELIMITER_CONT:
                    operation = clownfish.CLOWNFISH_DELIMITER_CONT

                if operation != "":
                    if argc_index == 0:
                        log.cl_stderr("invalid command line [%s]: no command before [%s]",
                                      cmdline, arg)
                        log.cl_result.cr_exit_status = -1
                        self.cc_cmdline_finish()
                        return
                    current_args = args[:argc_index]

                    if argc_index == argc - 1:
                        log.cl_stderr("invalid command line [%s]: tailing [%s]",
                                      cmdline, arg)
                        log.cl_result.cr_exit_status = -1
                        self.cc_cmdline_finish()
                        return
                    args = args[argc_index + 1:]
                    break
            if operation == "":
                current_args = args

            command = current_args[0]
            if command not in clownfish.CLOWNFISH_SERVER_COMMNADS:
                log.cl_stderr('unknown command [%s]', command)
                retval = -1
            else:
                ccommand = clownfish.CLOWNFISH_SERVER_COMMNADS[command]
                try:
                    retval = ccommand.cc_function(self, current_args)
                    log.cl_debug("finished cmdline part %s", current_args)
                except Exception, err:
                    log.cl_stderr("failed to run cmdline part %s, exception: "
                                  "%s, %s",
                                  current_args, err, traceback.format_exc())
                    retval = -1
                    break

        log.cl_result.cr_exit_status = retval
        self.cc_cmdline_finish()

    def cc_abort(self):
        """
        Set the abort flag of the log
        """
        self.cc_command_log.cl_abort = True

    def cc_consume_command_log(self, thread_log, command_reply):
        """
        Get the log of the command
        """
        thread_log.cl_debug("consuming log of connection [%s]",
                            self.cc_connection_name)
        log = self.cc_command_log
        if log.cl_result.cr_exit_status is None:
            command_reply.ccry_is_final = False
        else:
            command_reply.ccry_is_final = True
            command_reply.ccry_final.ccfr_exit_status = log.cl_result.cr_exit_status
            command_reply.ccry_final.ccfr_quit = self.cc_quit
        records = command_reply.ccry_logs
        for clog_record in log.cl_consume():
            record = records.add()
            log_record = clog_record.clr_record
            record.clr_is_stdout = clog_record.clr_is_stdout
            record.clr_is_stderr = clog_record.clr_is_stderr
            record.clr_name = log_record.name
            record.clr_levelno = log_record.levelno
            record.clr_pathname = log_record.pathname
            record.clr_lineno = log_record.lineno
            record.clr_funcname = log_record.funcName
            record.clr_created_time = log_record.created
            record.clr_msg = log_record.msg

    def cc_command(self, thread_log, cmd_line, command_reply):
        """
        Run command for a connection
        """
        # pylint: disable=broad-except
        thread_log.cl_info("running command [%s]", cmd_line)
        log = self.cc_command_log
        self.cc_last_retval = log.cl_result.cr_exit_status
        log.cl_result.cr_clear()
        log.cl_abort = False

        utils.thread_start(self.cc_cmdline_thread, (cmd_line, ))
        # Wait a little bit for the command that can finish quickly
        self.cc_condition.acquire()
        self.cc_condition.wait(clownfish.MAX_FAST_COMMAND_TIME)
        self.cc_condition.release()
        self.cc_consume_command_log(thread_log, command_reply)
        thread_log.cl_info("returned reply of command [%s]", cmd_line)


class ClownfishServer(object):
    """
    This server that listen and handle requests from console
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, log, server_port, instance):
        self.cs_log = log
        self.cs_running = True
        self.cs_instance = instance
        self.cs_url_client = "tcp://*:" + str(server_port)
        self.cs_url_worker = "inproc://workers"
        self.cs_context = zmq.Context.instance()
        self.cs_client_socket = self.cs_context.socket(zmq.ROUTER)
        self.cs_client_socket.bind(self.cs_url_client)
        self.cs_worker_socket = self.cs_context.socket(zmq.DEALER)
        self.cs_worker_socket.bind(self.cs_url_worker)
        # Sequence is protected by cs_condition
        self.cs_sequence = 0
        # The key is the sequence of the connection, protected by cs_condition
        self.cs_connections = {}
        self.cs_condition = threading.Condition()
        for worker_index in range(CLOWNFISH_WORKER_NUMBER):
            log.cl_info("starting worker thread [%d]", worker_index)
            utils.thread_start(self.cs_worker_thread, (worker_index, ))
        utils.thread_start(self.cs_connection_cleanup_thread, ())

    def cs_connection_cleanup_thread(self):
        """
        Cleanup dead thread
        """
        log = self.cs_log
        log.cl_info("starting connection cleanup thread")
        while self.cs_running:
            sleep_time = CLOWNFISH_CONNECTION_TIMEOUT
            now = time.time()
            self.cs_condition.acquire()
            for client_uuid, connection in self.cs_connections.items():
                if (connection.cc_atime + CLOWNFISH_CONNECTION_TIMEOUT <=
                        now):
                    log.cl_info("connection [%s] times out, cleaning it up",
                                client_uuid)
                    del self.cs_connections[client_uuid]
                else:
                    my_sleep_time = (connection.cc_atime +
                                     CLOWNFISH_CONNECTION_TIMEOUT - now)
                    if my_sleep_time < sleep_time:
                        sleep_time = my_sleep_time
            self.cs_condition.release()
            time.sleep(sleep_time)
        log.cl_info("connection cleanup thread exited")

    def cs_connection_allocate(self, client_hash):
        """
        Allocate a new connection
        """
        log = self.cs_log
        self.cs_condition.acquire()
        sequence = self.cs_sequence
        self.cs_sequence += 1
        connection = ClownfishConnection(log, client_hash, sequence, self.cs_instance)
        self.cs_connections[str(sequence)] = connection
        self.cs_condition.release()
        log.cl_debug("allocated uuid [%s] for client with hash [%s]",
                     sequence, client_hash)
        return connection

    def cs_connection_find(self, client_uuid):
        """
        Find the connection from UUID
        """
        log = self.cs_log
        self.cs_condition.acquire()
        sequence_string = str(client_uuid)
        if sequence_string in self.cs_connections:
            connection = self.cs_connections[sequence_string]
        else:
            connection = None
        self.cs_condition.release()
        if connection is None:
            log.cl_debug("can not find client with uuid [%s]",
                         client_uuid)
        else:
            log.cl_debug("found client with uuid [%s]",
                         client_uuid)
            connection.cc_update_atime()
        return connection

    def cs_connection_delete(self, client_uuid):
        """
        Find the connection from UUID
        """
        log = self.cs_log
        sequence_string = str(client_uuid)
        self.cs_condition.acquire()
        if sequence_string in self.cs_connections:
            del self.cs_connections[sequence_string]
            ret = 0
        else:
            ret = -1
        self.cs_condition.release()
        if ret == 0:
            log.cl_info("disconnected client [%s] is cleaned up",
                        client_uuid)
        else:
            log.cl_info("failed to disconnect client [%s], because it "
                        "doesnot exist", client_uuid)
        return ret

    def cs_fini(self):
        """
        Finish server
        """
        self.cs_instance.ci_fini()
        self.cs_running = False
        self.cs_client_socket.close()
        self.cs_worker_socket.close()
        self.cs_context.term()

    def cs_worker_thread(self, worker_index):
        """
        Worker routine
        """
        # pylint: disable=too-many-nested-blocks,too-many-locals
        # pylint: disable=too-many-branches,too-many-statements
        # Socket to talk to dispatcher
        instance = self.cs_instance

        name = "thread_worker_%s" % worker_index
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.cs_log.cl_error("failed to create directory [%s] on local host",
                                     thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.cs_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.cs_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting worker thread [%s]", worker_index)
        dispatcher_socket = self.cs_context.socket(zmq.REP)
        dispatcher_socket.connect(self.cs_url_worker)

        while self.cs_running:
            try:
                request_message = dispatcher_socket.recv()
            except zmq.ContextTerminated:
                log.cl_info("worker thread [%s] exiting because context has "
                            "been terminated", worker_index)
                break

            cmessage = clownfish_pb2.ClownfishMessage
            request = cmessage()
            request.ParseFromString(request_message)
            log.cl_debug("received request with type [%s]", request.cm_type)
            reply = cmessage()
            reply.cm_protocol_version = cmessage.CPV_ZERO
            reply.cm_errno = cmessage.CE_NO_ERROR

            if request.cm_type == cmessage.CMT_CONNECT_REQUEST:
                client_hash = request.cm_connect_request.ccrt_client_hash
                connection = self.cs_connection_allocate(client_hash)
                reply.cm_type = cmessage.CMT_CONNECT_REPLY
                reply.cm_connect_reply.ccry_client_hash = client_hash
                reply.cm_client_uuid = connection.cc_sequence
            else:
                client_uuid = request.cm_client_uuid
                reply.cm_client_uuid = client_uuid
                connection = self.cs_connection_find(client_uuid)
                if connection is None:
                    log.cl_error("received a request with UUID [%s] that "
                                 "doesnot exist",
                                 client_uuid)
                    # Reply type doesn't matter
                    reply.cm_type = cmessage.CMT_PING_REPLY
                    reply.cm_errno = cmessage.CE_NO_UUID
                elif request.cm_type == cmessage.CMT_PING_REQUEST:
                    reply.cm_type = cmessage.CMT_PING_REPLY
                elif request.cm_type == cmessage.CMT_COMMAND_DICT_REQUEST:
                    reply.cm_type = cmessage.CMT_COMMAND_DICT_REPLY
                    item_list = reply.cm_command_dict_reply.ccdry_items
                    for command in clownfish.CLOWNFISH_SERVER_COMMNADS.values():
                        item = item_list.add()
                        item.cci_command = command.cc_command
                        item.cci_need_child = command.cc_need_child
                        if command.cc_arguments is not None:
                            for argument in command.cc_arguments:
                                item.cci_arguments.append(argument)
                elif request.cm_type == cmessage.CMT_PWD_REQUEST:
                    reply.cm_type = cmessage.CMT_PWD_REPLY
                    reply.cm_pwd_reply.cpry_pwd = clownfish.clownfish_pwd(connection.cc_walk)
                elif request.cm_type == cmessage.CMT_COMMAND_REQUEST:
                    reply.cm_type = cmessage.CMT_COMMAND_REPLY
                    cmd_line = request.cm_command_request.ccrt_cmd_line
                    connection.cc_command(log, cmd_line, reply.cm_command_reply)
                elif request.cm_type == cmessage.CMT_COMMAND_PARTWAY_QUERY:
                    reply.cm_type = cmessage.CMT_COMMAND_REPLY
                    query = request.cm_command_partway_query
                    if query.ccpq_abort:
                        connection.cc_abort()
                    connection.cc_consume_command_log(log,
                                                      reply.cm_command_reply)
                elif request.cm_type == cmessage.CMT_COMMAND_CHILDREN_REQUEST:
                    reply.cm_type = cmessage.CMT_COMMAND_CHILDREN_REPLY
                    children = clownfish.clownfish_children(connection.cc_walk)
                    item_list = reply.cm_command_children_reply.cccry_children
                    for child in children:
                        item_list.append(child)
                else:
                    reply.cm_type = cmessage.CMT_GENERAL
                    reply.cm_errno = cmessage.CE_NO_TYPE
                    log.cl_error("recived a request with type [%s] that "
                                 "is not supported",
                                 request.cm_type)
                    continue
                if (reply.cm_type == cmessage.CMT_COMMAND_REPLY and
                        connection.cc_quit):
                    ret = self.cs_connection_delete(connection.cc_sequence)
                    if ret:
                        reply.cm_errno = cmessage.CE_NO_UUID

            reply_message = reply.SerializeToString()
            dispatcher_socket.send(reply_message)
        dispatcher_socket.close()
        log.cl_info("worker thread [%s] exited", worker_index)

    def cs_loop(self):
        """
        Proxy the server
        """
        # pylint: disable=bare-except
        try:
            zmq.proxy(self.cs_client_socket, self.cs_worker_socket)
        except:
            self.cs_log.cl_info("got exception when running proxy, exiting")


def clownfish_server_do_loop(log, workspace, config, config_fpath):
    """
    Server routine
    """
    # pylint: disable=unused-argument
    clownfish_server_port = utils.config_value(config, cstr.CSTR_CLOWNFISH_PORT)
    if clownfish_server_port is None:
        log.cl_info("no [%s] is configured, using port [%s]",
                    cstr.CSTR_CLOWNFISH_PORT,
                    constants.CLOWNFISH_DEFAULT_SERVER_PORT)
        clownfish_server_port = constants.CLOWNFISH_DEFAULT_SERVER_PORT

    clownfish_instance = clownfish.init_instance(log, workspace, config,
                                                 config_fpath)
    if clownfish_instance is None:
        log.cl_error("failed to init Clownfish")
        return -1

    cserver = ClownfishServer(log, clownfish_server_port, clownfish_instance)
    cserver.cs_loop()
    cserver.cs_fini()


def clownfish_server_loop(log, workspace, config_fpath):
    """
    Start Clownfish holding the configure lock
    """
    # pylint: disable=too-many-branches,bare-except,too-many-locals
    # pylint: disable=too-many-statements
    config_fd = open(config_fpath)
    ret = 0
    try:
        config = yaml.load(config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s", config_fpath,
                     traceback.format_exc())
        ret = -1
    config_fd.close()
    if ret:
        return -1

    try:
        ret = clownfish_server_do_loop(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    return ret


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s <config_file>" %
                 sys.argv[0])


def main():
    """
    Start clownfish server
    """
    cmd_general.main(constants.CLOWNFISH_CONFIG, CLOWNFISH_SERVER_LOG_DIR,
                     clownfish_server_loop)
