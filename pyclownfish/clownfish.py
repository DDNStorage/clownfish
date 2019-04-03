# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for clownfish
Clownfish is an automatic management system for Lustre
"""
# pylint: disable=too-many-lines
import getopt
import threading
import os
import time
import yaml

# Local libs
from pylcommon import utils
from pylcommon import parallel
from pylcommon import lustre
from pylcommon import cstr
from pylcommon import lyaml
from pyclownfish import clownfish_qos

CLOWNFISH_STATUS_CHECK_INTERVAL = 1

CLOWNFISH_COMMNAD_CD = "cd"
CLOWNFISH_COMMNAD_DISABLE = "disable"
CLOWNFISH_COMMNAD_ENABLE = "enable"
CLOWNFISH_COMMNAD_FORMAT = "format"
CLOWNFISH_COMMNAD_HELP = "h"
CLOWNFISH_COMMNAD_LS = "ls"
CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE = "R"
CLOWNFISH_COMMNAD_LS_OPTION_LONG_RECURSIVE = "recursive"
CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS = "s"
CLOWNFISH_COMMNAD_LS_OPTION_LONG_STATUS = "status"
CLOWNFISH_COMMNAD_MANUAL = "m"
CLOWNFISH_COMMNAD_MOUNT = "mount"
CLOWNFISH_COMMNAD_NONEXISTENT = "nonexistent"
CLOWNFISH_COMMNAD_PREPARE = "prepare"
CLOWNFISH_COMMNAD_PWD = "pwd"
CLOWNFISH_COMMNAD_QUIT = "q"
CLOWNFISH_COMMNAD_RETVAL = "retval"
CLOWNFISH_COMMNAD_UMOUNT = "umount"

CLOWNFISH_DELIMITER_AND = "AND"
CLOWNFISH_DELIMITER_OR = "OR"
CLOWNFISH_DELIMITER_CONT = "CONT"

# The command that can never finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_SLOW = "always_slow"
# The command that can always finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_FAST = "always_fast"
SPEED_SLOW_OR_FAST = "slow_or_fast"
MAX_FAST_COMMAND_TIME = 1


class ClownfishCommand(object):
    """
    Config command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, command, function, arguments=None, need_child=False,
                 speed=SPEED_ALWAYS_FAST):
        self.cc_command = command
        self.cc_function = function
        self.cc_arguments = arguments
        self.cc_need_child = need_child
        self.cc_speed = speed

# Server side commands
CLOWNFISH_SERVER_COMMNADS = {}


def clownfish_command_help(connection, args):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    log = connection.cc_command_log
    log.cl_stdout("""Command action:
   cd $entry            change the current entry to $entry
   disable              disable the current setting
   enable               enable the current setting
   format               format the filesystem
   h                    print this menu
   ls                   list sub entries under current entry
   ls -s|--status       list status information
   ls -R|--recursive    list sub entries recursively under current entry
   q                    quit
   m                    show the manual about the current path
   mount                mount the filesystem
   pwd                  print the current path
   umount               umount the filesystem""")

    return 0


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_HELP] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_HELP, clownfish_command_help)


def clownfish_command_retval(connection, args):
    # pylint: disable=unused-argument
    """
    Retun the last exit status
    """
    log = connection.cc_command_log
    log.cl_stdout("%s", connection.cc_last_retval)
    return 0


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_RETVAL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_RETVAL, clownfish_command_retval)


def clownfish_command_quit(connection, args):
    # pylint: disable=unused-argument
    """
    Quit this connection
    """
    connection.cc_quit = True
    log = connection.cc_command_log
    log.cl_stdout("disconnected from server")
    return 0


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_QUIT] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_QUIT, clownfish_command_quit)


def clownfish_command_ls(connection, args):
    """
    Print the children in the current directory
    """
    # pylint: disable=unused-variable,bare-except,too-many-locals,too-many-branches
    walk = connection.cc_walk
    log = connection.cc_command_log

    current_entry = walk.cw_entry_current
    list_all = False
    recursive = False

    short_options = ""
    short_options += CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS
    short_options += CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE

    long_options = []
    long_options.append(CLOWNFISH_COMMNAD_LS_OPTION_LONG_STATUS)
    long_options.append(CLOWNFISH_COMMNAD_LS_OPTION_LONG_RECURSIVE)
    try:
        options, remainder = getopt.getopt(args[1:], short_options,
                                           long_options)
        for opt, arg in options:
            if opt in ("-" + CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS,
                       "--" + CLOWNFISH_COMMNAD_LS_OPTION_LONG_STATUS):
                list_all = True
            elif opt in ("-" + CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE,
                         "--" + CLOWNFISH_COMMNAD_LS_OPTION_LONG_RECURSIVE):
                recursive = True
            else:
                log.cl_stderr('unkown option "%s %s"', opt, arg)
                return -1
    except:
        option_string = ""
        for arg in args:
            if option_string != "":
                option_string += " "
            option_string += arg
        log.cl_stderr('unkown option "%s"', option_string)
        return -1

    encoded = current_entry.ce_encode(list_all, recursive)
    if encoded is None:
        return -1

    if list_all or recursive:
        log.cl_stdout('%s', yaml.dump(encoded, Dumper=lyaml.YamlDumper,
                                      default_flow_style=False))
    else:
        for child in encoded:
            log.cl_stdout(child)
    return 0

CLOWNFISH_COMMNAD_LS_OPTIONS = []
CLOWNFISH_COMMNAD_LS_OPTIONS.append("-" + CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE)
CLOWNFISH_COMMNAD_LS_OPTIONS.append("-" + CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS)
CLOWNFISH_COMMNAD_LS_OPTIONS.append("--" + CLOWNFISH_COMMNAD_LS_OPTION_LONG_RECURSIVE)
CLOWNFISH_COMMNAD_LS_OPTIONS.append("--" + CLOWNFISH_COMMNAD_LS_OPTION_LONG_STATUS)
CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_LS] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_LS, clownfish_command_ls,
                     arguments=CLOWNFISH_COMMNAD_LS_OPTIONS)


def clownfish_command_manual(connection, args):
    """
    Print the manual about the current directory
    """
    walk = connection.cc_walk
    log = connection.cc_command_log
    current = walk.cw_entry_current

    return current.ce_command_manual(log, args)

CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_MANUAL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_MANUAL, clownfish_command_manual)


def clownfish_enter_entry(log, walk, current, escaped_entry_name):
    """
    Enter the escaped entry name
    """
    entry_name = escaped_entry_name.replace(r'\/', "/")
    if "\\" in entry_name:
        log.cl_stderr(r'invalid [\] in entry name [%s]', entry_name)
        return None

    if entry_name == "..":
        if current == walk.cw_entry_root:
            return current
        else:
            current = current.ce_parent_entry
            return current

    child = current.ce_child(entry_name)
    if child is None:
        log.cl_stderr('[%s] is not found under [%s]', entry_name,
                      current.ce_path)
        return None
    current = child
    return current


def clownfish_command_cd(connection, args):
    """
    Change the current directory
    """
    # pylint: disable=too-many-branches
    walk = connection.cc_walk
    log = connection.cc_command_log

    if len(args) == 1:
        walk.cw_entry_current = walk.cw_entry_root
        return 0

    arg = args[1]

    if arg[0] == "/":
        current = walk.cw_entry_root
    else:
        current = walk.cw_entry_current

    buf = arg[:]
    while True:
        while buf != "" and buf[0] == "/":
            buf = buf[1:]

        if buf == "":
            break

        next_buf = buf
        escaped_entry = ""
        while True:
            stop_index = next_buf.find("/")
            if stop_index < 0:
                # All of the following is a entry name
                escaped_entry += next_buf
                buf = ""
                break
            elif next_buf[stop_index - 1] != "\\":
                # The stop index should not be the first char, since leading /
                # have all been removed. This is not an escaped /, so actual
                # stop index.
                escaped_entry += next_buf[:stop_index]
                if stop_index == len(next_buf) - 1:
                    buf = ""
                else:
                    buf = next_buf[stop_index + 1:]
                break
            else:
                # All chars before / are escaped_entry, including /
                escaped_entry += next_buf[:stop_index + 1]
                if stop_index == len(next_buf) - 1:
                    buf = ""
                    break
                next_buf = next_buf[stop_index + 1:]

        current = clownfish_enter_entry(log, walk, current, escaped_entry)
        if current is None:
            return -1

    walk.cw_entry_current = current
    return 0


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_CD] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_CD, clownfish_command_cd,
                     need_child=True)


def clownfish_command_enable(connection, args):
    """
    Enable the option
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    walk = connection.cc_walk
    current = walk.cw_entry_current

    return current.ce_command_enable(log, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_ENABLE] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_ENABLE, clownfish_command_enable)


def clownfish_command_disable(connection, args):
    """
    Disable the option
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    walk = connection.cc_walk
    current = walk.cw_entry_current

    return current.ce_command_disable(log, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_DISABLE] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_DISABLE, clownfish_command_disable,
                     speed=SPEED_SLOW_OR_FAST)


def clownfish_command_format(connection, args):
    """
    Format the filesystem/OST/MDT/MGS
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    walk = connection.cc_walk
    current = walk.cw_entry_current

    return current.ce_command_format(log, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_FORMAT] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_FORMAT, clownfish_command_format,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_mount(connection, args):
    """
    Mount the filesystem/OST/MDT/MGS
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    walk = connection.cc_walk
    current = walk.cw_entry_current

    return current.ce_command_mount(log, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_MOUNT] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_MOUNT, clownfish_command_mount,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_prepare(connection, args):
    """
    Prepare the hosts
    """
    # pylint: disable=redefined-variable-type,unused-argument
    walk = connection.cc_walk
    current = walk.cw_entry_current

    return current.ce_command_prepare(connection, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_PREPARE] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_PREPARE, clownfish_command_prepare,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_umount(connection, args):
    """
    Umount the filesystem/OST/MDT/MGS
    """
    # pylint: disable=redefined-variable-type,unused-argument
    walk = connection.cc_walk
    log = connection.cc_command_log
    current = walk.cw_entry_current

    return current.ce_command_umount(log, args)


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_UMOUNT] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_UMOUNT, clownfish_command_umount,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_pwd(walk):
    """
    Print the config in the current directory
    """
    return walk.cw_entry_current.ce_path


def clownfish_path_level(escaped_path):
    """
    Return the entry level of pwd
    /: 0
    /lustres: 1
    escaped_path should be well formatted root path, not something like ///
    or relative path
    """
    if escaped_path == "/":
        return 0

    assert escaped_path[0] == "/"

    slash_number = escaped_path.count('/')
    escaped = escaped_path.count(r'\/')
    assert slash_number > escaped
    return slash_number - escaped


def clownfish_command_pwd(connection, args):
    """
    Print the current path
    """
    # pylint: disable=unused-argument
    walk = connection.cc_walk
    log = connection.cc_command_log
    log.cl_stdout(clownfish_pwd(walk))
    return 0


CLOWNFISH_SERVER_COMMNADS[CLOWNFISH_COMMNAD_PWD] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_PWD, clownfish_command_pwd)


def clownfish_children(walk):
    """
    Return the names/IDs of children
    """
    return walk.cw_entry_current.ce_encode(False, False)


class ClownfishWalk(object):
    """
    Each connection that is walking in the paths has a object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, instance):
        self.cw_entry_root = ClownfishEntryRoot(self)
        self.cw_entry_current = self.cw_entry_root
        self.cw_instance = instance


class ClownfishServiceStatus(object):
    """
    A global object for service status
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, instance, log):
        self.css_instance = instance
        # Keys are the LustreService.ls_service_name, value is instance of
        # LustreServiceStatus
        self.css_service_status_dict = {}
        # Protects css_service_status_dict
        self.css_service_status_condition = threading.Condition()
        # The status of services that have problems.
        # Keys are the LustreService.ls_service_name, value is instance of
        # LustreServiceStatus
        self.css_problem_status_dict = {}
        # Protects css_problem_status_dict and css_fix_thread_waiting_number
        self.css_problem_condition = threading.Condition()
        # The fixing time of services
        # Keys are teh LustreService.ls_service_name, value is time.time()
        self.css_fix_time_dict = {}
        # The fixing services
        self.css_fix_services = []
        # Protected by css_problem_condition
        self.css_fix_thread_waiting_number = 0
        self.css_fix_thread_number = 5
        self.css_log = log
        self.css_start_status_threads()
        self.css_start_fix_threads()

    def css_service_status(self, service_name):
        """
        Return the service status
        """
        self.css_service_status_condition.acquire()
        if service_name in self.css_service_status_dict:
            status = self.css_service_status_dict[service_name]
        else:
            status = None
        self.css_service_status_condition.release()
        return status

    def css_update_status(self, status):
        """
        Update the status
        """
        service = status.lss_service
        service_name = service.ls_service_name
        self.css_service_status_condition.acquire()
        self.css_service_status_dict[service_name] = status
        self.css_service_status_condition.release()

        self.css_problem_condition.acquire()
        if status.lss_has_problem():
            self.css_problem_status_dict[service_name] = status
            self.css_problem_condition.notifyAll()
        else:
            if service_name in self.css_problem_status_dict:
                del self.css_problem_status_dict[service_name]
        self.css_problem_condition.release()

    def css_status_thread(self, service):
        """
        Thread that checks status of a service
        """
        service_name = service.ls_service_name
        instance = self.css_instance

        name = "thread_checking_service_%s" % service_name
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.css_log.cl_error("failed to create direcotry [%s] on local host",
                                      thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.css_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.css_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting thread that checks status of service [%s]",
                    service_name)
        while instance.ci_running:
            status = lustre.LustreServiceStatus(service)
            status.lss_check(log)

            self.css_update_status(status)

            time.sleep(CLOWNFISH_STATUS_CHECK_INTERVAL)
        log.cl_info("thread that checks status of service [%s] exited",
                    service_name)
        return 0

    def css_start_status_threads(self):
        """
        Start the status thread
        """
        instance = self.css_instance

        for mgs in instance.ci_mgs_dict.values():
            utils.thread_start(self.css_status_thread,
                               (mgs, ))

        for lustrefs in instance.ci_lustres.values():
            services = lustrefs.lf_services()
            for service in services:
                utils.thread_start(self.css_status_thread,
                                   (service, ))

    def css_fix_thread(self, thread_id):
        """
        Thread that fix the services
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        instance = self.css_instance

        name = "thread_fixing_service_%s" % thread_id
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.css_log.cl_error("failed to create direcotry [%s] on local host",
                                      thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.css_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.css_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting thread [%s] that fix services", thread_id)
        fixing_status = None
        while instance.ci_running:
            self.css_problem_condition.acquire()
            if fixing_status is not None:
                fix_name = fixing_status.lss_service.ls_service_name
                assert fix_name in self.css_fix_services
                fix_index = self.css_fix_services.index(fix_name)
                del self.css_fix_services[fix_index]

            # When HA is disabled, this thread does nothing
            self.css_fix_thread_waiting_number += 1
            self.css_problem_condition.notifyAll()
            while ((not instance.ci_high_availability) or
                   (len(self.css_problem_status_dict) == 0)):
                self.css_problem_condition.wait()
            self.css_fix_thread_waiting_number -= 1
            #
            # Do no remove the status from the dictionary, remove it after
            # fixing, because the check threads might add the status when
            # fixing anyway.
            #
            # The priority level of services is:
            # 1. The MGS or the MDT combined with MGS
            # 2. The MDTs
            # 3. The OSTs
            # For the services that have the same priority, the service that
            # has smaller fix time has the higher priority
            fixing_status = None
            for status in self.css_problem_status_dict.values():
                service = status.lss_service
                service_type = service.ls_service_type
                service_name = service.ls_service_name

                if service_name in self.css_fix_services:
                    continue

                if fixing_status is None:
                    fixing_status = status
                    continue

                fix_service = fixing_status.lss_service
                fix_type = fix_service.ls_service_type
                fix_name = fix_service.ls_service_name
                fix_is_mgs = bool((fix_type == lustre.LUSTRE_SERVICE_TYPE_MGS) or
                                  (fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT and
                                   fix_service.lmdt_is_mgs))

                is_mgs = bool((service_type == lustre.LUSTRE_SERVICE_TYPE_MGS) or
                              (service_type == lustre.LUSTRE_SERVICE_TYPE_MDT and
                               service.lmdt_is_mgs))

                if fix_name not in self.css_fix_time_dict:
                    fix_time_ealier = True
                elif service_name not in self.css_fix_time_dict:
                    fix_time_ealier = False
                else:
                    fix_time_ealier = bool(self.css_fix_time_dict[fix_name] <=
                                           self.css_fix_time_dict[service_name])

                if is_mgs:
                    if fix_is_mgs:
                        if not fix_time_ealier:
                            fixing_status = status
                    else:
                        fixing_status = status
                elif service_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                    if fix_is_mgs:
                        pass
                    elif fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                        if not fix_time_ealier:
                            fixing_status = status
                    else:
                        fixing_status = status
                else:
                    if fix_is_mgs:
                        pass
                    elif fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                        pass
                    else:
                        if not fix_time_ealier:
                            fixing_status = status
            if fixing_status is not None:
                fix_name = fixing_status.lss_service.ls_service_name
                self.css_fix_time_dict[fix_name] = time.time()
                assert fix_name not in self.css_fix_services
                self.css_fix_services.append(fix_name)
            self.css_problem_condition.release()

            if fixing_status is None:
                continue

            service = fixing_status.lss_service
            service_name = service.ls_service_name

            log.cl_info("checking the status of service [%s]", service_name)
            # Check the status by myself, since the status might be outdated
            status = lustre.LustreServiceStatus(fixing_status.lss_service)
            status.lss_check(log)
            if status.lss_has_problem():
                ret = status.lss_fix_problem(log)
                if ret:
                    log.cl_error("failed to fix problem of service [%s]",
                                 service_name)

                service = fixing_status.lss_service
                service_name = service.ls_service_name
                status = lustre.LustreServiceStatus(fixing_status.lss_service)
                status.lss_check(log)
                if status.lss_has_problem():
                    log.cl_error("service [%s] still has problem after fixing",
                                 service_name)
                else:
                    log.cl_info("service [%s] was successfully fixed",
                                service_name)
            else:
                log.cl_info("the problem of service [%s] has disapeared "
                            "without fixing", service_name)

            # Update the status
            self.css_update_status(status)
        log.cl_info("thread [%s] that fix services exited", thread_id)

    def css_start_fix_threads(self):
        """
        Start the status thread
        """
        for thread_id in range(self.css_fix_thread_number):
            utils.thread_start(self.css_fix_thread, (thread_id, ))


class ClownfishInstance(object):
    """
    This instance saves the global clownfish information
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # pylint: disable=too-many-arguments,too-many-public-methods
    def __init__(self, log, workspace, lazy_prepare, hosts, mgs_dict, lustres,
                 high_availability, qos_dict, no_operation=False):
        self.ci_lazy_prepare = lazy_prepare
        # Keys are the host IDs, not the hostnames
        self.ci_hosts = hosts
        # Keys are the MGS IDs, values ares instances of LustreService
        self.ci_mgs_dict = mgs_dict
        # Keys are the fsname, values ares instances of LustreFilesystem
        self.ci_lustres = lustres
        self.ci_workspace = workspace
        self.ci_running = True
        self.ci_high_availability = high_availability
        if not no_operation:
            self.ci_service_status = ClownfishServiceStatus(self, log)
        self.ci_qos_dict = qos_dict

    def ci_mount_lustres(self, log):
        """
        Mount all Lustre file systems, including MGS if necessary
        """
        for lustrefs in self.ci_lustres.values():
            if log.cl_abort:
                log.cl_stderr("aborting mounting file systems")
                return -1
            ret = lustrefs.lf_mount(log)
            if ret:
                log.cl_stderr("failed to mount file system [%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_umount_lustres(self, log):
        """
        Umount all Lustre file systems, not including MGS
        """
        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_umount(log)
            if ret:
                log.cl_stderr("failed to umount file system [%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_mount_mgs(self, log):
        """
        Mount all MGS
        """
        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_mount(log)
            if ret:
                log.cl_stderr("failed to mount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_umount_mgs(self, log):
        """
        Umount all MGS
        """
        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_umount(log)
            if ret:
                log.cl_stderr("failed to umount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_umount_all(self, log):
        """
        Umount all file system and MGS
        """
        ret = self.ci_umount_lustres(log)
        if ret:
            log.cl_stderr("failed to umount all Lustre file systems")
            return -1

        ret = self.ci_umount_mgs(log)
        if ret:
            log.cl_stderr("failed to umount all MGS")
            return -1

        return 0

    def ci_umount_all_nolock(self, log):
        """
        Umount all file system and MGS
        Locks should be held
        """
        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_umount_nolock(log)
            if ret:
                log.cl_stderr("failed to umount file system [%s]",
                              lustrefs.lf_fsname)
                return -1

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_umount_nolock(log)
            if ret:
                log.cl_stderr("failed to umount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_mount_all(self, log):
        """
        Mount all file system and MGS
        """
        ret = self.ci_mount_mgs(log)
        if ret:
            log.cl_stderr("failed to mount all MGS")
            return -1

        ret = self.ci_mount_lustres(log)
        if ret:
            log.cl_stderr("failed to mount all Lustre file systems")
            return -1

        return 0

    def ci_format_all_nolock(self, log):
        """
        Format all file system and MGS
        Locks should be held
        """
        ret = self.ci_umount_all_nolock(log)
        if ret:
            log.cl_stderr("failed to umount all")
            return ret

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_format_nolock(log)
            if ret:
                log.cl_stderr("failed to umount and format MGS [%s]",
                              mgs.ls_service_name)
                return -1

        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_format_nolock(log)
            if ret:
                log.cl_stderr("failed to umount and format Lustre file system "
                              "[%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_format_all(self, log):
        """
        Format all file system and MGS
        """

        lock_handles = []
        for mgs in self.ci_mgs_dict.values():
            mgs_lock_handle = mgs.ls_lock.rwl_writer_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting formating all file systems and MGS")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(mgs_lock_handle)

        for lustrefs in self.ci_lustres.values():
            fs_lock_handle = lustrefs.lf_lock.rwl_writer_acquire(log)
            if fs_lock_handle is None:
                log.cl_stderr("aborting formating all file systems and MGS")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(fs_lock_handle)

        ret = self.ci_format_all_nolock(log)

        for lock_handle in reversed(lock_handles):
            lock_handle.rwh_release()

        return ret

    def ci_prepare_all_nolock(self, log, workspace):
        """
        Prepare all hosts
        Locks should be held
        """
        ret = self.ci_umount_all_nolock(log)
        if ret:
            log.cl_stderr("failed to umount all")
            return ret

        args_array = []
        thread_ids = []
        for host in self.ci_hosts.values():
            args = (host, self.ci_lazy_prepare)
            args_array.append(args)
            thread_id = "prepare_%s" % host.sh_host_id
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(log, workspace,
                                                    "host_prepare",
                                                    lustre.host_lustre_prepare,
                                                    args_array,
                                                    thread_ids=thread_ids,
                                                    parallelism=8)
        return parallel_execute.pe_run()

    def ci_prepare_all(self, log, workspace):
        """
        Prepare all hosts
        """
        lock_handles = []
        for mgs in self.ci_mgs_dict.values():
            mgs_lock_handle = mgs.ls_lock.rwl_writer_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting preparing all hosts")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(mgs_lock_handle)

        for lustrefs in self.ci_lustres.values():
            fs_lock_handle = lustrefs.lf_lock.rwl_writer_acquire(log)
            if fs_lock_handle is None:
                log.cl_stderr("aborting preparing all hosts")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(fs_lock_handle)

        ret = self.ci_prepare_all_nolock(log, workspace)

        for lock_handle in reversed(lock_handles):
            lock_handle.rwh_release()

        return ret

    def ci_fini(self):
        """
        quiting
        """
        self.ci_running = False

    def ci_high_availability_enable(self):
        """
        Enable high availability
        """
        self.ci_high_availability = True

    def ci_high_availability_disable(self, log):
        """
        disable high availability
        """
        service_status = self.ci_service_status

        self.ci_high_availability = False
        ret = 0
        service_status.css_problem_condition.acquire()
        while (service_status.css_fix_thread_waiting_number !=
               service_status.css_fix_thread_number):
            if log.cl_abort:
                ret = -1
                break
            service_status.css_problem_condition.wait()
        service_status.css_problem_condition.release()

        if log.cl_abort or ret < 0:
            ret = -1
            log.cl_stderr("abort waiting high availability to be disabled")
        elif ret == 0:
            log.cl_stdout("disabled high availability")
        return ret

    def ci_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        service_status = self.ci_service_status
        status_funct = service_status.css_service_status

        if not need_structure and not need_status:
            return [cstr.CSTR_HOSTS, cstr.CSTR_MGS_LIST, cstr.CSTR_LUSTRES,
                    cstr.CSTR_LAZY_PREPARE, cstr.CSTR_HIGH_AVAILABILITY]

        mgs_list = []
        for mgs in self.ci_mgs_dict.values():
            mgs_list.append(mgs.ls_encode(need_status,
                                          status_funct,
                                          need_structure))
        instance_code = {cstr.CSTR_MGS_LIST: mgs_list}

        lustres = []
        for lustrefs in self.ci_lustres.values():
            lustres.append(lustrefs.lf_encode(need_status,
                                              status_funct,
                                              need_structure))
        instance_code[cstr.CSTR_LUSTRES] = lustres
        if need_structure:
            instance_code[cstr.CSTR_LAZY_PREPARE] = self.ci_lazy_prepare

        hosts = []
        for host in self.ci_hosts.values():
            hosts.append(host.lsh_encode(need_status,
                                         status_funct,
                                         need_structure))
        instance_code[cstr.CSTR_HOSTS] = hosts
        return instance_code


def clownfish_entry_escape(entry_name):
    r"""
    Return the escaped entry ename by replacing the "/" to "\/"
    """
    escaped_name = entry_name.replace("/", r"\/")
    return escaped_name


class ClownfishEntry(object):
    """
    Common entry
    """
    # pylint: disable=no-self-use,unused-argument
    def __init__(self, walk, entry_name, parent_entry):
        self.ce_walk = walk
        self.ce_entry_name = entry_name
        self.ce_parent_entry = parent_entry
        self.ce_escaped_name = clownfish_entry_escape(entry_name)
        if parent_entry is None:
            assert entry_name == "/"
            self.ce_path = "/"
        elif parent_entry.ce_path == "/":
            self.ce_path = "/" + self.ce_escaped_name
        else:
            self.ce_path = parent_entry.ce_path + "/" + self.ce_escaped_name
        self.ceh_entry_name = entry_name

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        return None

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stderr("no manual for this path")
        return -1

    def ce_command_enable(self, log, args):
        """
        Implementation of enable command
        """
        log.cl_stderr("can not enable in this path")
        return -1

    def ce_command_disable(self, log, args):
        """
        Implementation of disable command
        """
        log.cl_stderr("can not disable in this path")
        return -1

    def ce_command_format(self, log, args):
        """
        Implementation of format command
        """
        log.cl_stderr("can not format in this path")
        return -1

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        log.cl_stderr("can not mount in this path")
        return -1

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        log.cl_stderr("can not umount in this path")
        return -1

    def ce_command_prepare(self, connection, args):
        """
        Implementation of prepare command
        """
        log = connection.cc_command_log
        log.cl_stderr("can not prepare in this path")
        return -1

    def ce_status_encode(self, log):
        """
        Implementation of status
        """
        log.cl_stderr("no status in this path")
        return None


class ClownfishEntryLustreClient(ClownfishEntry):
    """
    Each Lustre client has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustre_client):
        super(ClownfishEntryLustreClient, self).__init__(walk, entry_name,
                                                         parent_entry)
        self.celc_lustre_client = lustre_client

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        return self.celc_lustre_client.lc_encode(need_status, None, need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        client = self.celc_lustre_client
        log.cl_stdout("This is the client of Lustre file system [%s] which "
                      "is mounted on mount point [%s] of host [%s].",
                      client.lc_lustre_fs.lf_fsname,
                      client.lc_mnt,
                      client.lc_host.sh_hostname)
        log.cl_stdout("No command is supported for client operation now.")
        return 0


class ClownfishEntryLustreClients(ClownfishEntry):
    """
    Each lustres on a host has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustre_clients):
        super(ClownfishEntryLustreClients, self).__init__(walk, entry_name,
                                                          parent_entry)
        self.celcs_lustre_clients = lustre_clients

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk

        if name in self.celcs_lustre_clients:
            lustre_client = self.celcs_lustre_clients[name]
            return ClownfishEntryLustreClient(walk, name, self, lustre_client)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if need_structure or need_status:
            clients = []
            for client in self.celcs_lustre_clients.values():
                clients.append(client.lc_encode(need_status, None,
                                                need_structure))
            return clients
        else:
            return self.celcs_lustre_clients.keys()

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        parent = self.ce_parent_entry
        if isinstance(parent, ClownfishEntryLustre):
            manual = ("""This is path for the clients of Lustre file system [%s].
To list the clients, please run [ls] command.
The format of the output of [ls] would have the format of $HOSTNAME:$MOUNT_POINT
""" % parent.cel_lustrefs.lf_fsname)
            log.cl_stdout(manual)
        else:
            assert isinstance(parent, ClownfishEntryHost)
            manual = ("""This is path for the clients of Lustre clients on host [%s].
To list the clients, please run [ls] command.
The format of the output of [ls] would have the format of $FSNAME:$MOUNT_POINT
""" % parent.ceh_host.sh_hostname)
            log.cl_stdout(manual)
        return 0


class ClownfishEntryLustreService(ClownfishEntry):
    """
    Each Lustre service has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, service):
        super(ClownfishEntryLustreService, self).__init__(walk, entry_name,
                                                          parent_entry)
        self.cels_service = service

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        service = self.cels_service
        if name in service.ls_instances:
            service_instance = service.ls_instances[name]
            return ClownfishEntryServiceInstance(walk, name, self,
                                                 service_instance)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        service = self.cels_service
        walk = self.ce_walk
        instance = walk.cw_instance
        service_status = instance.ci_service_status

        return service.ls_encode(need_status, service_status.css_service_status,
                                 need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is path for the Lustre service [%s].",
                      self.cels_service.ls_service_name)
        log.cl_stdout('To show the status of the service, please run [ls -s] command.')
        log.cl_stdout('To show the instances of the service, please run [ls] command.')
        log.cl_stdout('The format of the output of [ls] would have the format of $HOSTNAME:$DEVICE')
        return 0

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        service = self.cels_service
        lustrefs = service.ls_lustre_fs

        ret = lustrefs.lf_mount_service(log, service)
        if ret:
            log.cl_stderr("failed to mount service [%s]",
                          service.ls_service_name)
        return ret

    def ce_command_umount(self, log, args):
        """
        Implementation of mount command
        """
        service = self.cels_service
        lustrefs = service.ls_lustre_fs

        ret = lustrefs.lf_umount_service(log, service)
        if ret:
            log.cl_stderr("failed to umount service [%s]",
                          service.ls_service_name)
        return ret


class ClownfishEntryLustreServices(ClownfishEntry):
    """
    Each lustre services on a host has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, services):
        super(ClownfishEntryLustreServices, self).__init__(walk, entry_name,
                                                           parent_entry)
        self.celss_services = services

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk

        if name in self.celss_services:
            service = self.celss_services[name]
            return ClownfishEntryLustreService(walk, name, self, service)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status

        services = []
        for service in self.celss_services.values():
            service_name = service.ls_service_name
            if need_status:
                status = service_status.css_service_status(service_name)
                if need_structure:
                    services.append(service.ls_encode(True,
                                                      service_status.css_service_status,
                                                      True))
                else:
                    service_code = {}
                    if status is None:
                        service_code[cstr.CSTR_SERVICE_NAME] = service_name
                        service_code[cstr.CSTR_STATUS] = cstr.CSTR_UNKNOWN
                    else:
                        service_code = status.lss_encode(False)
                    services.append(service_code)
            else:
                if need_structure:
                    services.append(service.ls_encode(False,
                                                      service_status.css_service_status,
                                                      True))
                else:
                    services.append(service_name)
        return services

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        parent = self.ce_parent_entry
        assert isinstance(parent, ClownfishEntryLustre)
        manual = ("""This is path for the %s of Lustre file system [%s].
To list the %s, please run [ls] command.
The format of the output of [ls] would have the format of $FSNAME-$SERVICE_UUID""" %
                  (self.ce_entry_name, parent.cel_lustrefs.lf_fsname,
                   self.ce_entry_name))
        log.cl_stdout(manual)
        return 0


class ClownfishEntryServiceInstance(ClownfishEntry):
    """
    Each Lustre service instance has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, service_instance):
        super(ClownfishEntryServiceInstance, self).__init__(walk, entry_name,
                                                            parent_entry)
        self.cesi_service_instance = service_instance

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status
        status_funct = service_status.css_service_status

        return self.cesi_service_instance.lsi_encode(need_status, status_funct,
                                                     need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        instance = self.cesi_service_instance
        log.cl_stdout("This is the instance [%s] of the service [%s].",
                      instance.lsi_service_instance_name,
                      instance.lsi_service.ls_service_name)
        log.cl_stdout('To show the status of the instance, please run [ls -s] command.')
        return 0


class ClownfishEntryServiceInstances(ClownfishEntry):
    """
    Each lustre service instances on a host has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, instances):
        super(ClownfishEntryServiceInstances, self).__init__(walk, entry_name,
                                                             parent_entry)
        self.cesi_service_instances = instances

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk

        if name in self.cesi_service_instances:
            service_instance = self.cesi_service_instances[name]
            return ClownfishEntryServiceInstance(walk, name, self,
                                                 service_instance)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if not need_structure and not need_status:
            return self.cesi_service_instances.keys()

        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status
        status_funct = service_status.css_service_status

        instances = []
        for service_instance in self.cesi_service_instances.values():
            instances.append(service_instance.lsi_encode(need_status,
                                                         status_funct,
                                                         need_structure))
        return instances

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        parent = self.ce_parent_entry
        assert isinstance(parent, ClownfishEntryHost)
        manual = ("""This is the %s instances on the host [%s].
To list the %s instances, please run [ls] command.
The format of the output of [ls] would have the format of $FSNAME-$SERVICE_UUID
""" %
                  (self.ce_entry_name, parent.ceh_host.sh_hostname,
                   self.ce_entry_name))
        log.cl_stdout(manual)
        return 0


class ClownfishEntryHost(ClownfishEntry):
    """
    Each host has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, host):
        super(ClownfishEntryHost, self).__init__(walk, entry_name,
                                                 parent_entry)
        self.ceh_host = host

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        if name == cstr.CSTR_CLIENTS:
            return ClownfishEntryLustreClients(walk, name, self,
                                               self.ceh_host.lsh_clients)
        elif name == cstr.CSTR_OSTS:
            return ClownfishEntryServiceInstances(walk, name, self,
                                                  self.ceh_host.lsh_ost_instances)
        elif name == cstr.CSTR_MDTS:
            return ClownfishEntryServiceInstances(walk, name, self,
                                                  self.ceh_host.lsh_mdt_instances)
        elif name == cstr.CSTR_MGS and self.ceh_host.lsh_mgsi is not None:
            return ClownfishEntryServiceInstance(walk, name, self,
                                                 self.ceh_host.lsh_mgsi)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status
        return self.ceh_host.lsh_encode(need_status,
                                        service_status.css_service_status,
                                        need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the directory of the host [%s].",
                      self.ceh_host.sh_hostname)
        log.cl_stdout('To list the subdirs, please run [ls] command.')
        return 0


class ClownfishEntryHosts(ClownfishEntry):
    """
    Each hosts on ROOT has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, hosts):
        super(ClownfishEntryHosts, self).__init__(walk, entry_name,
                                                  parent_entry)
        self.ceh_hosts = hosts

    def ce_child(self, name):
        """
        Create and return child entry
        """
        if name in self.ceh_hosts:
            host = self.ceh_hosts[name]
            return ClownfishEntryHost(self.ce_walk, name, self, host)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status
        if need_structure or need_status:
            hosts = []
            for host in self.ceh_hosts.values():
                host_encoded = host.lsh_encode(need_status,
                                               service_status.css_service_status,
                                               need_structure)
                hosts.append(host_encoded)
            return hosts
        else:
            return self.ceh_hosts.keys()

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the entry for all the hosts configured.")
        log.cl_stdout('To list the hosts, please run [ls] command.')
        log.cl_stdout('The output is the host IDs, not the hostnames')
        return 0


class ClownfishEntryQosInterval(ClownfishEntry):
    """
    Each interval of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosInterval, self).__init__(walk, entry_name,
                                                        parent_entry)
        self.ceqi_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqi_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_INTERVAL: qos.cdqos_interval}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqi_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the QoS time interval of file system [%s].",
                      fsname)
        log.cl_stdout("QoS time interval is the length of each QoS time period.")
        log.cl_stdout("At the beginning of the time period, data and metadata "
                      "throughput of each user will be recorded from zero.")
        log.cl_stdout("During the time period, if the total data or metadata "
                      "throughput of a user reaches the threashold, TBF "
                      "throttling will be enforced.")
        log.cl_stdout("All TBF rules will be cleared at the beginning of "
                      "next time period.")
        return 0


class ClownfishEntryQosTrottledOssRpcRate(ClownfishEntry):
    """
    Each OSS RPC rate of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosTrottledOssRpcRate, self).__init__(walk, entry_name,
                                                                  parent_entry)
        self.ceqtorr_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqtorr_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_THROTTLED_OSS_RPC_RATE: qos.cdqos_throttled_oss_rpc_rate}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqtorr_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the default RPC limit on OSS when a "
                      "user is being throttled [%s].", fsname)
        log.cl_stdout("If a user has no specific configuration of RPC limit, "
                      "this value will be used for the user.")
        log.cl_stdout("RPC limit is the top RPC rate allowed for the throttled "
                      "user on each service part.")
        log.cl_stdout("Note that an OSS might have multiple service parts.")
        return 0


class ClownfishEntryQosTrottledMdsRpcRate(ClownfishEntry):
    """
    Each MDS RPC rate of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosTrottledMdsRpcRate, self).__init__(walk, entry_name,
                                                                  parent_entry)
        self.ceqtmrr_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqtmrr_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_THROTTLED_MDS_RPC_RATE: qos.cdqos_throttled_mds_rpc_rate}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqtmrr_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the default RPC limit on MDS when a "
                      "user is being throttled [%s].", fsname)
        log.cl_stdout("If a user has no specific configuration of RPC limit, "
                      "this value will be used for the user.")
        log.cl_stdout("RPC limit is the top RPC rate allowed for the throttled "
                      "user on each service part.")
        log.cl_stdout("Note that a MDS might have multiple service parts.")
        return 0


class ClownfishEntryQosMbpsThreshold(ClownfishEntry):
    """
    Each rate threshold of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosMbpsThreshold, self).__init__(walk, entry_name,
                                                             parent_entry)
        self.ceqmt_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqmt_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_MBPS_THRESHOLD: qos.cdqos_mbps_threshold}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqmt_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the default QoS I/O throughput rate "
                      "threshold of file system [%s].",
                      fsname)
        log.cl_stdout("If a user has no specific configuration of I/O "
                      "throughput rate threshold, this value will be used for "
                      "that user.")
        log.cl_stdout("The average I/O throughput rate (MB/s) of the user "
                      "during the QoS interval shall not be higher than this "
                      "threshold.")
        log.cl_stdout("After the threshold is reached, TBF limitation will "
                      "be enforced for that user on all OSS.")
        return 0


class ClownfishEntryQoSIopsThreshold(ClownfishEntry):
    """
    Each rate threshold of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQoSIopsThreshold, self).__init__(walk, entry_name,
                                                             parent_entry)
        self.ceqit_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqit_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_IOPS_THRESHOLD: qos.cdqos_iops_threshold}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqit_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the default QoS IOPS threshold of "
                      "file system [%s].", fsname)
        log.cl_stdout("If a user has no specific configuration of IOPS "
                      "threshold, this value will be used for that user.")
        log.cl_stdout("The average IOPS of the user during the QoS interval "
                      "shall not be higher than this threshold.")
        log.cl_stdout("After the threshold is reached, TBF limitation will "
                      "be enforced for that user on all MDS.")
        return 0


class ClownfishEntryEsmonServerHostname(ClownfishEntry):
    """
    hostname of ESMON server for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryEsmonServerHostname, self).__init__(walk, entry_name,
                                                                parent_entry)
        self.ceesh_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceesh_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_ESMON_SERVER_HOSTNAME: qos.cdqos_esmon_server_hostname}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceesh_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the hostname of ESMON server for file system [%s].",
                      fsname)
        log.cl_stdout("ESMON is used by Clownfish to collect performance statistics of users.")
        return 0


class ClownfishEntryQosEnabled(ClownfishEntry):
    """
    Each file system with QoS support will has an entry of this
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosEnabled, self).__init__(walk,
                                                       entry_name,
                                                       parent_entry)
        self.ceqe_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceqe_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_ENABLED: qos.cdqos_enabled}
        else:
            return []

    def ce_command_enable(self, log, args):
        """
        Implementation of enable command
        """
        qos = self.ceqe_lustrefs.lf_qos
        return qos.cqqos_enable(log)

    def ce_command_disable(self, log, args):
        """
        Implementation of disable command
        """
        qos = self.ceqe_lustrefs.lf_qos
        return qos.cqqos_disable(log)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqe_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for enabling/disabling QoS of file system [%s].", fsname)
        log.cl_stdout("If this entry is enabled, Clownfish will enforce QoS "
                      "rules on file system [%s].", fsname)
        log.cl_stdout("To enable QoS for file system [%s], please run "
                      "[enable] command.", fsname)
        log.cl_stdout("To disable QoS for file system [%s], please run "
                      "[disable] command.", fsname)
        return 0


class ClownfishEntryEsmonCollectInterval(ClownfishEntry):
    """
    Each collect interval of ESMON for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryEsmonCollectInterval, self).__init__(walk,
                                                                 entry_name,
                                                                 parent_entry)
        self.ceeci_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.ceeci_lustrefs.lf_qos
        if need_structure or need_status:
            return {cstr.CSTR_ESMON_COLLECT_INTERVAL: qos.cdqos_esmon_collect_interval}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceeci_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the collecting interval of ESPerfMon "
                      "server for file system [%s].", fsname)
        log.cl_stdout("Clownfish needs this information to calculate the "
                      "I/O througput of each user from the performance collected by "
                      "ESPerfMon.")
        return 0


class ClownfishEntryQosUserUid(ClownfishEntry):
    """
    Uid for QoS has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUserUid, self).__init__(walk,
                                                       entry_name,
                                                       parent_entry)
        self.cequu_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequu_user
        if need_structure or need_status:
            return {cstr.CSTR_UID: qos_user.cdqosu_uid}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is path for uid of this user.")
        return 0


class ClownfishEntryQosUserMbpsThreshold(ClownfishEntry):
    """
    Each user of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUserMbpsThreshold, self).__init__(walk, entry_name,
                                                                 parent_entry)
        self.cequmt_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequmt_user
        if need_structure or need_status:
            return {cstr.CSTR_MBPS_THRESHOLD: qos_user.cdqosu_mbps_threshold}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        qos_user = self.cequmt_user
        uid = qos_user.cdqosu_uid
        log.cl_stdout("This is path for the default QoS MB/s threshold of uid "
                      "[%s].", uid)
        log.cl_stdout("The average I/O throughput rate (MB/s) of the user with "
                      "uid [%s] during the QoS interval shall not be higher than this "
                      "threshold.", uid)
        log.cl_stdout("After the threshold is reached, TBF limitation will "
                      "be enforced for that user on all MDS.")
        return 0


class ClownfishEntryQosUserIopsThreshold(ClownfishEntry):
    """
    Each user of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUserIopsThreshold, self).__init__(walk, entry_name,
                                                                 parent_entry)
        self.cequmi_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequmi_user
        if need_structure or need_status:
            return {cstr.CSTR_MBPS_THRESHOLD: qos_user.cdqosu_mbps_threshold}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        qos_user = self.cequmi_user
        uid = qos_user.cdqosu_uid
        log.cl_stdout("This is path for the default QoS IOPS threshold of uid "
                      "[%s].", uid)
        log.cl_stdout("The average IOPS of the user with "
                      "uid [%s] during the QoS interval shall not be higher than this "
                      "threshold.", uid)
        log.cl_stdout("After the threshold is reached, TBF limitation will "
                      "be enforced for that user on all MDS.")
        return 0


class ClownfishEntryQosUserTrottledOssRpcRate(ClownfishEntry):
    """
    Each user of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUserTrottledOssRpcRate, self).__init__(walk, entry_name,
                                                                      parent_entry)
        self.cequtorr_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequtorr_user
        if need_structure or need_status:
            return {cstr.CSTR_THROTTLED_OSS_RPC_RATE: qos_user.cdqosu_throttled_oss_rpc_rate}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        qos_user = self.cequtorr_user
        uid = qos_user.cdqosu_uid
        log.cl_stdout("This is path for the RPC limit of user [%s] on OSS "
                      "when the user is being throttled.", uid)
        log.cl_stdout("This value overwrites the default RPC limit configured "
                      "for the file system.")
        log.cl_stdout("RPC limit is the top RPC rate allowed for the throttled "
                      "user on each service part.")
        log.cl_stdout("Note that an OSS might have multiple service parts.")
        return 0


class ClownfishEntryQosUserTrottledMdsRpcRate(ClownfishEntry):
    """
    Each user of QoS for a file system has an object of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUserTrottledMdsRpcRate, self).__init__(walk, entry_name,
                                                                      parent_entry)
        self.cequtmrr_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequtmrr_user
        if need_structure or need_status:
            return {cstr.CSTR_THROTTLED_MDS_RPC_RATE: qos_user.cdqosu_throttled_mds_rpc_rate}
        else:
            return []

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        qos_user = self.cequtmrr_user
        uid = qos_user.cdqosu_uid
        log.cl_stdout("This is path for the RPC limit of user [%s] on MDS "
                      "when the user is being throttled.", uid)
        log.cl_stdout("This value overwrites the default RPC limit configured "
                      "for the file system.")
        log.cl_stdout("RPC limit is the top RPC rate allowed for the throttled "
                      "user on each service part.")
        log.cl_stdout("Note that an MDS might have multiple service parts.")
        return 0


class ClownfishEntryQosUser(ClownfishEntry):
    """
    Each Lustre service has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, qos_user):
        super(ClownfishEntryQosUser, self).__init__(walk, entry_name,
                                                    parent_entry)
        self.cequ_user = qos_user

    def ce_child(self, name):
        """
        Create and return child entry
        """
        qos_user = self.cequ_user
        walk = self.ce_walk
        if name == cstr.CSTR_UID:
            return ClownfishEntryQosUserUid(walk, name, self,
                                            qos_user)
        elif name == cstr.CSTR_THROTTLED_OSS_RPC_RATE:
            return ClownfishEntryQosUserTrottledOssRpcRate(walk, name,
                                                           self, qos_user)
        elif name == cstr.CSTR_THROTTLED_MDS_RPC_RATE:
            return ClownfishEntryQosUserTrottledMdsRpcRate(walk, name,
                                                           self, qos_user)
        elif name == cstr.CSTR_MBPS_THRESHOLD:
            return ClownfishEntryQosUserMbpsThreshold(walk, name,
                                                      self,
                                                      qos_user)
        elif name == cstr.CSTR_MBPS_THRESHOLD:
            return ClownfishEntryQosUserIopsThreshold(walk, name,
                                                      self,
                                                      qos_user)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos_user = self.cequ_user
        return qos_user.cdqosu_encode(need_status, need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        qos_user = self.cequ_user
        uid = qos_user.cdqosu_uid
        parent = self.ce_parent_entry
        assert isinstance(parent, ClownfishEntryQosUsers)
        lustrefs = parent.cequs_lustrefs
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the QoS configuration of user with "
                      "uid [%s] on file system [%s].", uid, fsname)
        return 0


class ClownfishEntryQosUsers(ClownfishEntry):
    """
    Lustre list has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQosUsers, self).__init__(walk, entry_name,
                                                     parent_entry)
        self.cequs_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        qos = self.cequs_lustrefs.lf_qos
        if name in qos.cdqos_users:
            qos_user = qos.cdqos_users[name]
            return ClownfishEntryQosUser(walk, name, self, qos_user)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        qos = self.cequs_lustrefs.lf_qos

        if need_structure or need_status:
            qos_users = []
            for qos_user in qos.cdqos_users.values():
                qos_users.append(qos_user.cdqosu_encode(need_status,
                                                        need_structure))
            return qos_users
        else:
            return qos.cdqos_users.keys()

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.cequs_lustrefs
        log.cl_stdout("This is the entry for QoS configurations of all users "
                      "on file system [%s].", lustrefs.lf_fsname)
        log.cl_stdout('To list all of the uids of the users, please run [ls] command.')
        return 0


class ClownfishEntryQoS(ClownfishEntry):
    """
    Each Lustre service has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryQoS, self).__init__(walk, entry_name,
                                                parent_entry)
        self.ceqos_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        # pylint: disable=too-many-return-statements
        qos = self.ceqos_lustrefs.lf_qos
        if qos is None:
            return None

        walk = self.ce_walk
        if name == cstr.CSTR_INTERVAL:
            return ClownfishEntryQosInterval(walk, name, self,
                                             self.ceqos_lustrefs)
        elif name == cstr.CSTR_THROTTLED_OSS_RPC_RATE:
            return ClownfishEntryQosTrottledOssRpcRate(walk, name, self,
                                                       self.ceqos_lustrefs)
        elif name == cstr.CSTR_THROTTLED_MDS_RPC_RATE:
            return ClownfishEntryQosTrottledMdsRpcRate(walk, name, self,
                                                       self.ceqos_lustrefs)
        elif name == cstr.CSTR_MBPS_THRESHOLD:
            return ClownfishEntryQosMbpsThreshold(walk, name, self,
                                                  self.ceqos_lustrefs)
        elif name == cstr.CSTR_IOPS_THRESHOLD:
            return ClownfishEntryQoSIopsThreshold(walk, name, self,
                                                  self.ceqos_lustrefs)
        elif name == cstr.CSTR_USERS:
            return ClownfishEntryQosUsers(walk, name, self,
                                          self.ceqos_lustrefs)
        elif name == cstr.CSTR_ESMON_SERVER_HOSTNAME:
            return ClownfishEntryEsmonServerHostname(walk, name, self,
                                                     self.ceqos_lustrefs)
        elif name == cstr.CSTR_ESMON_COLLECT_INTERVAL:
            return ClownfishEntryEsmonCollectInterval(walk, name, self,
                                                      self.ceqos_lustrefs)
        elif name == cstr.CSTR_ENABLED:
            return ClownfishEntryQosEnabled(walk, name, self,
                                            self.ceqos_lustrefs)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        lustrefs = self.ceqos_lustrefs
        qos = lustrefs.lf_qos

        if qos is None:
            return []

        return qos.cdqos_encode(need_status, need_structure)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        lustrefs = self.ceqos_lustrefs
        qos = lustrefs.lf_qos
        fsname = lustrefs.lf_fsname
        log.cl_stdout("This is path for the QoS of file system [%s].",
                      fsname)
        if qos is None:
            log.cl_stdout("QoS of file system [%s] is not configured yet, so "
                          "nothing in this path.", fsname)
        return 0


class ClownfishEntryLustre(ClownfishEntry):
    """
    Each Lustre has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, lustrefs):
        super(ClownfishEntryLustre, self).__init__(walk, entry_name,
                                                   parent_entry)
        self.cel_lustrefs = lustrefs

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        if name == cstr.CSTR_CLIENTS:
            return ClownfishEntryLustreClients(walk, name, self,
                                               self.cel_lustrefs.lf_clients)
        elif name == cstr.CSTR_OSTS:
            return ClownfishEntryLustreServices(walk, name, self,
                                                self.cel_lustrefs.lf_osts)
        elif name == cstr.CSTR_MDTS:
            return ClownfishEntryLustreServices(walk, name, self,
                                                self.cel_lustrefs.lf_mdts)
        elif name == cstr.CSTR_MGS:
            return ClownfishEntryMGS(walk, name, self,
                                     self.cel_lustrefs.lf_mgs)
        elif name == cstr.CSTR_QOS:
            return ClownfishEntryQoS(walk, name, self, self.cel_lustrefs)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.ce_walk.cw_instance
        service_status = instance.ci_service_status
        return self.cel_lustrefs.lf_encode(need_status,
                                           service_status.css_service_status,
                                           need_structure)

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        lustrefs = self.cel_lustrefs
        ret = lustrefs.lf_mount(log)
        if ret:
            log.cl_stderr("failed to mount file system [%s]",
                          lustrefs.lf_fsname)
            return -1
        return 0

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        lustrefs = self.cel_lustrefs
        ret = lustrefs.lf_umount(log)
        if ret:
            log.cl_stderr("failed to umount file system [%s]",
                          lustrefs.lf_fsname)
            return -1
        return 0

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the entry for the Lustre file system [%s].",
                      self.cel_lustrefs.lf_fsname)
        log.cl_stdout('To list the subdirs, please run [ls] command.')
        log.cl_stdout('To show the status of the instance, please run [ls -s] command.')
        log.cl_stdout('To umount the file system, please run [umount] command.')
        log.cl_stdout('To mount the file system, please run [mount] command.')
        return 0


class ClownfishEntryMGS(ClownfishEntryLustreService):
    """
    Each MGS has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry, mgs):
        super(ClownfishEntryMGS, self).__init__(walk, entry_name,
                                                parent_entry, mgs)
        self.celm_mgs = mgs

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        mgs = self.celm_mgs
        ret = mgs.ls_mount(log)
        if ret:
            log.cl_stderr("failed to mount MGS [%s]",
                          mgs.ls_service_name)
            return -1

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        mgs = self.celm_mgs
        ret = mgs.ls_umount(log)
        if ret:
            log.cl_stderr("failed to umount MGS [%s]",
                          mgs.ls_service_name)
            return -1
        return 0

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the entry for the MGS [%s].",
                      self.celm_mgs.ls_service_name)
        log.cl_stdout('To list the instances of this MGS, please run [ls] command.')
        log.cl_stdout('The format of the output of [ls] would have the format of $HOSTNAME-$DEVICE')
        log.cl_stdout('To show the status of the instance, please run [ls -s] command.')
        log.cl_stdout('To umount the MGS, please run [umount] command.')
        log.cl_stdout('To mount the MGS, please run [mount] command.')
        return 0


class ClownfishEntryMGSs(ClownfishEntry):
    """
    MGS list has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry):
        instance = walk.cw_instance
        mgs_dict = instance.ci_mgs_dict
        super(ClownfishEntryMGSs, self).__init__(walk, entry_name,
                                                 parent_entry)
        self.cem_mgs_dict = mgs_dict

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        if name in self.cem_mgs_dict:
            mgs = self.cem_mgs_dict[name]
            return ClownfishEntryMGS(walk, name, self, mgs)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        walk = self.ce_walk
        instance = walk.cw_instance
        service_status = instance.ci_service_status

        if need_structure or need_status:
            encoded = []
            for mgs in self.cem_mgs_dict.values():
                encoded.append(mgs.ls_encode(need_status,
                                             service_status.css_service_status,
                                             need_structure))
            return encoded
        else:
            return self.cem_mgs_dict.keys()

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        return self.ce_walk.cw_instance.ci_mount_mgs(log)

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        return self.ce_walk.cw_instance.ci_umount_mgs(log)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the entry for the all the MGS configured.")
        log.cl_stdout('To list IDs of the MGS, please run [ls] command.')
        log.cl_stdout('To umount all of the MGS, please run [umount] command.')
        log.cl_stdout('To mount all of the MGS, please run [mount] command.')
        return 0


class ClownfishEntryLustres(ClownfishEntry):
    """
    Lustre list has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry):
        instance = walk.cw_instance
        lustres = instance.ci_lustres
        super(ClownfishEntryLustres, self).__init__(walk, entry_name,
                                                    parent_entry)
        self.cel_lustres = lustres

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        if name in self.cel_lustres:
            lustrefs = self.cel_lustres[name]
            return ClownfishEntryLustre(walk, name, self, lustrefs)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        walk = self.ce_walk
        instance = walk.cw_instance
        service_status = instance.ci_service_status

        if need_structure or need_status:
            lustres = []
            for lustrefs in self.cel_lustres.values():
                lustres.append(lustrefs.lf_encode(need_status,
                                                  service_status.css_service_status,
                                                  need_structure))
            return lustres
        else:
            return self.cel_lustres.keys()

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        return self.ce_walk.cw_instance.ci_mount_lustres(log)

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        return self.ce_walk.cw_instance.ci_umount_lustres(log)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the entry for the all the Lustre file systems configured.")
        log.cl_stdout('To list all of the Lustre file system names, please run [ls] command.')
        log.cl_stdout('To umount all of the Lustre file systems, please run [umount] command.')
        log.cl_stdout('To mount all of the Lustre file systems, please run [mount] command.')
        return 0


class ClownfishEntryLazyPrepare(ClownfishEntry):
    """
    Each host has an entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry):
        super(ClownfishEntryLazyPrepare, self).__init__(walk, entry_name,
                                                        parent_entry)

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if need_structure or need_status:
            return {cstr.CSTR_LAZY_PREPARE: self.ce_walk.cw_instance.ci_lazy_prepare}
        else:
            return []

    def ce_command_enable(self, log, args):
        """
        Implementation of enable command
        """
        self.ce_walk.cw_instance.ci_lazy_prepare = True
        log.cl_stdout("enabled lazy prepare")
        return 0

    def ce_command_disable(self, log, args):
        """
        Implementation of disable command
        """
        self.ce_walk.cw_instance.ci_lazy_prepare = False
        log.cl_stdout("disabled lazy prepare")
        return 0

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        manual = """This is the entry for configuring lazy prepare.
If lazy prepare is enabled, reinstallation will be skipped if RPMs are already installed.
To show the current configuration, please run [ls -s] command.
To enable lazy prepare, please run [enable] command.
To disable lazy prepare, please run [disable] command."""
        log.cl_stdout(manual)
        return 0


class ClownfishEntryHighAvailability(ClownfishEntry):
    """
    High availabity entry of this type
    """
    def __init__(self, walk, entry_name, parent_entry):
        super(ClownfishEntryHighAvailability, self).__init__(walk, entry_name,
                                                             parent_entry)

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if need_structure or need_status:
            return {cstr.CSTR_HIGH_AVAILABILITY: self.ce_walk.cw_instance.ci_high_availability}
        else:
            return []

    def ce_command_enable(self, log, args):
        """
        Implementation of enable command
        """
        self.ce_walk.cw_instance.ci_high_availability_enable()
        log.cl_stderr("enabled high availability")
        return 0

    def ce_command_disable(self, log, args):
        """
        Implementation of disable command
        """
        return self.ce_walk.cw_instance.ci_high_availability_disable(log)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        manual = """This is the entry for configuring high availablity.
If high availablity is enabled, Clownfish will try to recover Lustre service automatically.
To show the current configuration, please run [ls -s] command.
To enable high availablity, please run [enable] command.
To disable high availablity, please run [disable] command."""
        log.cl_stdout(manual)
        return 0


class ClownfishEntryRoot(ClownfishEntry):
    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    Each host being used to run Lustre tests has an object of this
    """
    def __init__(self, walk):
        super(ClownfishEntryRoot, self).__init__(walk, "/", None)

    def ce_child(self, name):
        """
        Create and return child entry
        """
        walk = self.ce_walk
        instance = walk.cw_instance
        if name == cstr.CSTR_HOSTS:
            return ClownfishEntryHosts(walk, name, self, instance.ci_hosts)
        elif name == cstr.CSTR_LUSTRES:
            return ClownfishEntryLustres(walk, name, self)
        elif name == cstr.CSTR_MGS_LIST:
            return ClownfishEntryMGSs(walk, name, self)
        elif name == cstr.CSTR_LAZY_PREPARE:
            return ClownfishEntryLazyPrepare(walk, name, self)
        elif name == cstr.CSTR_HIGH_AVAILABILITY:
            return ClownfishEntryHighAvailability(walk, name, self)
        return None

    def ce_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        return self.ce_walk.cw_instance.ci_encode(need_status, need_structure)

    def ce_command_format(self, log, args):
        """
        Implementation of format command
        """
        return self.ce_walk.cw_instance.ci_format_all(log)

    def ce_command_mount(self, log, args):
        """
        Implementation of mount command
        """
        return self.ce_walk.cw_instance.ci_mount_all(log)

    def ce_command_umount(self, log, args):
        """
        Implementation of umount command
        """
        return self.ce_walk.cw_instance.ci_umount_all(log)

    def ce_command_prepare(self, connection, args):
        """
        Implementation of prepare command
        """
        log = connection.cc_command_log
        return self.ce_walk.cw_instance.ci_prepare_all(log, connection.cc_workspace)

    def ce_command_manual(self, log, args):
        """
        Implementation of manual command
        """
        log.cl_stdout("This is the ROOT entry.")
        log.cl_stdout("To prepare the whole cluster, please run [prepare] command.")
        log.cl_stdout("To format the whole cluster, please run [format] command.")
        log.cl_stdout("To mount the whole cluster, please run [mount] command.")
        log.cl_stdout("To umount the whole cluster, please run [umount] command.")
        return 0


def parse_qos_user_config(log, lustre_fs, qos_user_config, config_fpath,
                          qos_users, interval,
                          default_mbps_threshold, default_iops_threshold,
                          default_throttled_oss_rpc_rate,
                          default_throttled_mds_rpc_rate):
    # pylint: disable=too-many-arguments,too-many-locals
    """
    Parse the config for QoS user
    """
    uid = utils.config_value(qos_user_config, cstr.CSTR_UID)
    if uid is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_UID,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1

    uid = str(uid)
    if uid in qos_users:
        log.cl_error("multiple uid [%s] configured for QoS of file system "
                     "[%s], please correct file [%s]",
                     cstr.CSTR_UID,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1

    mbps_threshold = utils.config_value(qos_user_config,
                                        cstr.CSTR_MBPS_THRESHOLD)
    if mbps_threshold is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_MBPS_THRESHOLD,
                     uid,
                     lustre_fs.lf_fsname,
                     default_mbps_threshold)
        mbps_threshold = default_mbps_threshold

    iops_threshold = utils.config_value(qos_user_config,
                                        cstr.CSTR_IOPS_THRESHOLD)
    if iops_threshold is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_IOPS_THRESHOLD,
                     uid,
                     lustre_fs.lf_fsname,
                     default_iops_threshold)
        iops_threshold = default_iops_threshold

    throttled_oss_rpc_rate = utils.config_value(qos_user_config,
                                                cstr.CSTR_THROTTLED_OSS_RPC_RATE)
    if throttled_oss_rpc_rate is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                     uid,
                     lustre_fs.lf_fsname,
                     default_throttled_oss_rpc_rate)
        throttled_oss_rpc_rate = default_throttled_oss_rpc_rate

    throttled_mds_rpc_rate = utils.config_value(qos_user_config,
                                                cstr.CSTR_THROTTLED_MDS_RPC_RATE)
    if throttled_mds_rpc_rate is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_THROTTLED_MDS_RPC_RATE,
                     uid,
                     lustre_fs.lf_fsname,
                     default_throttled_oss_rpc_rate)
        throttled_mds_rpc_rate = default_throttled_mds_rpc_rate

    qos_user = clownfish_qos.ClownfishDecayQoSUser(uid, interval,
                                                   mbps_threshold,
                                                   throttled_oss_rpc_rate,
                                                   iops_threshold,
                                                   throttled_mds_rpc_rate)
    qos_users[uid] = qos_user
    return 0


def parse_qos_config(log, lustre_fs, lustre_config, config_fpath, workspace):
    """
    Parse the config for QoS
    """
    # pylint: disable=too-many-locals,too-many-branches
    qos_config = utils.config_value(lustre_config, cstr.CSTR_QOS)
    if qos_config is None:
        log.cl_info("no [%s] is configured for file system [%s], no QoS "
                    "control for that file system",
                    cstr.CSTR_QOS, lustre_fs.lf_fsname)
        return 0, None

    esmon_server_hostname = utils.config_value(qos_config,
                                               cstr.CSTR_ESMON_SERVER_HOSTNAME)
    if esmon_server_hostname is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ESMON_SERVER_HOSTNAME,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_enabled = utils.config_value(qos_config, cstr.CSTR_ENABLED)
    if qos_enabled is None:
        log.cl_error("no [%s] is configured for for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ENABLED,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_interval = utils.config_value(qos_config,
                                      cstr.CSTR_INTERVAL)
    if qos_interval is None:
        log.cl_error("no [%s] is configured for for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_INTERVAL,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_iops_threshold = utils.config_value(qos_config,
                                            cstr.CSTR_IOPS_THRESHOLD)
    if qos_iops_threshold is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_IOPS_THRESHOLD, lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_mbps_threshold = utils.config_value(qos_config,
                                            cstr.CSTR_MBPS_THRESHOLD)
    if qos_mbps_threshold is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_MBPS_THRESHOLD, lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_throttled_oss_rpc_rate = utils.config_value(qos_config,
                                                    cstr.CSTR_THROTTLED_OSS_RPC_RATE)
    if qos_throttled_oss_rpc_rate is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_throttled_mds_rpc_rate = utils.config_value(qos_config,
                                                    cstr.CSTR_THROTTLED_MDS_RPC_RATE)
    if qos_throttled_mds_rpc_rate is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_THROTTLED_MDS_RPC_RATE,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_users = {}
    qos_user_configs = utils.config_value(qos_config,
                                          cstr.CSTR_USERS)
    if qos_user_configs is None:
        log.cl_info("no [%s] is configured for QoS of file system [%s]",
                    cstr.CSTR_USERS,
                    lustre_fs.lf_fsname)
        qos_user_configs = []

    for qos_user_config in qos_user_configs:
        ret = parse_qos_user_config(log, lustre_fs, qos_user_config,
                                    config_fpath,
                                    qos_users, qos_interval,
                                    qos_mbps_threshold,
                                    qos_iops_threshold,
                                    qos_throttled_oss_rpc_rate,
                                    qos_throttled_mds_rpc_rate)
        if ret:
            return -1, None
    esmon_collect_interval = utils.config_value(qos_config,
                                                cstr.CSTR_ESMON_COLLECT_INTERVAL)
    if esmon_collect_interval is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ESMON_COLLECT_INTERVAL,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos = clownfish_qos.ClownfishDecayQoS(log, lustre_fs,
                                          esmon_server_hostname,
                                          qos_interval,
                                          qos_mbps_threshold,
                                          qos_throttled_oss_rpc_rate,
                                          qos_iops_threshold,
                                          qos_throttled_mds_rpc_rate,
                                          esmon_collect_interval,
                                          qos_users, qos_enabled, workspace)
    return 0, qos


def init_instance(log, workspace, config, config_fpath, no_operation=False):
    """
    Parse the config and init the instance
    """
    # pylint: disable=too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    lazy_prepare = utils.config_value(config, cstr.CSTR_LAZY_PREPARE)
    if lazy_prepare is None:
        lazy_prepare = False
        log.cl_info("no [%s] is configured, using default value false",
                    cstr.CSTR_LAZY_PREPARE)

    if lazy_prepare:
        lazy_prepare_string = "enabled"
    else:
        lazy_prepare_string = "disabled"

    log.cl_info("lazy prepare is %s", lazy_prepare_string)

    high_availability = utils.config_value(config,
                                           cstr.CSTR_HIGH_AVAILABILITY)
    if high_availability is None:
        high_availability = False
        log.cl_info("no [%s] is configured, using default value false",
                    cstr.CSTR_HIGH_AVAILABILITY)

    if high_availability:
        high_availability_string = "enabled"
    else:
        high_availability_string = "disabled"
    log.cl_info("high availability is %s", high_availability_string)

    dist_configs = utils.config_value(config, cstr.CSTR_LUSTRE_DISTRIBUTIONS)
    if dist_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_LUSTRE_DISTRIBUTIONS, config_fpath)
        return None

    # Keys are the distribution IDs, values are LustreRPMs
    lustre_distributions = {}
    for dist_config in dist_configs:
        lustre_distribution_id = utils.config_value(dist_config,
                                                    cstr.CSTR_LUSTRE_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_DISTRIBUTION_ID, config_fpath)
            return None

        if lustre_distribution_id in lustre_distributions:
            log.cl_error("multiple distributions with ID [%s] is "
                         "configured, please correct file [%s]",
                         lustre_distribution_id, config_fpath)
            return None

        lustre_rpm_dir = utils.config_value(dist_config,
                                            cstr.CSTR_LUSTRE_RPM_DIR)
        if lustre_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_RPM_DIR, config_fpath)
            return None

        lustre_rpm_dir = lustre_rpm_dir.rstrip("/")

        e2fsprogs_rpm_dir = utils.config_value(dist_config,
                                               cstr.CSTR_E2FSPROGS_RPM_DIR)
        if e2fsprogs_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_E2FSPROGS_RPM_DIR, config_fpath)
            return None

        e2fsprogs_rpm_dir = e2fsprogs_rpm_dir.rstrip("/")

        lustre_rpms = lustre.LustreRPMs(lustre_distribution_id,
                                        lustre_rpm_dir, e2fsprogs_rpm_dir)
        ret = lustre_rpms.lr_prepare(log)
        if ret:
            log.cl_error("failed to prepare Lustre RPMs")
            return None

        lustre_distributions[lustre_distribution_id] = lustre_rpms

    ssh_host_configs = utils.config_value(config, cstr.CSTR_SSH_HOSTS)
    if ssh_host_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_SSH_HOSTS, config_fpath)
        return None

    hosts = {}
    for host_config in ssh_host_configs:
        host_id = utils.config_value(host_config,
                                     cstr.CSTR_HOST_ID)
        if host_id is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_HOST_ID, config_fpath)
            return None

        if host_id in hosts:
            log.cl_error("multiple SSH hosts with the same ID [%s], please "
                         "correct file [%s]", host_id, config_fpath)
            return None

        lustre_distribution_id = utils.config_value(host_config,
                                                    cstr.CSTR_LUSTRE_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_DISTRIBUTION_ID, config_fpath)
            return None

        if lustre_distribution_id not in lustre_distributions:
            log.cl_error("no Lustre distributions with ID [%s] is "
                         "configured, please correct file [%s]",
                         lustre_distribution_id, config_fpath)
            return None

        lustre_distribution = lustre_distributions[lustre_distribution_id]

        hostname = utils.config_value(host_config, cstr.CSTR_HOSTNAME)
        if hostname is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "with ID [%s], please correct file [%s]",
                         cstr.CSTR_HOSTNAME, host_id, config_fpath)
            return None

        ssh_identity_file = utils.config_value(host_config, cstr.CSTR_SSH_IDENTITY_FILE)

        host = lustre.LustreServerHost(hostname,
                                       lustre_rpms=lustre_distribution,
                                       identity_file=ssh_identity_file,
                                       host_id=host_id)
        hosts[host_id] = host

    lustre_configs = utils.config_value(config, cstr.CSTR_LUSTRES)
    if lustre_configs is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_LUSTRES, config_fpath)
        return None

    mgs_configs = utils.config_value(config, cstr.CSTR_MGS_LIST)
    if mgs_configs is None:
        log.cl_debug("no [%s] is configured", cstr.CSTR_MGS_LIST)
        mgs_configs = []

    server_hosts = {}
    mgs_dict = {}
    for mgs_config in mgs_configs:
        # Parse MGS configs
        mgs_id = utils.config_value(mgs_config, cstr.CSTR_MGS_ID)
        if mgs_id is None:
            log.cl_error("no [%s] is configured for a MGS, please correct "
                         "file [%s]",
                         cstr.CSTR_MGS_ID, config_fpath)
            return None

        if mgs_id in mgs_dict:
            log.cl_error("multiple configurations for MGS [%s], please "
                         "correct file [%s]",
                         mgs_id, config_fpath)
            return None

        backfstype = utils.config_value(mgs_config, cstr.CSTR_BACKFSTYPE)
        if backfstype is None:
            log.cl_debug("no [%s] is configured for MGS [%s], using [%s] as "
                         "default value", cstr.CSTR_BACKFSTYPE, mgs_id,
                         lustre.BACKFSTYPE_LDISKFS)
            backfstype = lustre.BACKFSTYPE_LDISKFS

        mgs = lustre.LustreMGS(log, mgs_id, backfstype)
        mgs_dict[mgs_id] = mgs

        instance_configs = utils.config_value(mgs_config, cstr.CSTR_INSTANCES)
        if instance_configs is None:
            log.cl_error("no [%s] is configured for MGS [%s], please correct "
                         "file [%s]",
                         cstr.CSTR_INSTANCES, mgs_id, config_fpath)
            return None

        for instance_config in instance_configs:
            host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
            if host_id is None:
                log.cl_error("no [%s] is configured for instance of MGS "
                             "[%s], please correct file [%s]",
                             cstr.CSTR_HOST_ID, mgs_id, config_fpath)
                return None

            if host_id not in hosts:
                log.cl_error("no host with [%s] is configured in hosts, "
                             "please correct file [%s]",
                             host_id, config_fpath)
                return None

            device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
            if device is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             cstr.CSTR_DEVICE, mgs_id, config_fpath)
                return None

            if backfstype == lustre.BACKFSTYPE_ZFS:
                if device.startswith("/"):
                    log.cl_error("device [%s] with absolute path is "
                                 "configured for instance of MGS [%s] "
                                 "with ZFS type, please correct file [%s]",
                                 cstr.CSTR_DEVICE, mgs_id, config_fpath)
                    return None
            else:
                if not device.startswith("/"):
                    log.cl_error("device [%s] with absolute path should be "
                                 "configured for instance of MGS [%s] with "
                                 "ldiskfs type, please correct file [%s]",
                                 cstr.CSTR_DEVICE, mgs_id, config_fpath)
                    return None

            nid = utils.config_value(instance_config, cstr.CSTR_NID)
            if nid is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             cstr.CSTR_NID, mgs_id, config_fpath)
                return None

            zpool_create = None
            if backfstype == lustre.BACKFSTYPE_ZFS:
                zpool_create = utils.config_value(instance_config,
                                                  cstr.CSTR_ZPOOL_CREATE)
                if zpool_create is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MGS [%s], please correct file [%s]",
                                 cstr.CSTR_ZPOOL_CREATE, mgs_id, config_fpath)
                    return None

            lustre_host = hosts[host_id]
            if host_id not in server_hosts:
                server_hosts[host_id] = lustre_host

            mnt = "/mnt/mgs_%s" % (mgs_id)
            lustre.LustreMGSInstance(log, mgs, lustre_host, device, mnt,
                                     nid, add_to_host=True)

    lustres = {}
    qos_dict = {}
    for lustre_config in lustre_configs:
        # Parse general configs of Lustre file system
        fsname = utils.config_value(lustre_config, cstr.CSTR_FSNAME)
        if fsname is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_FSNAME, config_fpath)
            return None

        if fsname in lustres:
            log.cl_error("file system [%s] is configured for multiple times, "
                         "please correct file [%s]",
                         fsname, config_fpath)
            return None

        lustre_fs = lustre.LustreFilesystem(fsname)
        lustres[fsname] = lustre_fs

        mgs_configured = False

        # Parse MGS config
        mgs_id = utils.config_value(lustre_config, cstr.CSTR_MGS_ID)
        if mgs_id is not None:
            log.cl_debug("[%s] is configured for file system [%s]",
                         cstr.CSTR_MGS_ID, fsname)

            if mgs_id not in mgs_dict:
                log.cl_error("no MGS with ID [%s] is configured, please "
                             "correct file [%s]",
                             mgs_id, config_fpath)
                return None

            mgs = mgs_dict[mgs_id]

            ret = mgs.lmgs_add_fs(log, lustre_fs)
            if ret:
                log.cl_error("failed to add file system [%s] to MGS [%s], "
                             "please correct file [%s]",
                             fsname, mgs_id, config_fpath)
                return None

            mgs_configured = True

        # Parse MDT configs
        mdt_configs = utils.config_value(lustre_config, cstr.CSTR_MDTS)
        if mdt_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_MDTS, config_fpath)
            return None

        for mdt_config in mdt_configs:
            mdt_index = utils.config_value(mdt_config, cstr.CSTR_INDEX)
            if mdt_index is None:
                log.cl_error("no [%s] is configured for a MDT of file system "
                             "[%s], please correct file [%s]",
                             cstr.CSTR_INDEX, fsname, config_fpath)
                return None

            is_mgs = utils.config_value(mdt_config, cstr.CSTR_IS_MGS)
            if is_mgs is None:
                log.cl_error("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using default value [False]",
                             cstr.CSTR_IS_MGS, mdt_index, fsname)
                is_mgs = False

            if is_mgs:
                if mgs_configured:
                    log.cl_error("multiple MGS are configured for file "
                                 "system [%s], please correct file [%s]",
                                 fsname, config_fpath)
                    return None
                mgs_configured = True

            backfstype = utils.config_value(mdt_config, cstr.CSTR_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using [%s] as the default "
                             "value", cstr.CSTR_BACKFSTYPE, mdt_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            mdt = lustre.LustreMDT(log, lustre_fs, mdt_index, backfstype,
                                   is_mgs=is_mgs)

            instance_configs = utils.config_value(mdt_config, cstr.CSTR_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_INSTANCES, config_fpath)
                return None

            for instance_config in instance_configs:
                host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
                if host_id is None:
                    log.cl_error("no [%s] is configured, please correct file [%s]",
                                 cstr.CSTR_HOST_ID, config_fpath)
                    return None

                if host_id not in hosts:
                    log.cl_error("no host with [%s] is configured in hosts, "
                                 "please correct file [%s]",
                                 host_id, config_fpath)
                    return None

                device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_DEVICE, mdt_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of MDT "
                                     "with index [%s] of file system [%s] "
                                     "with ZFS type, please correct file [%s]",
                                     cstr.CSTR_DEVICE, mdt_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of MDT "
                                     "with index [%s] of file system [%s] "
                                     "with ldiskfs type, please correct file "
                                     "[%s]",
                                     cstr.CSTR_DEVICE, mdt_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, cstr.CSTR_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_NID, mdt_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config,
                                                      cstr.CSTR_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "MDT with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     cstr.CSTR_ZPOOL_CREATE, mdt_index, fsname,
                                     config_fpath)
                        return None

                lustre_host = hosts[host_id]
                if host_id not in server_hosts:
                    server_hosts[host_id] = lustre_host

                mnt = "/mnt/%s_mdt_%s" % (fsname, mdt_index)
                lustre.LustreMDTInstance(log, mdt, lustre_host, device, mnt,
                                         nid, add_to_host=True,
                                         zpool_create=zpool_create)

        if not mgs_configured:
            log.cl_error("None MGS is configured, please correct file [%s]",
                         config_fpath)
            return None

        # Parse OST configs
        ost_configs = utils.config_value(lustre_config, cstr.CSTR_OSTS)
        if ost_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_OSTS, config_fpath)
            return None

        for ost_config in ost_configs:
            ost_index = utils.config_value(ost_config, cstr.CSTR_INDEX)
            if ost_index is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_INDEX, config_fpath)
                return None

            backfstype = utils.config_value(ost_config, cstr.CSTR_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], using [%s] as default",
                             cstr.CSTR_BACKFSTYPE, ost_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            ost = lustre.LustreOST(log, lustre_fs, ost_index, backfstype)

            instance_configs = utils.config_value(ost_config, cstr.CSTR_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], please correct file [%s]",
                             cstr.CSTR_INSTANCES, ost_index, fsname,
                             config_fpath)
                return None

            for instance_config in instance_configs:
                host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
                if host_id is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_HOST_ID, ost_index, fsname,
                                 config_fpath)
                    return None

                if host_id not in hosts:
                    log.cl_error("no host with ID [%s] is configured in hosts, "
                                 "please correct file [%s]",
                                 host_id, config_fpath)
                    return None

                device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_DEVICE, ost_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of OST "
                                     "with index [%s] of file system [%s] "
                                     "with ZFS type, please correct file [%s]",
                                     cstr.CSTR_DEVICE, ost_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] with none-absolute path is "
                                     "configured for an instance of OST "
                                     "with index [%s] of file system [%s] "
                                     "with ldiskfs type, please correct file "
                                     "[%s]",
                                     cstr.CSTR_DEVICE, ost_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, cstr.CSTR_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_NID, ost_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config, cstr.CSTR_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "OST with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     cstr.CSTR_ZPOOL_CREATE, mdt_index, fsname,
                                     config_fpath)
                        return None

                lustre_host = hosts[host_id]
                if host_id not in server_hosts:
                    server_hosts[host_id] = lustre_host

                mnt = "/mnt/%s_ost_%s" % (fsname, ost_index)
                lustre.LustreOSTInstance(log, ost, lustre_host, device, mnt,
                                         nid, add_to_host=True,
                                         zpool_create=zpool_create)
        # Parse client configs
        client_configs = utils.config_value(lustre_config,
                                            cstr.CSTR_CLIENTS)
        if client_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_CLIENTS, config_fpath)
            return None

        for client_config in client_configs:
            host_id = utils.config_value(client_config, cstr.CSTR_HOST_ID)
            if host_id is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_HOST_ID, config_fpath)
                return None

            if host_id not in hosts:
                log.cl_error("no host with [%s] is configured in hosts, "
                             "please correct file [%s]",
                             host_id, config_fpath)
                return None

            lustre_host = hosts[host_id]

            mnt = utils.config_value(client_config, cstr.CSTR_MNT)
            if mnt is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_MNT, config_fpath)
                return None

            lustre.LustreClient(log, lustre_fs, lustre_host, mnt, add_to_host=True)

        # No operation means this instance should not do any operation.
        # QoS won't be used.
        if no_operation:
            continue

        ret, qos = parse_qos_config(log, lustre_fs, lustre_config,
                                    config_fpath, workspace)
        if ret:
            log.cl_error("failed to parse QoS for file system [%s]",
                         lustre_fs.lf_fsname)
            return None

        if qos is None:
            continue
        qos_dict[lustre_fs.lf_fsname] = qos

    return ClownfishInstance(log, workspace, lazy_prepare, hosts, mgs_dict, lustres,
                             high_availability, qos_dict, no_operation=no_operation)


def clownfish_entry_path(obj):
    """
    Return the default entry path of a instance.
    """
    if isinstance(obj, lustre.LustreFilesystem):
        return "/" + cstr.CSTR_LUSTRES + "/" + obj.lf_fsname
    elif isinstance(obj, lustre.LustreClient):
        # Two paths to a client:
        # 1. lustres/$fsname/clients/$client_name
        # 2. hosts/$hostname/clients/$fsname:$mnt
        # return the first one
        return (clownfish_entry_path(obj.lc_lustre_fs) + "/" +
                cstr.CSTR_CLIENTS + "/" +
                clownfish_entry_escape(obj.lc_client_name))
    elif isinstance(obj, lustre.LustreMDT):
        # Two paths:
        # 1. lustres/$fsname/mdts/$service_name
        # 2. hosts/$hostname/mdts/$service_name
        # return the first one
        return (clownfish_entry_path(obj.ls_lustre_fs) + "/" +
                cstr.CSTR_MDTS + "/" + obj.ls_service_name)
    elif isinstance(obj, lustre.LustreOST):
        # Two paths:
        # 1. lustres/$fsname/osts/$service_name
        # 2. hosts/$hostname/osts/$service_name
        # return the first one
        return (clownfish_entry_path(obj.ls_lustre_fs) + "/" +
                cstr.CSTR_OSTS + "/" + obj.ls_service_name)
    elif isinstance(obj, lustre.LustreMGS):
        # Two paths:
        # 1. lustres/$fsname/mgs
        # 2. mgs_list/$mgs_id
        # return the first one
        return (clownfish_entry_path(obj.ls_lustre_fs) + "/" +
                cstr.CSTR_MGS)
    elif isinstance(obj, lustre.LustreServiceInstance):
        # Two paths for LustreOSTInstance and LustreMDTInstance
        # 1. lustres/$fsname/[osts|mdts]/$service_name/$service_instance_name
        # 2. hosts/$hostname/[osts|mdts]/$service_name/$service_instance_name
        #
        # Three patchs for LustreMGSInstance:
        # 1. lustres/$fsname/mgs/$service_instance_name
        # 2. mgs_list/$mgs_id/$service_instance_name
        # 3. hosts/$hostname/mgs
        # return the first one
        return (clownfish_entry_path(obj.lsi_service) + "/" +
                clownfish_entry_escape(obj.lsi_service_instance_name))
    reason = ("not able to get clownfish entry path for object [%s]" %
              type(obj))
    raise Exception(reason)
