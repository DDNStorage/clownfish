# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Test Library for clownfish
Clownfish is an automatic management system for Lustre
"""
# pylint: disable=too-many-lines
import traceback
import os
import time
import yaml

# Local libs
from pylcommon import utils
from pylcommon import cstr
from pylcommon import cmd_general
from pylcommon import ssh_host
from pylcommon import test_common
from pylcommon import constants
from pyclownfish import clownfish_console
from pyclownfish import clownfish
from pyclownfish import clownfish_install_nodeps

COMMAND_ABORT_TIMEOUT = 10
CLOWNFISH_TESTS = []


def run_commands(log, cclient, cmds):
    """
    Run a list of commands, if exit status is none zero, return failure
    """
    result = log.cl_result
    for command in cmds:
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1
    return 0


def path_tests(log, workspace, cclient):
    """
    Tests of path
    """
    # pylint: disable=unused-argument,too-many-return-statements
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_CD + " /"
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_PWD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    elif result.cr_stdout != "/\n":
        log.cl_error("unexpected pwd [%s]", result.cr_stdout)
        return -1

    # Repeated / is allowed
    command = clownfish.CLOWNFISH_COMMNAD_CD + " ////"
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    # Escaped / is not /
    command = clownfish.CLOWNFISH_COMMNAD_CD + r" \/"
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0:
        log.cl_error("unexpected success of command [%s]", command)
        return -1
    elif result.cr_stderr == "":
        log.cl_error("failed command [%s] should have error message", command)
        return -1

    # \ is not allowed if not before /
    command = clownfish.CLOWNFISH_COMMNAD_CD + " \\"
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0:
        log.cl_error("unexpected success of command [%s]", command)
        return -1
    elif result.cr_stderr == "":
        log.cl_error("failed command [%s] should have error message", command)
        return -1
    return 0

CLOWNFISH_TESTS.append(path_tests)


def delimiter_tests(log, workspace, cclient):
    """
    Tests of AND and OR
    """
    # pylint: disable=unused-argument,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    result = log.cl_result
    # tailing AND is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_AND)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND AND is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading AND is not allowed
    command = (clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND after failed command will quit
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout != ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND after succeeded command will execute
    command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr != "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND: failure after succeeded command will return failure
    command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND: failure after succeeded commands will return failure
    command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stdout == "" or
            result.cr_stderr == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # tailing OR is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_OR)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading OR is not allowed
    command = (clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR OR is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success after failed command will return success
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_CD)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success after failed commands will return success
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_CD)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success before OR will return sucess
    command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr != "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure OR SUCESS AND sucess -> success
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_COMMNAD_CD + " " +
               clownfish.CLOWNFISH_DELIMITER_AND + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr == ""
            or result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # tailing GOON is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading CONT is not allowed
    command = (clownfish.CLOWNFISH_DELIMITER_OR + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # CONT CONT is not allowed
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure CONT success -> succeess
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr == "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # success CONT failure -> failure
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure CONT failure -> failure
    command = (clownfish.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout != ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # success CONT success -> success
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " " +
               clownfish.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish.CLOWNFISH_COMMNAD_LS)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr != "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1
    return 0


CLOWNFISH_TESTS.append(delimiter_tests)


def nonexistent_command(log, workspace, cclient):
    # pylint: disable=unused-argument,too-many-return-statements
    """
    Nonexistent command should return failure
    """
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_NONEXISTENT
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0:
        log.cl_error("nonexistent command [%s] succeeded unexpectly", command)
        return -1
    elif result.cr_stderr == "":
        log.cl_error("nonexistent command [%s] has no error output", command)
        return -1

    return 0

CLOWNFISH_TESTS.append(nonexistent_command)


def pwd_manual_ls_cd(log, workspace, cclient):
    # pylint: disable=unused-argument,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    """
    cd to all subdirs, and run ls/status commands
    """
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_PWD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    current_path = result.cr_stdout.strip()
    log.cl_info("PWD: %s", current_path)

    # cd ../../... until to the root
    level = clownfish.clownfish_path_level(current_path)
    command = clownfish.CLOWNFISH_COMMNAD_CD + " "
    while level > 0:
        command += "../"
        level -= 1
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    # Check whether cd to the root
    command = clownfish.CLOWNFISH_COMMNAD_PWD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    elif result.cr_stdout != "/\n":
        log.cl_error("failed to change to root from path [%s] by command "
                     "[%s]", current_path, command)
        return -1

    # cd to using absolute path
    command = clownfish.CLOWNFISH_COMMNAD_CD + " " + current_path
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    # Check the pwd
    command = clownfish.CLOWNFISH_COMMNAD_PWD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    elif result.cr_stdout != current_path + "\n":
        log.cl_error("failed to change to [%s] from root by command "
                     "[%s], current path [%s]", current_path, command,
                     result.cr_stdout.strip())
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_MANUAL
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = (clownfish.CLOWNFISH_COMMNAD_LS + " -" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = (clownfish.CLOWNFISH_COMMNAD_LS + " --" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_LONG_RECURSIVE)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = (clownfish.CLOWNFISH_COMMNAD_LS + " -" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = (clownfish.CLOWNFISH_COMMNAD_LS + " --" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_LONG_STATUS)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = (clownfish.CLOWNFISH_COMMNAD_LS + " -" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS +
               " -" + clownfish.CLOWNFISH_COMMNAD_LS_OPTION_SHORT_RECURSIVE)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_LS
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    subdirs = result.cr_stdout.splitlines()
    for subdir in subdirs:
        command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
                   clownfish.clownfish_entry_escape(subdir))
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1

        ret = pwd_manual_ls_cd(log, workspace, cclient)
        if ret:
            return ret

        command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1
    return 0

CLOWNFISH_TESTS.append(pwd_manual_ls_cd)


def umount_prepare_format_mount_umount_mount_format(log, workspace, cclient):
    """
    prepare the hosts, mount file systems, umount file system
    """
    # pylint: disable=invalid-name,unused-argument
    cmds = []

    # Change to Root
    cmds.append(clownfish.CLOWNFISH_COMMNAD_CD)

    # Change to lazy_prepare and enable it
    cmds.append(clownfish.CLOWNFISH_COMMNAD_CD + " " + cstr.CSTR_LAZY_PREPARE)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_ENABLE)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_CD + " ..")

    cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_PREPARE)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_FORMAT)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_MOUNT)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_MOUNT)
    # This tests that format will umount automatically
    cmds.append(clownfish.CLOWNFISH_COMMNAD_FORMAT)

    ret = run_commands(log, cclient, cmds)
    if ret:
        log.cl_error("failed to do umount_prepare_format_mount_umount_mount_format")
        return -1
    return 0


CLOWNFISH_TESTS.append(umount_prepare_format_mount_umount_mount_format)


def umount_x2_mount_x2_umount_x2(log, workspace, cclient):
    """
    1) umount twice, 2) mount twice and 3) umount twice
    Umount or mount twice makes sure the commands can be run for multiple times
    """
    # pylint: disable=invalid-name,unused-argument
    result = log.cl_result
    cmds = []

    # Change to Root
    command = clownfish.CLOWNFISH_COMMNAD_CD + " /" + cstr.CSTR_LUSTRES
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_LS
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    lustres = result.cr_stdout.splitlines()
    for fsname in lustres:
        cmds = []
        cmds.append(clownfish.CLOWNFISH_COMMNAD_CD + " " + fsname)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_MOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_MOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_UMOUNT)
        cmds.append(clownfish.CLOWNFISH_COMMNAD_CD + " ..")
        ret = run_commands(log, cclient, cmds)
        if ret:
            log.cl_error("failed to do umount_x2_mount_x2_umount_x2 to file"
                         "system [%s]", fsname)
            return -1
    return 0

CLOWNFISH_TESTS.append(umount_x2_mount_x2_umount_x2)


def service_mount_check(log, cclient, service_name, expect_status):
    """
    Make sure the service is mounted.
    This function assume it is already under the service path.
    """
    # pylint: disable=bare-except
    result = log.cl_result
    command = (clownfish.CLOWNFISH_COMMNAD_LS + " -" +
               clownfish.CLOWNFISH_COMMNAD_LS_OPTION_SHORT_STATUS)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    output_string = result.cr_stdout
    try:
        output = yaml.load(output_string)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s", output_string,
                     traceback.format_exc())
        return -1

    mounted_status = output[cstr.CSTR_IS_MOUNTED]
    if mounted_status != expect_status:
        log.cl_error("The [%s] status of service [%s] is [%s], expected [%s]",
                     cstr.CSTR_IS_MOUNTED, service_name, mounted_status,
                     expect_status)
        return -1

    return 0


def all_lustre_mgs_run(log, cclient, funct, args):
    """
    Run function on all MGS
    """
    # Change to directory of Lustre MGS list
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_CD + " /" + cstr.CSTR_MGS_LIST
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_LS
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    mgs_list = result.cr_stdout.splitlines()
    for mgs_name in mgs_list:
        command = clownfish.CLOWNFISH_COMMNAD_CD + " " + mgs_name
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1

        ret = funct(log, cclient, mgs_name, args)
        if ret:
            log.cl_error("failed to run on MGS [%s]",
                         mgs_name)
            return ret

        command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1
    return 0


def all_mgs_mount_check(log, cclient):
    """
    Make sure all the MGS is mounted
    """
    return all_lustre_mgs_run(log, cclient, service_mount_check,
                              (cstr.CSTR_TRUE))


def all_lustre_service_run(log, cclient, funct, args):
    """
    Run function on all Lustre service, not including MGS
    """
    # pylint: disable=too-many-return-statements,too-many-branches
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_CD + " /" + cstr.CSTR_LUSTRES
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_LS
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    lustres = result.cr_stdout.splitlines()
    for lustre in lustres:
        command = clownfish.CLOWNFISH_COMMNAD_CD + " " + lustre
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1

        for subset in [cstr.CSTR_OSTS, cstr.CSTR_MDTS]:
            command = clownfish.CLOWNFISH_COMMNAD_CD + " " + subset
            cclient.cc_command(log, command)
            if result.cr_exit_status:
                log.cl_error("failed to run command [%s]", command)
                return -1

            command = clownfish.CLOWNFISH_COMMNAD_LS
            cclient.cc_command(log, command)
            if result.cr_exit_status:
                log.cl_error("failed to run command [%s]", command)
                return -1

            services = result.cr_stdout.splitlines()
            for service in services:
                command = clownfish.CLOWNFISH_COMMNAD_CD + " " + service
                cclient.cc_command(log, command)
                if result.cr_exit_status:
                    log.cl_error("failed to run command [%s]", command)
                    return -1

                ret = funct(log, cclient, service, args)
                if ret:
                    log.cl_error("failed to run on service [%s] of "
                                 "file system [%s]", service, lustre)
                    return ret

                command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
                cclient.cc_command(log, command)
                if result.cr_exit_status:
                    log.cl_error("failed to run command [%s]", command)
                    return -1

            command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
            cclient.cc_command(log, command)
            if result.cr_exit_status:
                log.cl_error("failed to run command [%s]", command)
                return -1

        command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            return -1

    command = clownfish.CLOWNFISH_COMMNAD_CD + " .."
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    return 0


def all_lustre_service_mount_check(log, cclient):
    """
    Make sure all the Lustre file systems is mounted, not including MGS
    """
    return all_lustre_service_run(log, cclient, service_mount_check,
                                  (cstr.CSTR_TRUE))


def ha_enable_or_disable(log, cclient, enable=True):
    """
    Enable or disable HA
    """
    result = log.cl_result

    command = clownfish.CLOWNFISH_COMMNAD_PWD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    former_pwd = result.cr_stdout.strip()

    command = (clownfish.CLOWNFISH_COMMNAD_CD + " /" +
               cstr.CSTR_HIGH_AVAILABILITY)
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    if enable:
        command = clownfish.CLOWNFISH_COMMNAD_ENABLE
    else:
        command = clownfish.CLOWNFISH_COMMNAD_DISABLE
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    command = clownfish.CLOWNFISH_COMMNAD_CD + " " + former_pwd
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    return 0


def ha_enable(log, cclient):
    """
    Enable HA
    """
    return ha_enable_or_disable(log, cclient, enable=True)


def ha_disable(log, cclient):
    """
    Disable HA
    """
    return ha_enable_or_disable(log, cclient, enable=False)


def check_service_ha(log, cclient, service_name, args):
    """
    Check the service HA works well.
    This function assume it is already under the service path.
    """
    # pylint: disable=too-many-return-statements,unused-argument
    result = log.cl_result

    command = "umount"
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    # The status check might delay, so wait here
    ret = utils.wait_condition(log, service_mount_check,
                               (cclient, service_name,
                                cstr.CSTR_FALSE))
    if ret:
        log.cl_error("timeout when waiting the status of service "
                     "[%s]", service_name)
        return ret

    ret = ha_enable(log, cclient)
    if ret:
        log.cl_error("failed to enable HA")
        return ret

    ret = utils.wait_condition(log, service_mount_check,
                               (cclient, service_name, cstr.CSTR_TRUE))
    if ret:
        log.cl_error("timeout when waiting the status of service "
                     "[%s]", service_name)
        return ret

    ret = ha_disable(log, cclient)
    if ret:
        log.cl_error("failed to disable HA")
        return ret
    return 0


def all_mgs_check_ha(log, cclient):
    """
    Make sure all the MGS is mounted
    """
    return all_lustre_mgs_run(log, cclient, check_service_ha,
                              ())


def all_lustre_service_check_ha(log, cclient):
    """
    Check HA works well for all Lustre services, not including MGS
    """
    return all_lustre_service_run(log, cclient, check_service_ha,
                                  ())


def ha_fix_umount(log, workspace, cclient):
    """
    HA should be able to fix problem after umount
    """
    # pylint: disable=too-many-return-statements,unused-argument
    cmds = []

    # Change to Root
    cmds.append(clownfish.CLOWNFISH_COMMNAD_CD)
    cmds.append(clownfish.CLOWNFISH_COMMNAD_MOUNT)
    ret = run_commands(log, cclient, cmds)
    if ret:
        log.cl_error("failed to run commands")
        return -1

    ret = ha_disable(log, cclient)
    if ret:
        log.cl_error("failed to disable HA")
        return -1

    ret = all_mgs_mount_check(log, cclient)
    if ret:
        log.cl_error("failed to check mount status of all MGS")
        return -1

    ret = all_lustre_service_mount_check(log, cclient)
    if ret:
        log.cl_error("failed to check mount status of all Lustre file systems")
        return -1

    ret = all_mgs_check_ha(log, cclient)
    if ret:
        log.cl_error("failed to check HA of all MGS")
        return -1

    ret = all_lustre_service_check_ha(log, cclient)
    if ret:
        log.cl_error("failed to check HA of all Lustre services")
        return -1
    return 0


CLOWNFISH_TESTS.append(ha_fix_umount)


def abort_command(log, workspace, cclient):
    """
    Test of abort a command
    """
    # pylint: disable=unused-argument
    # Change to ROOT directory
    result = log.cl_result
    command = clownfish.CLOWNFISH_COMMNAD_CD
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    cclient.cc_abort_event.set()
    for ccommand in clownfish.CLOWNFISH_SERVER_COMMNADS.values():
        command = ccommand.cc_command

        if (command == clownfish.CLOWNFISH_COMMNAD_DISABLE or
                command == clownfish.CLOWNFISH_COMMNAD_ENABLE):
            cd_command = (clownfish.CLOWNFISH_COMMNAD_CD + " " +
                          cstr.CSTR_HIGH_AVAILABILITY)
        elif command == clownfish.CLOWNFISH_COMMNAD_QUIT:
            continue
        else:
            cd_command = clownfish.CLOWNFISH_COMMNAD_CD
        cclient.cc_command(log, cd_command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", cd_command)
            return -1

        time_start = time.time()
        cclient.cc_command(log, command)
        time_end = time.time()
        if time_start + COMMAND_ABORT_TIMEOUT < time_end:
            log.cl_error("command [%s] costs [%s] seconds even when aborting",
                         command, time_end - time_start)
            return -1

        if ccommand.cc_speed == clownfish.SPEED_ALWAYS_SLOW:
            if result.cr_exit_status == 0:
                log.cl_error("slow command [%s] should return failure when "
                             "aborting", command)
                return -1
        elif ccommand.cc_speed == clownfish.SPEED_ALWAYS_FAST:
            if result.cr_exit_status != 0:
                log.cl_error("quick command [%s] should succeed when "
                             "aborting", command)
                return -1
    cclient.cc_abort_event.clear()
    return 0


CLOWNFISH_TESTS.append(abort_command)


def prepare_twice(log, workspace, cclient):
    """
    Test of running prepare for twice
    """
    # pylint: disable=unused-argument
    # EX-234: failure when prepare for multiple times
    result = log.cl_result
    command = clownfish.CLOWNFISH_COMMNAD_PREPARE
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    return 0


CLOWNFISH_TESTS.append(prepare_twice)


def do_test_connected(log, workspace, console_client,
                      test_config, test_config_fpath,
                      test_functs):
    """
    Run test with the console connected
    """
     # pylint: disable=too-many-branches,too-many-locals,too-many-arguments
    test_dict = {}
    for test_funct in test_functs:
        test_dict[test_funct.__name__] = test_funct

    quit_on_error = True
    only_test_configs = utils.config_value(test_config,
                                           cstr.CSTR_ONLY_TESTS)
    if only_test_configs is None:
        log.cl_debug("no [%s] is configured, run all tests",
                     cstr.CSTR_ONLY_TESTS)
        selected_tests = test_functs
    else:
        selected_tests = []
        for test_name in only_test_configs:
            if test_name not in test_dict:
                log.cl_error("test [%s] doenot exist, please correct file "
                             "[%s]", test_name, test_config_fpath)
                return -1
            test_funct = test_dict[test_name]
            selected_tests.append(test_funct)

    not_selected_tests = []
    for test_funct in test_functs:
        if test_funct not in selected_tests:
            not_selected_tests.append(test_funct)

    passed_tests = []
    failed_tests = []
    skipped_tests = []

    for test_func in selected_tests:
        test_workspace = workspace + "/" + test_func.__name__
        ret = utils.mkdir(test_workspace)
        if ret:
            log.cl_error("failed to create directory [%s] on local host",
                         test_workspace)
            return -1

        ret = test_func(log, test_workspace, console_client)
        if ret < 0:
            log.cl_error("test [%s] failed", test_func.__name__)
            failed_tests.append(test_func)
            if quit_on_error:
                return -1
        elif ret == 1:
            log.cl_warning("test [%s] skipped", test_func.__name__)
            skipped_tests.append(test_func)
        else:
            log.cl_info("test [%s] passed", test_func.__name__)
            passed_tests.append(test_func)

    if len(not_selected_tests) != 0:
        for not_selected_test in not_selected_tests:
            log.cl_warning("test [%s] is not selected", not_selected_test.__name__)

    if len(skipped_tests) != 0:
        for skipped_test in skipped_tests:
            log.cl_warning("test [%s] skipped", skipped_test.__name__)

    ret = 0
    if len(failed_tests) != 0:
        for failed_test in failed_tests:
            log.cl_error("test [%s] failed", failed_test.__name__)
            ret = -1

    if len(passed_tests) != 0:
        for passed_test in passed_tests:
            log.cl_info("test [%s] passed", passed_test.__name__)
    return ret


def connect_and_test(log, workspace, test_config, test_config_fpath,
                     install_config, install_config_fpath,
                     clownfish_config, clownfish_config_fpath,
                     test_functs):
    """
    Connect Clownfish and test
    """
    # pylint: disable=too-many-arguments
    clownfish_server_ip = utils.config_value(install_config, cstr.CSTR_VIRTUAL_IP)
    if not clownfish_server_ip:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VIRTUAL_IP, install_config_fpath)
        return -1

    clownfish_server_port = utils.config_value(clownfish_config,
                                               cstr.CSTR_CLOWNFISH_PORT)
    if clownfish_server_port is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_CLOWNFISH_PORT, clownfish_config_fpath)
        return -1

    server_url = "tcp://%s:%s" % (clownfish_server_ip, clownfish_server_port)
    console_client = clownfish_console.ClownfishClient(log, workspace,
                                                       server_url)
    ret = console_client.cc_init()
    if ret == 0:
        ret = do_test_connected(log, workspace, console_client,
                                test_config, test_config_fpath,
                                test_functs)
        if ret:
            log.cl_error("failed to run test with console connected to "
                         "Clownfish server")
    else:
        log.cl_error("failed to connect to Clownfish server")

    # No matter connection fails or not, need to finish
    console_client.cc_fini()
    return ret


def clownfish_send_lustre_rpms(log, install_config,
                               install_config_fpath,
                               config, config_fpath):
    """
    Send the required Lustre RPMs to the server hosts
    """
    # pylint: disable=too-many-locals
    server_hosts = clownfish_install_nodeps.clownfish_parse_server_hosts(log,
                                                                         install_config,
                                                                         install_config_fpath)
    if server_hosts is None:
        log.cl_error("failed to parse Clownfish server hosts, please correct "
                     "file [%s]", install_config_fpath)
        return -1

    dist_configs = utils.config_value(config, cstr.CSTR_LUSTRE_DISTRIBUTIONS)
    if dist_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_LUSTRE_DISTRIBUTIONS, config_fpath)
        return None

    directories = []
    for dist_config in dist_configs:
        lustre_rpm_dir = utils.config_value(dist_config,
                                            cstr.CSTR_LUSTRE_RPM_DIR)
        if lustre_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_RPM_DIR, config_fpath)
            return None
        lustre_rpm_dir = lustre_rpm_dir.rstrip("/")
        if lustre_rpm_dir not in directories:
            directories.append(lustre_rpm_dir)

        e2fsprogs_rpm_dir = utils.config_value(dist_config,
                                               cstr.CSTR_E2FSPROGS_RPM_DIR)
        if e2fsprogs_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_E2FSPROGS_RPM_DIR, config_fpath)
            return None

        e2fsprogs_rpm_dir = e2fsprogs_rpm_dir.rstrip("/")
        if e2fsprogs_rpm_dir not in directories:
            directories.append(e2fsprogs_rpm_dir)

    for server_host in server_hosts:
        for directory in directories:
            parent = os.path.dirname(directory)
            command = "mkdir -p %s" % parent
            retval = server_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             server_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = server_host.sh_send_file(log, directory, parent)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "directory [%s] on host [%s]",
                             directory, parent,
                             server_host.sh_hostname)
                return -1
    return 0


def clownfish_do_test(log, workspace, test_config, test_config_fpath):
    """
    Start to test
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    ret = test_common.test_install_virt(log, workspace, test_config,
                                        test_config_fpath)
    if ret:
        log.cl_error("failed to install virtual machine")
        return -1

    install_config_fpath = utils.config_value(test_config,
                                              cstr.CSTR_INSTALL_CONFIG)
    if install_config_fpath is None:
        log.cl_error("can NOT find [%s] in the test config, "
                     "please correct file [%s]",
                     cstr.CSTR_INSTALL_CONFIG, test_config_fpath)
        return -1

    skip_install = utils.config_value(test_config,
                                      cstr.CSTR_SKIP_INSTALL)
    if skip_install is None:
        log.cl_debug("no [%s] is configured, do not skip install")
        skip_install = False

    install_config_fd = open(install_config_fpath)
    ret = 0
    try:
        install_config = yaml.load(install_config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s",
                     install_config_fpath, traceback.format_exc())
        ret = -1
    install_config_fd.close()
    if ret:
        return -1

    clownfish_config_fpath = utils.config_value(install_config,
                                                cstr.CSTR_CONFIG_FPATH)
    if clownfish_config_fpath is None:
        log.cl_error("can NOT find [%s] in the installation config, "
                     "please correct file [%s]",
                     cstr.CSTR_CONFIG_FPATH, install_config_fpath)
        return -1

    clownfish_config_fd = open(clownfish_config_fpath)
    ret = 0
    try:
        clownfish_config = yaml.load(clownfish_config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s",
                     clownfish_config_fpath, traceback.format_exc())
        ret = -1
    clownfish_config_fd.close()
    if ret:
        return -1

    if not skip_install:
        ret = clownfish_send_lustre_rpms(log, install_config,
                                         install_config_fpath,
                                         clownfish_config,
                                         clownfish_config_fpath)
        if ret:
            log.cl_error("failed to send Lustre RPMs")
            return -1

    install_server_config = utils.config_value(test_config,
                                               cstr.CSTR_INSTALL_SERVER)
    if install_server_config is None:
        log.cl_error("can NOT find [%s] in the config file [%s], "
                     "please correct it", cstr.CSTR_INSTALL_SERVER,
                     test_config_fpath)
        return -1

    install_server_hostname = utils.config_value(install_server_config,
                                                 cstr.CSTR_HOSTNAME)
    if install_server_hostname is None:
        log.cl_error("can NOT find [%s] in the config of installation host, "
                     "please correct file [%s]",
                     cstr.CSTR_HOSTNAME, test_config_fpath)
        return None

    ssh_identity_file = utils.config_value(install_server_config,
                                           cstr.CSTR_SSH_IDENTITY_FILE)
    install_server = ssh_host.SSHHost(install_server_hostname,
                                      identity_file=ssh_identity_file)

    ret = test_common.test_install(log, workspace, install_config_fpath,
                                   skip_install, install_server, "clownfish",
                                   constants.CLOWNFISH_INSTALL_CONFIG_FNAME)
    if ret:
        log.cl_error("failed to test installation of Clownfish")
        return -1

    ret = connect_and_test(log, workspace, test_config,
                           test_config_fpath, install_config,
                           install_config_fpath, clownfish_config,
                           clownfish_config_fpath, CLOWNFISH_TESTS)
    return ret


def clownfish_test(log, workspace, config_fpath):
    """
    Start to test holding the confiure lock
    """
    # pylint: disable=bare-except
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
        ret = clownfish_do_test(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    if ret:
        log.cl_error("test of Clownfish failed, please check [%s] for more "
                     "log", workspace)
    else:
        log.cl_info("test of Clownfish passed, please check [%s] "
                    "for more log", workspace)
    return ret


def main():
    """
    Start clownfish test
    """
    cmd_general.main(constants.CLOWNFISH_TEST_CONFIG,
                     constants.CLOWNFISH_TEST_LOG_DIR,
                     clownfish_test)
