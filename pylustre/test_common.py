# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for testing
"""

import traceback
import os
import yaml

# Local libs
from pylustre import utils
from pylustre import cstr
from pylustre import ssh_host
from pylustre import watched_io
from pylustre import lyaml
from pylustre import lvirt


def mount_and_run(log, workspace, host, host_iso_path, config, funct,
                  arg):
    """
    Mount the ISO and run a funct
    """
    # pylint: disable=bare-except,too-many-arguments
    mnt_path = "/mnt/" + utils.random_word(8)

    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, host_iso_path, mnt_path))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    try:
        ret = funct(log, workspace, host, mnt_path, config, arg)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    command = ("umount %s" % (mnt_path))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        ret = -1

    command = ("rmdir %s" % (mnt_path))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return ret


def _start_install(log, workspace, install_server, mnt_path,
                   install_config, arg):
    """
    Run the install test
    """
    # pylint: disable=too-many-locals,too-many-arguments

    # Make sure install server is local host, since this will overwrite the
    # local config files
    uuid_install = install_server.sh_uuid(log)
    if uuid_install is None:
        log.cl_error("failed to get the UUID on host [%s]",
                     install_server.sh_hostname)
        return -1

    local_host = ssh_host.SSHHost("localhost", local=True)
    uuid_local = local_host.sh_uuid(log)
    if uuid_local is None:
        log.cl_error("failed to get the UUID on localhost")
        return -1

    if uuid_local == uuid_install:
        log.cl_error("please do NOT use host [%s] as the install server, "
                     "since it is the localhost, and installation test "
                     "would overwrite the local configuration files",
                     install_server.sh_hostname)
        return -1

    ret = install_server.sh_rpm_find_and_uninstall(log, "grep pylustre")
    if ret:
        log.cl_error("failed to uninstall pylustre rpms on host [%s]",
                     install_server.sh_hostname)
        return -1

    ret = install_server.sh_rpm_find_and_uninstall(log, "grep clownfish")
    if ret:
        log.cl_error("failed to uninstall Clownfish rpms on host [%s]",
                     install_server.sh_hostname)
        return -1

    package_dir = mnt_path + "/" + cstr.CSTR_PACKAGES
    command = ("rpm -ivh %s/clownfish-pylustre-*.x86_64.rpm "
               "%s/clownfish-1.*.x86_64.rpm" %
               (package_dir, package_dir))
    retval = install_server.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     install_server.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    cmd_name, config_fname = arg
    install_config_fpath = (workspace + "/" + config_fname)
    config_string = ("""#
# Configuration file for installing %s from DDN
#
""" % (cmd_name))
    config_string += yaml.dump(install_config, Dumper=lyaml.YamlDumper,
                               default_flow_style=False)
    try:
        with open(install_config_fpath, 'w') as yaml_file:
            yaml_file.write(config_string)
    except:
        log.cl_error("failed to save the config file to [%s]")
        return -1

    ret = install_server.sh_send_file(log, install_config_fpath, "/etc")
    if ret:
        log.cl_error("failed to send file [%s] on local host to "
                     "/etc on host [%s]",
                     install_config_fpath,
                     install_server.sh_hostname)
        return -1

    args = {}
    args["log"] = log
    args["hostname"] = install_server.sh_hostname
    stdout_file = (workspace + "/" + cmd_name + "_install.stdout")
    stderr_file = (workspace + "/" + cmd_name + "_install.stderr")
    stdout_fd = watched_io.watched_io_open(stdout_file,
                                           watched_io.log_watcher_info, args)
    stderr_fd = watched_io.watched_io_open(stderr_file,
                                           watched_io.log_watcher_error, args)
    command = ("%s_install" % (cmd_name))
    retval = install_server.sh_run(log, command, stdout_tee=stdout_fd,
                                   stderr_tee=stderr_fd, return_stdout=False,
                                   return_stderr=False, timeout=None,
                                   flush_tee=True)
    stdout_fd.close()
    stderr_fd.close()

    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d]",
                     command,
                     install_server.sh_hostname,
                     retval.cr_exit_status)
        return -1
    return 0


def start_install(log, workspace, install_server,
                  install_config, config_fpath, arg):
    """
    Start installation
    """
    # pylint: disable=too-many-locals,too-many-arguments
    command = "mkdir -p %s" % workspace
    retval = install_server.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     install_server.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    local_host = ssh_host.SSHHost("localhost", local=True)
    command = "ls clownfish-*.iso"
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    current_dir = os.getcwd()
    iso_names = retval.cr_stdout.split()
    if len(iso_names) != 1:
        log.cl_error("found unexpected ISOs [%s] under currect directory "
                     "[%s]", iso_names, current_dir)
        return -1

    iso_name = iso_names[0]
    iso_path = current_dir + "/" + iso_name

    ret = install_server.sh_send_file(log, config_fpath, workspace)
    if ret:
        log.cl_error("failed to send Clownfish config [%s] on local host to "
                     "directory [%s] on host [%s]",
                     config_fpath, workspace,
                     install_server.sh_hostname)
        return -1
    config_fname = os.path.basename(config_fpath)

    ret = install_server.sh_send_file(log, iso_path, workspace)
    if ret:
        log.cl_error("failed to send Clownfish ISO [%s] on local host to "
                     "directory [%s] on host [%s]",
                     iso_path, workspace,
                     install_server.sh_hostname)
        return -1

    host_iso_path = workspace + "/" + iso_name
    host_config_fpath = workspace + "/" + config_fname
    install_config[cstr.CSTR_ISO_PATH] = host_iso_path
    install_config[cstr.CSTR_CONFIG_FPATH] = host_config_fpath
    ret = mount_and_run(log, workspace, install_server, host_iso_path,
                        install_config, _start_install, arg)
    if ret:
        log.cl_error("failed to test installation on host [%s]",
                     install_server.sh_hostname)
        return -1
    return 0


def test_install(log, workspace, install_config_fpath,
                 skip_install, install_server, cmd_name, config_fname):
    """
    Start to test
    """
    # pylint: disable=too-many-arguments
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

    config_fpath = utils.config_value(install_config,
                                      cstr.CSTR_CONFIG_FPATH)
    if config_fpath is None:
        log.cl_error("can NOT find [%s] in the installation config, "
                     "please correct file [%s]",
                     cstr.CSTR_CONFIG_FPATH, install_config_fpath)
        return -1

    if not skip_install:
        arg = (cmd_name, config_fname)
        ret = start_install(log, workspace, install_server, install_config,
                            config_fpath, arg)
        if ret:
            log.cl_error("failed to run install test")
            return -1
    return 0


def test_install_virt(log, workspace, test_config, test_config_fpath):
    """
    Start to install virt
    """
    skip_virt = utils.config_value(test_config,
                                   cstr.CSTR_SKIP_VIRT)
    if skip_virt is None:
        log.cl_debug("no [%s] is configured, do not skip checking virt")
        skip_virt = False

    if skip_virt:
        log.cl_debug("skip checking virt")
        return 0

    virt_config_fpath = utils.config_value(test_config,
                                           cstr.CSTR_VIRT_CONFIG)
    if virt_config_fpath is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VIRT_CONFIG, test_config_fpath)
        return -1
    ret = lvirt.lvirt(log, workspace, virt_config_fpath)
    if ret:
        log.cl_error("failed to install the virtual machines")
        return -1
    return 0
