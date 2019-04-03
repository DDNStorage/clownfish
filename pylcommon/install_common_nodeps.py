# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for installing a tool from ISO
"""
import traceback
import yaml

from pylcommon import utils
from pylcommon import ssh_host
from pylcommon import cstr
from pylcommon import install_common


def _iso_mount_and_install(log, workspace, config, config_fpath, install_funct):
    """
    Mount the ISO and install the system
    """
    # pylint: disable=bare-except
    local_host = ssh_host.SSHHost("localhost", local=True)
    iso_path = utils.config_value(config, cstr.CSTR_ISO_PATH)
    if iso_path is None:
        iso_path = install_common.find_iso_path_in_cwd(log, local_host, "clownfish-*.iso")
        if iso_path is None:
            log.cl_error("failed to find Clownfish ISO %s under currect "
                         "directory")
            return -1
        log.cl_info("no [%s] is configured, use [%s] under current "
                    "directory", cstr.CSTR_ISO_PATH, iso_path)

    mnt_path = "/mnt/" + utils.random_word(8)

    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, iso_path, mnt_path))
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

    try:
        ret = install_funct(log, workspace, config, config_fpath, mnt_path,
                            local_host)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    command = ("umount %s" % (mnt_path))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        ret = -1

    command = ("rmdir %s" % (mnt_path))
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
    return ret


def mount_iso_and_install(log, workspace, config_fpath, install_funct):
    """
    Start Clownfish holding the configure lock
    """
    # pylint: disable=too-many-branches,bare-except,too-many-locals
    # pylint: disable=too-many-statements
    ret = 0
    try:
        config_fd = open(config_fpath)
        config = yaml.load(config_fd)
        config_fd.close()
    except:
        log.cl_error("not able to load [%s] as yaml file: %s", config_fpath,
                     traceback.format_exc())
        ret = -1

    if ret:
        return -1

    try:
        ret = _iso_mount_and_install(log, workspace, config, config_fpath,
                                     install_funct)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    return ret
