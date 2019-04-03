# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for building Clownfish
"""
# pylint: disable=too-many-lines
import traceback
import os
import re
import yaml

# Local libs
from pylcommon import utils
from pylcommon import ssh_host
from pylcommon import cstr
from pylcommon import constants
from pylcommon import install_common
from pylcommon import cmd_general
from pyclownfish import clownfish_common

SOURCE_DIR = None


def clone_src_from_git(log, build_dir, git_url, branch,
                       ssh_identity_file=None):
    """
    Get the Lustre soure codes from Git server.
    """
    command = ("rm -fr %s && mkdir -p %s && git init %s" %
               (build_dir, build_dir, build_dir))
    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    command = ("cd %s && git config remote.origin.url %s && "
               "GIT_SSH_COMMAND=\"ssh -i /root/.ssh/id_dsa\" "
               "git fetch --tags --progress %s "
               "+refs/heads/*:refs/remotes/origin/* && "
               "git checkout %s -f" %
               (build_dir, git_url, git_url, branch))
    if ssh_identity_file is not None:
        # Git 2.3.0+ has GIT_SSH_COMMAND
        command = ("ssh-agent sh -c 'ssh-add " + ssh_identity_file +
                   " && " + command + "'")

    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def download_dependent_rpms(log, host, packages_dir):
    """
    Download dependent RPMs
    """
    # pylint: disable=too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    # The yumdb might be broken, so sync
    log.cl_info("downloading dependency RPMs")
    command = "yumdb sync"
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

    command = ("mkdir -p %s" % (packages_dir))
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

    command = ("ls %s" % (packages_dir))
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
    existing_rpm_fnames = retval.cr_stdout.split()

    dependent_rpms = clownfish_common.CLOWNFISH_DEPENDENT_RPMS[:]
    for rpm_name in install_common.CLOWNFISH_INSTALL_DEPENDENT_RPMS:
        if rpm_name not in dependent_rpms:
            dependent_rpms.append(rpm_name)

    command = "repotrack -a x86_64 -p %s" % (packages_dir)
    for rpm_name in dependent_rpms:
        command += " " + rpm_name

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

    exist_pattern = (r"^%s/(?P<rpm_fname>\S+) already exists and appears to be "
                     "complete$" % (packages_dir))
    exist_regular = re.compile(exist_pattern)
    download_pattern = (r"^Downloading (?P<rpm_fname>\S+)$")
    download_regular = re.compile(download_pattern)
    lines = retval.cr_stdout.splitlines()
    for line in lines:
        match = exist_regular.match(line)
        if match:
            rpm_fname = match.group("rpm_fname")
        else:
            match = download_regular.match(line)
            if match:
                rpm_fname = match.group("rpm_fname")
            else:
                log.cl_error("unkown output [%s] of repotrack on host "
                             "[%s], stdout = [%s]",
                             line, host.sh_hostname, retval.cr_stdout)
                return -1
        if rpm_fname in existing_rpm_fnames:
            existing_rpm_fnames.remove(rpm_fname)

    for fname in existing_rpm_fnames:
        fpath = packages_dir + "/" + fname
        log.cl_debug("found unnecessary file [%s] under directory [%s], "
                     "removing it", fname, packages_dir)
        ret = host.sh_remove_file(log, fpath)
        if ret:
            return -1
    return 0


def check_dir_content(log, host, directory, contents, cleanup=False):
    """
    Check that the directory has expected content
    """
    command = ("ls %s" % (directory))
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
    existing_fnames = retval.cr_stdout.split()

    for fname in contents:
        if fname not in existing_fnames:
            log.cl_error("can not find necessary content [%s] under "
                         "directory [%s]", fname, directory)
        existing_fnames.remove(fname)

    for fname in existing_fnames:
        fpath = directory + "/" + fname
        if cleanup:
            log.cl_debug("found unnecessary content [%s] under directory "
                         "[%s], removing it", fname, directory)
            ret = host.sh_remove_file(log, fpath)
            if ret:
                return -1
        else:
            log.cl_error("found unnecessary content [%s] under directory "
                         "[%s]", fname, directory)
            return -1
    return 0


def do_build(log, source_dir, config, config_fpath):
    """
    Build the ISO
    """
    # pylint: disable=too-many-return-statements,too-many-locals
    # pylint: disable=unused-argument
    log.cl_info("building using config [%s]", config_fpath)
    local_host = ssh_host.SSHHost("localhost", local=True)
    distro = local_host.sh_distro(log)
    if distro != ssh_host.DISTRO_RHEL7:
        log.cl_error("build can only be launched on RHEL7/CentOS7 host")
        return -1

    iso_cached_dir = source_dir + "/../iso_cached_dir"
    command = ("mkdir -p %s" % iso_cached_dir)
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

    package_dir = iso_cached_dir + "/" + cstr.CSTR_PACKAGES

    ret = download_dependent_rpms(log, local_host, package_dir)
    if ret:
        log.cl_error("failed to download dependent rpms")
        return -1

    contents = [cstr.CSTR_PACKAGES]
    ret = check_dir_content(log, local_host, iso_cached_dir, contents,
                            cleanup=True)
    if ret:
        log.cl_error("directory [%s] doesn't have expected content",
                     iso_cached_dir)
        return -1

    log.cl_info("building Clownfish ISO")

    command = ("cd %s && rm clownfish-*.tar.bz2 clownfish-*.tar.gz -f && "
               "sh autogen.sh && "
               "./configure --with-cached-iso=%s && "
               "make && "
               "make iso" %
               (source_dir, iso_cached_dir))
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

    return 0


def lbuild(log, workspace, config_fpath):
    """
    Build the ISO
    """
    # pylint: disable=bare-except,unused-argument
    if config_fpath is None:
        config = None
    else:
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

    return do_build(log, SOURCE_DIR, config, config_fpath)


def main():
    """
    Start to build Clownfish
    """
    # pylint: disable=global-statement
    global SOURCE_DIR
    SOURCE_DIR = os.getcwd()
    cmd_general.main(constants.CLOWNFISH_BUILD_CONFIG, SOURCE_DIR, lbuild)
