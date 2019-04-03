# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for installing a tool from ISO

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""
import os

from pylcommon import utils
from pylcommon import ssh_host
from pylcommon import cstr

RPM_PATTERN_RHEL7 = r"^%s-\d.+(\.el7|).*\.(x86_64|noarch)\.rpm$"
RPM_PATTERN_RHEL6 = r"^%s-\d.+(\.el6|).*\.(x86_64|noarch)\.rpm$"
CLOWNFISH_INSTALL_DEPENDENT_RPMS = ["rsync",
                                    "libyaml",
                                    "PyYAML",
                                    "python2-filelock",
                                    "pytz",
                                    "python-dateutil",
                                    "zeromq3",
                                    "python-zmq",
                                    "protobuf-python",
                                    "python-requests"]


def iso_path_in_config(log, host, config_fpath):
    """
    Return the ISO path in the config file
    """
    command = (r"grep -v ^\# %s | "
               "grep ^%s: | awk '{print $2}'" %
               (config_fpath, cstr.CSTR_ISO_PATH))

    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on localhost, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    lines = retval.cr_stdout.splitlines()
    if len(lines) != 1:
        log.cl_error("unexpected iso path in config file: %s", lines)
        return None
    return lines[0]


def find_iso_path_in_cwd(log, host, iso_path_pattern):
    """
    Find iso path in current work directory
    """
    command = ("ls %s" % (iso_path_pattern))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    current_dir = os.getcwd()
    iso_names = retval.cr_stdout.split()
    if len(iso_names) != 1:
        log.cl_error("found unexpected ISOs %s under currect directory [%s]",
                     iso_names, current_dir)
        return None

    iso_name = iso_names[0]
    iso_path = current_dir + "/" + iso_name
    return iso_path


def generate_repo_file(repo_fpath, packages_dir, package_name):
    """
    Prepare the local repo config file
    """
    repo_config = ("""# %s packages
[%s]
name=%s Packages
baseurl=file://%s
priority=1
gpgcheck=0
enabled=1
gpgkey=
""" % (package_name, package_name, package_name, packages_dir))
    with open(repo_fpath, 'w') as config_fd:
        config_fd.write(repo_config)


def yum_repo_install(log, host, repo_fpath, rpms):
    """
    Install RPMs using YUM
    """
    if len(rpms) == 0:
        return 0

    repo_ids = host.sh_yum_repo_ids(log)
    if repo_ids is None:
        log.cl_error("failed to get the yum repo IDs on host [%s]",
                     host.sh_hostname)
        return -1

    command = "yum -c %s" % repo_fpath
    for repo_id in repo_ids:
        command += " --disablerepo %s" % repo_id

    command += " install -y"
    for rpm in rpms:
        command += " %s" % rpm

    retval = host.sh_run(log, command, timeout=ssh_host.LONGEST_TIME_YUM_INSTALL)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1
    return 0


def dependency_install(log, host, config, rpms, name, iso_path_pattern):
    """
    Install the dependent RPMs
    """
    # pylint: disable=too-many-arguments
    if not os.path.exists(config):
        log.cl_error("config file [%s] doesn't exist", config)
        return -1

    if not os.path.isfile(config):
        log.cl_error("config file [%s] isn't a file", config)
        return -1

    iso_path = iso_path_in_config(log, host, config)
    if iso_path is None:
        iso_path = find_iso_path_in_cwd(log, host, iso_path_pattern)
        if iso_path is None:
            log.cl_error("failed to find %s ISO under currect directory "
                         "with pattern [%s]", name, iso_path_pattern)
            return -1
        log.cl_info("no [%s] is configured, use [%s] under current "
                    "directory", cstr.CSTR_ISO_PATH, iso_path)

    mnt_path = "/mnt/" + utils.random_word(8)
    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, iso_path, mnt_path))
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

    repo_config_fpath = ("/tmp/%s.repo" % (name))
    packages_dir = mnt_path + "/" + cstr.CSTR_PACKAGES
    generate_repo_file(repo_config_fpath, packages_dir, name)
    ret = yum_repo_install(log, host, repo_config_fpath, rpms)
    if ret:
        log.cl_error("failed to install dependency RPMs on from ISO at localhost")
        return -1

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
        ret = -1
    return ret


def clownfish_rpm_install(log, host, iso_path):
    """
    Reinstall the Clownfish RPMs
    """
    ret = host.sh_rpm_find_and_uninstall(log, "grep clownfish")
    if ret:
        log.cl_error("failed to uninstall Clownfish rpm on host [%s]",
                     host.sh_hostname)
        return -1

    ret = host.sh_rpm_find_and_uninstall(log, "grep pylcommon")
    if ret:
        log.cl_error("failed to uninstall pylcommon rpm on host [%s]",
                     host.sh_hostname)
        return -1

    package_dir = iso_path + "/" + cstr.CSTR_PACKAGES

    command = ("rpm -ivh %s/clownfish-pylcommon-*.x86_64.rpm "
               "%s/clownfish-1.*.x86_64.rpm --nodeps" %
               (package_dir, package_dir))
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
    return 0


class InstallationCluster(object):
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    Installation cluster config.
    """
    def __init__(self, workspace, hosts, mnt_path):
        self.ic_hosts = hosts
        self.ic_mnt_path = mnt_path
        self.ic_workspace = workspace
        self.ic_iso_basename = "ISO"
        self.ic_iso_dir = workspace + "/" + self.ic_iso_basename
        self.ic_pip_dir = self.ic_iso_dir + "/" + cstr.CSTR_PIP
        self.ic_rpm_fnames = None
        self.ic_repo_config_fpath = workspace + "/clownfish.repo"

    def _ic_send_iso_files(self, log, host):
        """
        send RPMs to a host
        """
        # pylint: disable=too-many-return-statements
        command = ("mkdir -p %s" % (self.ic_workspace))
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

        ret = host.sh_send_file(log, self.ic_mnt_path, self.ic_workspace)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         self.ic_mnt_path, self.ic_workspace,
                         host.sh_hostname)
            return -1

        basename = os.path.basename(self.ic_mnt_path)
        command = ("cd %s && mv %s %s" %
                   (self.ic_workspace, basename,
                    self.ic_iso_basename))
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

        command = "ls %s" % self.ic_iso_dir
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
        self.ic_rpm_fnames = retval.cr_stdout.split()
        return 0

    def _ic_host_install(self, log, host, pip_libs, dependent_rpms):
        """
        Install Clownfish on a host
        """
        hostname = host.sh_hostname
        # only support RHEL7 series
        distro = host.sh_distro(log)
        if distro not in [ssh_host.DISTRO_RHEL7]:
            log.cl_error("unsupported distro of host [%s]",
                         hostname)
            return -1

        ret = self._ic_send_iso_files(log, host)
        if ret:
            log.cl_error("failed to send ISO files to host [%s]",
                         hostname)
            return -1

        log.cl_info("installing dependent RPMs %s on host [%s]",
                    dependent_rpms, hostname)
        ret = host.sh_send_file(log, self.ic_repo_config_fpath, self.ic_workspace)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         self.ic_repo_config_fpath, self.ic_workspace,
                         host.sh_hostname)
            return -1

        ret = yum_repo_install(log, host, self.ic_repo_config_fpath,
                               dependent_rpms)
        if ret:
            log.cl_error("failed to install dependent RPMs on host "
                         "[%s]", host.sh_hostname)
            return -1

        for pip_lib in pip_libs:
            log.cl_info("installing pip lib [%s] on host [%s]",
                        pip_lib, hostname)
            command = ("pip install --no-index --find-links %s %s" %
                       (self.ic_pip_dir, pip_lib))
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

        ret = clownfish_rpm_install(log, host, self.ic_iso_dir)
        if ret:
            log.cl_error("failed to install Clownfish RPMs or the "
                         "dependencies on host [%s]", host.sh_hostname)
            return -1
        return 0

    def ic_install(self, log, pip_libs, dependent_rpms):
        """
        Install RPMs
        """
        # Prepare the local repo config file
        packages_dir = self.ic_iso_dir + "/" + cstr.CSTR_PACKAGES
        generate_repo_file(self.ic_repo_config_fpath, packages_dir,
                           "Clownfish")
        ret = 0
        for host in self.ic_hosts:
            ret = self._ic_host_install(log, host, pip_libs, dependent_rpms)
            if ret:
                log.cl_error("failed to prepare host [%s] for Clownfish cluster",
                             host.sh_hostname)
                return -1
        return ret
