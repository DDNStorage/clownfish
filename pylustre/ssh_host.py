# pylint: disable=too-many-lines
# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
The host that localhost could use SSH to run command

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

import time
import os
import glob
import shutil
import re
import stat

# local libs
from pylustre import utils
from pylustre import watched_io


# OS distribution RHEL6/CentOS6
DISTRO_RHEL6 = "rhel6"
# OS distribution RHEL7/CentOS7
DISTRO_RHEL7 = "rhel7"
# The shortest time that a reboot could finish. It is used to check whether
# a host has actually rebooted or not.
SHORTEST_TIME_REBOOT = 10
# The logest time that a reboot wil takes
LONGEST_TIME_REBOOT = 240
# The longest time that a simple command should finish
LONGEST_SIMPLE_COMMAND_TIME = 600
# Yum install is slow, so use a larger timeout value
LONGEST_TIME_YUM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 2
# RPM install is slow, so use a larger timeout value
LONGEST_TIME_RPM_INSTALL = LONGEST_SIMPLE_COMMAND_TIME * 2
# The longest time that a issue reboot would stop the SSH server
LONGEST_TIME_ISSUE_REBOOT = 10


def sh_escape(command):
    """
    Escape special characters from a command so that it can be passed
    as a double quoted (" ") string in a (ba)sh command.

    Args:
            command: the command string to escape.

    Returns:
            The escaped command string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.

    See also: http://www.tldp.org/LDP/abs/html/escapingsection.html
    """
    command = command.replace("\\", "\\\\")
    command = command.replace("$", r'\$')
    command = command.replace('"', r'\"')
    command = command.replace('`', r'\`')
    return command


def scp_remote_escape(filename):
    """
    Escape special characters from a filename so that it can be passed
    to scp (within double quotes) as a remote file.

    Bis-quoting has to be used with scp for remote files, "bis-quoting"
    as in quoting x 2
    scp does not support a newline in the filename

    Args:
            filename: the filename string to escape.

    Returns:
            The escaped filename string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.
    """
    escape_chars = r' !"$&' "'" r'()*,:;<=>?[\]^`{|}'

    new_name = []
    for char in filename:
        if char in escape_chars:
            new_name.append("\\%s" % (char,))
        else:
            new_name.append(char)

    return sh_escape("".join(new_name))


def make_ssh_command(login_name="root", identity_file=None):
    """
    Return the ssh cmd string
    """
    extra_option = ""
    if identity_file is not None:
        extra_option = ("-i %s" % identity_file)
    full_command = ("ssh -a -x -l %s -o StrictHostKeyChecking=no "
                    "-o BatchMode=yes %s" %
                    (login_name, extra_option))
    return full_command


def ssh_command(hostname, command, login_name="root", identity_file=None):
    """
    Return the ssh command on a remote host
    """
    ssh_string = make_ssh_command(login_name=login_name,
                                  identity_file=identity_file)
    full_command = ("%s %s \"LANG=en_US.UTF-8 %s\"" %
                    (ssh_string, hostname, sh_escape(command)))
    return full_command


def ssh_run(hostname, command, login_name="root", timeout=None,
            stdout_tee=None, stderr_tee=None, stdin=None,
            return_stdout=True, return_stderr=True,
            quit_func=None, identity_file=None, flush_tee=False):
    """
    Use ssh to run command on a remote host
    """
    # pylint: disable=too-many-arguments
    if not isinstance(command, basestring):
        stderr = "type of command argument is not a basestring"
        return utils.CommandResult(stderr=stderr, exit_status=-1)

    full_command = ssh_command(hostname, command, login_name, identity_file)
    return utils.run(full_command, timeout=timeout, stdout_tee=stdout_tee,
                     stderr_tee=stderr_tee, stdin=stdin,
                     return_stdout=return_stdout, return_stderr=return_stderr,
                     quit_func=quit_func, flush_tee=flush_tee)


class SSHHost(object):
    """
    Each SSH host has an object of SSHHost
    """
    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    def __init__(self, hostname, identity_file=None, local=False, host_id=None):
        self.sh_hostname = hostname
        self.sh_identity_file = identity_file
        self.sh_local = local
        self.sh_cached_distro = None
        # Key: command name, Value: True/False
        self.sh_cached_has_commands = {}
        self.sh_host_id = host_id
        self.sh_latest_uptime = 0

    def sh_is_up(self, log, timeout=60):
        """
        Whether this host is up now
        """
        ret = self.sh_run(log, "true", timeout=timeout)
        if ret.cr_exit_status != 0:
            return False
        return True

    def sh_expect_retval(self, retval, args):
        """
        Return 0 if got expected retval
        """
        # pylint: disable=no-self-use
        expect_exit_status = args[0]
        expect_stdout = args[1]
        expect_stderr = args[2]
        if (expect_exit_status is not None and
                expect_exit_status != retval.cr_exit_status):
            return -1

        if (expect_stdout is not None and
                expect_stdout != retval.cr_stdout):
            return -1

        if (expect_stderr is not None and
                expect_stderr != retval.cr_stderr):
            return -1
        return 0

    def sh_wait_condition(self, log, command, condition_func, args, timeout=90,
                          sleep_interval=1):
        # pylint: disable=too-many-arguments
        """
        Wait until the condition_func returns 0
        """
        time_start = time.time()
        while True:
            retval = self.sh_run(log, command)
            ret = condition_func(retval, args)
            if ret == 0:
                return 0

            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < timeout:
                time.sleep(sleep_interval)
                continue
            log.cl_error("timeout on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
            return -1
        return -1

    def sh_wait_update(self, log, command, expect_exit_status=None,
                       expect_stdout=None,
                       expect_stderr=None,
                       timeout=90,
                       sleep_interval=1):
        # pylint: disable=too-many-arguments
        """
        Wait until the command result on a host changed to expected values
        """
        args = [expect_exit_status, expect_stdout, expect_stderr]
        return self.sh_wait_condition(log, command, self.sh_expect_retval,
                                      args, timeout=timeout,
                                      sleep_interval=sleep_interval)

    def sh_wait_up(self, log, timeout=LONGEST_TIME_REBOOT):
        """
        Wait until the host is up
        """
        return self.sh_wait_update(log, "true", expect_exit_status=0,
                                   timeout=timeout)

    def sh_distro(self, log):
        """
        Return the distro of this host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if self.sh_cached_distro is not None:
            return self.sh_cached_distro

        no_lsb = False
        ret = self.sh_run(log, "which lsb_release")
        if ret.cr_exit_status != 0:
            log.cl_warning("lsb_release is needed on host [%s] for accurate "
                           "distro identification", self.sh_hostname)
            no_lsb = True

        if no_lsb:
            ret = self.sh_run(log, "uname -r")
            if ret.cr_exit_status != 0:
                log.cl_error("lsb_release is needed on host [%s] for accurate "
                             "distro identification", self.sh_hostname)
                return None
            else:
                if "el7" in ret.cr_stdout:
                    self.sh_cached_distro = DISTRO_RHEL7
                    return DISTRO_RHEL7
                elif "el6" in ret.cr_stdout:
                    self.sh_cached_distro = DISTRO_RHEL6
                    return DISTRO_RHEL6
                else:
                    return None

        ret = self.sh_run(log, "lsb_release -s -i")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to distro name on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return None
        name = ret.cr_stdout.strip('\n')

        ret = self.sh_run(log, "lsb_release -s -r")
        if ret.cr_exit_status != 0:
            log.cl_error("failed to distro version on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
            return None
        version = ret.cr_stdout.strip('\n')

        if (name == "RedHatEnterpriseServer" or
                name == "ScientificSL" or
                name == "CentOS"):
            if version.startswith("7"):
                self.sh_cached_distro = DISTRO_RHEL7
                return DISTRO_RHEL7
            elif version.startswith("6"):
                self.sh_cached_distro = DISTRO_RHEL6
                return DISTRO_RHEL6
            else:
                log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                             version, "rhel", self.sh_hostname)
                return None
        elif name == "EnterpriseEnterpriseServer":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "oel", self.sh_hostname)
            return None
        elif name == "SUSE LINUX":
            # PATCHLEVEL=$(sed -n -e 's/^PATCHLEVEL = //p' /etc/SuSE-release)
            # version="${version}.$PATCHLEVEL"
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "sles", self.sh_hostname)
            return None
        elif name == "Fedora":
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, "fc", self.sh_hostname)
            return None
        else:
            log.cl_error("unsupported version [%s] of [%s] on host [%s]",
                         version, name, self.sh_hostname)
            return None

    def sh_prepare_user(self, log, name, uid, gid):
        """
        Add an user if it doesn't exist
        """
        # pylint: disable=too-many-return-statements
        ret = self.sh_run(log, "grep '%s:%s' /etc/passwd | wc -l" % (uid, gid))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to check uid [%s] gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           uid, gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_stdout.strip() != "0":
            log.cl_debug("user [%s] with uid [%s] gid [%s] already exists "
                         "on host [%s], will not create it",
                         name, uid, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "getent group %s" % (gid))
        if ret.cr_exit_status != 0 and len(ret.cr_stdout.strip()) != 0:
            log.cl_warning("failed to check gid [%s] on host "
                           "[%s], ret = [%d], stdout = [%s], stderr = [%s]",
                           gid, self.sh_hostname, ret.cr_exit_status,
                           ret.cr_stdout, ret.cr_stderr)
            return -1

        if ret.cr_stdout.strip() == "0":
            ret = self.sh_run(log, "groupadd -g %s %s" % (gid, name))
            if ret.cr_exit_status != 0:
                log.cl_warning("failed to add group [%s] with gid [%s] on "
                               "host [%s], ret = [%d], stdout = [%s], "
                               "stderr = [%s]",
                               name, gid, self.sh_hostname,
                               ret.cr_exit_status,
                               ret.cr_stdout,
                               ret.cr_stderr)
                return -1
        else:
            log.cl_debug("group [%s] with gid [%s] already exists on "
                         "host [%s], will not create it",
                         name, gid, self.sh_hostname)
            return 0

        ret = self.sh_run(log, "useradd -u %s -g %s %s" % (uid, gid, name))
        if ret.cr_exit_status != 0:
            log.cl_warning("failed to add user [%s] with uid [%s] gid [%s] "
                           "on host [%s], ret = [%d], stdout = [%s], "
                           "stderr = [%s]",
                           name, uid, gid, self.sh_hostname,
                           ret.cr_exit_status, ret.cr_stdout, ret.cr_stderr)
            return -1
        return 0

    def sh_umount(self, log, device):
        """
        Umount the file system of a device
        """
        retval = self.sh_run(log, "umount %s" % device)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run [umount %s] on host [%s], "
                         "will try with -f again",
                         device, self.sh_hostname)
            retval = self.sh_run(log, "umount -f %s" % device)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run [umount -f %s] on host [%s]",
                             device, self.sh_hostname)
                return -1
        return 0

    def sh_get_uptime(self, log):
        """
        Get the uptime of the host
        """
        command = ("expr $(date +%s) - $(cat /proc/uptime | "
                   "awk -F . '{print $1}')")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("can't get uptime on host [%s], command = [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         self.sh_hostname, command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return int(retval.cr_stdout)

    def sh_umount_nfs(self, log, server_name, nfs_path, mnt_path):
        """
        Umount a NFS client on the host
        """
        log.cl_info("umounting NFS client of [%s:%s] from [%s] on host [%s]",
                    server_name, nfs_path, mnt_path, self.sh_hostname)
        nfs_commands = [("umount %s" % (mnt_path))]
        for command in nfs_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s]",
                             command, self.sh_hostname)
                return -1
        log.cl_info("umounted NFS client of [%s:%s] from [%s] on host [%s]",
                    server_name, nfs_path, mnt_path, self.sh_hostname)
        return 0

    def sh_export_nfs(self, log, nfs_path):
        """
        Export NFS server on the host
        """
        log.cl_info("exporting nfs directory [%s] on host [%s]",
                    nfs_path, self.sh_hostname)
        nfs_commands = ["service nfs start", "mount | grep nfsd",
                        ("exportfs -o rw,no_root_squash *:%s" % nfs_path),
                        ("exportfs | grep %s" % nfs_path)]
        for command in nfs_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s]",
                             command, self.sh_hostname)
                return -1
        log.cl_info("exported nfs directory [%s] on host [%s]",
                    nfs_path, self.sh_hostname)
        return 0

    def sh_mount_nfs(self, log, server_name, nfs_path, mnt_path):
        """
        Mount a NFS client on the host
        """
        log.cl_info("mounting NFS client of [%s:%s] to [%s] on host [%s]",
                    server_name, nfs_path, mnt_path, self.sh_hostname)
        nfs_commands = [("umount %s || echo -n """ % mnt_path),
                        ("ping -c 1 %s" % server_name),
                        ("test -e %s || mkdir -p %s" % (mnt_path, mnt_path)),
                        ("test -d %s" % mnt_path),
                        ("mount -t nfs %s:%s %s" %
                         (server_name, nfs_path, mnt_path)),
                        ("mount | grep %s:%s" % (server_name, nfs_path))]
        for command in nfs_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout, retval.cr_stderr)
                return -1
        log.cl_info("mounted NFS client of [%s:%s] to [%s] on host [%s]",
                    server_name, nfs_path, mnt_path, self.sh_hostname)
        return 0

    def sh_make_rsync_compatible_globs(self, log, path, is_local):
        """
        Given an rsync-style path, returns a list of globbed paths
        that will hopefully provide equivalent behaviour for scp. Does not
        support the full range of rsync pattern matching behaviour, only that
        exposed in the get/send_file interface (trailing slashes).

        The is_local param is flag indicating if the paths should be
        interpreted as local or remote paths.
        """

        # non-trailing slash paths should just work
        if len(path) == 0 or path[-1] != "/":
            return [path]

        # make a function to test if a pattern matches any files
        if is_local:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on local host
                """
                # pylint: disable=unused-argument
                return len(glob.glob(path + pattern)) > 0
        else:
            def glob_matches_files(log, path, pattern):
                """
                Match the files on remote host
                """
                result = self.sh_run(log, "ls \"%s\"%s" %
                                     (sh_escape(path), pattern))
                return result.cr_exit_status == 0

        # take a set of globs that cover all files, and see which are needed
        patterns = ["*", ".[!.]*"]
        patterns = [p for p in patterns if glob_matches_files(log, path, p)]

        # convert them into a set of paths suitable for the commandline
        if is_local:
            return ["\"%s\"%s" % (sh_escape(path), pattern)
                    for pattern in patterns]
        else:
            return [scp_remote_escape(path) + pattern
                    for pattern in patterns]

    def sh_make_scp_cmd(self, sources, dest):
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        extra_option = ""
        if self.sh_identity_file is not None:
            extra_option = ("-i %s" % self.sh_identity_file)
        command = ("scp -rqp -o StrictHostKeyChecking=no %s "
                   "%s '%s'")
        return command % (extra_option, " ".join(sources), dest)

    def sh_make_rsync_compatible_source(self, log, source, is_local):
        """
        Applies the same logic as sh_make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self.sh_make_rsync_compatible_globs(log, path, is_local)
                    for path in source), [])

    def sh_encode_remote_paths(self, paths, escape=True):
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [scp_remote_escape(path) for path in paths]
        return 'root@%s:"%s"' % (self.sh_hostname, " ".join(paths))

    def sh_set_umask_perms(self, dest):
        """
        Given a destination file/dir (recursively) set the permissions on
        all the files and directories to the max allowed by running umask.

        now this looks strange but I haven't found a way in Python to _just_
        get the umask, apparently the only option is to try to set it
        """
        # pylint: disable=no-self-use
        umask = os.umask(0)
        os.umask(umask)

        max_privs = 0777 & ~umask

        def set_file_privs(filename):
            """
            Set the privileges of a file
            """
            file_stat = os.stat(filename)

            file_privs = max_privs
            # if the original file permissions do not have at least one
            # executable bit then do not set it anywhere
            if not file_stat.st_mode & 0111:
                file_privs &= ~0111

            os.chmod(filename, file_privs)

        # try a bottom-up walk so changes on directory permissions won't cut
        # our access to the files/directories inside it
        for root, dirs, files in os.walk(dest, topdown=False):
            # when setting the privileges we emulate the chmod "X" behaviour
            # that sets to execute only if it is a directory or any of the
            # owner/group/other already has execute right
            for dirname in dirs:
                os.chmod(os.path.join(root, dirname), max_privs)

            for filename in files:
                set_file_privs(os.path.join(root, filename))

        # now set privs for the dest itself
        if os.path.isdir(dest):
            os.chmod(dest, max_privs)
        else:
            set_file_privs(dest)

    def sh_get_file(self, log, source, dest, delete_dest=False,
                    preserve_perm=True):
        """
        copy the file/dir from the host to local host

        Currently, get file is based on scp. For bettern scalability
        we should improve to rsync.
        scp has no equivalent to --delete, just drop the entire dest dir
        """
        # pylint: disable=too-many-arguments
        dest = os.path.abspath(dest)
        if isinstance(source, basestring):
            source = [source]

        if delete_dest and os.path.isdir(dest):
            shutil.rmtree(dest)
            ret = utils.mkdir(dest)
            if ret:
                log.cl_error("failed to mkdir [%s] on localhost",
                             dest)
                return -1

        remote_source = self.sh_make_rsync_compatible_source(log, source, False)
        if remote_source:
            # sh_make_rsync_compatible_source() already did the escaping
            remote_source = self.sh_encode_remote_paths(remote_source,
                                                        escape=False)
            local_dest = sh_escape(dest)
            scp = self.sh_make_scp_cmd([remote_source], local_dest)
            retval = utils.run(scp)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to get file [%s] on host [%s] to "
                             "local directory [%s], command = [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             source, self.sh_hostname, dest, scp,
                             retval.cr_exit_status, retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self.sh_set_umask_perms(dest)
        return 0

    def sh_make_rsync_cmd(self, sources, dest, delete_dest, preserve_symlinks):
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        """
        # pylint: disable=no-self-use
        ssh_cmd = make_ssh_command(identity_file=self.sh_identity_file)
        if delete_dest:
            delete_flag = "--delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = "-L"
        command = "rsync %s %s --timeout=1800 --rsh='%s' -az %s %s"
        return command % (symlink_flag, delete_flag, ssh_cmd,
                          " ".join(sources), dest)

    def sh_has_command(self, log, command):
        """
        Check whether host has a command
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if command in self.sh_cached_has_commands:
            return self.sh_cached_has_commands[command]

        ret = self.sh_run(log, "which %s" % command)
        if ret.cr_exit_status != 0:
            result = False
        else:
            result = True
        self.sh_cached_has_commands[command] = result
        return result

    def sh_has_zpool(self, log):
        """
        Check whether host has zpool command
        """
        return self.sh_has_command(log, "zpool")

    def sh_zfspool_list(self, log):
        """
        Return the ZFS pools
        """
        command = "lsmod | grep ^zfs"
        retval = self.sh_run(log, command)
        if (retval.cr_exit_status == 1 and retval.cr_stdout == ""):
            return []

        ret = self.sh_has_zpool(log)
        if not ret:
            return []

        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        command = "zpool list -o name"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        if (len(lines) < 1 or
                (lines[0] != "NAME" and lines[0] != "no pools available")):
            log.cl_error("unexpected command result [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return lines[1:]

    def sh_destroy_zfs_pools(self, log):
        """
        Destroy all ZFS pools
        """
        zfs_pools = self.sh_zfspool_list(log)
        if zfs_pools is None:
            log.cl_error("failed to list ZFS pools on host [%s]",
                         self.sh_hostname)
            return -1

        if len(zfs_pools) == 0:
            return 0

        for zfs_pool in zfs_pools:
            command = "zpool destroy %s" % (zfs_pool)
            retval = self.sh_run(log, command, timeout=10)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        zfs_pools = self.sh_zfspool_list(log)
        if zfs_pools is None:
            log.cl_error("failed to list ZFS pools on host [%s]",
                         self.sh_hostname)
            return -1

        if len(zfs_pools) > 0:
            log.cl_error("failed to destroy all ZFS pools on host [%s], "
                         "still has pools %s",
                         self.sh_hostname, zfs_pools)
            return -1
        return 0

    def sh_has_rsync(self, log):
        """
        Check whether host has rsync
        """
        return self.sh_has_command(log, "rsync")

    def sh_send_file(self, log, source, dest, delete_dest=False,
                     preserve_symlinks=False,
                     from_local=True,
                     remote_host=None):
        """
        Send file/dir from a host to another host
        If from_local is True, the file will be sent from local host;
        Otherwise, it will be sent from this host.
        If remot_host is not none, the file will be sent to that host;
        Otherwise, it will be sent to this host.
        """
        # pylint: disable=too-many-arguments
        if not self.sh_has_rsync(log):
            log.cl_debug("host [%s] doesnot have rsync, trying to install",
                         self.sh_hostname)
            ret = self.sh_run(log, "yum install rsync -y")
            if ret.cr_exit_status:
                log.cl_error("failed to install rsync")
                return -1
            self.sh_cached_has_commands["rsync"] = True

        if isinstance(source, basestring):
            source = [source]
        if remote_host is None:
            remote_host = self
        remote_dest = remote_host.sh_encode_remote_paths([dest], False)

        local_sources = [sh_escape(path) for path in source]
        rsync = remote_host.sh_make_rsync_cmd(local_sources, remote_dest,
                                              delete_dest, preserve_symlinks)
        if from_local:
            ret = utils.run(rsync)
            from_host = "local"
        else:
            from_host = self.sh_hostname
            ret = self.sh_run(log, rsync)
        if ret.cr_exit_status:
            log.cl_error("failed to send file [%s] on host [%s]  "
                         "to dest [%s] on host [%s] using rsync, "
                         "command = [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         source, from_host, dest, remote_host.sh_hostname,
                         rsync, ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_run(self, log, command, silent=False, login_name="root",
               timeout=LONGEST_SIMPLE_COMMAND_TIME, stdout_tee=None,
               stderr_tee=None, stdin=None, return_stdout=True,
               return_stderr=True, quit_func=None, flush_tee=False):
        """
        Run a command on the host
        """
        # pylint: disable=too-many-arguments
        if not silent:
            log.cl_debug("starting [%s] on host [%s]", command,
                         self.sh_hostname)
        if self.sh_local:
            ret = utils.run(command, timeout=timeout, stdout_tee=stdout_tee,
                            stderr_tee=stderr_tee, stdin=stdin,
                            return_stdout=return_stdout,
                            return_stderr=return_stderr,
                            quit_func=quit_func, flush_tee=flush_tee)
        else:
            ret = ssh_run(self.sh_hostname, command, login_name=login_name,
                          timeout=timeout,
                          stdout_tee=stdout_tee, stderr_tee=stderr_tee,
                          stdin=stdin, return_stdout=return_stdout,
                          return_stderr=return_stderr, quit_func=quit_func,
                          identity_file=self.sh_identity_file,
                          flush_tee=flush_tee)
        if not silent:
            log.cl_debug("ran [%s] on host [%s], ret = [%d], stdout = [%s], "
                         "stderr = [%s]",
                         command, self.sh_hostname, ret.cr_exit_status,
                         ret.cr_stdout, ret.cr_stderr)
        return ret

    def sh_get_kernel_ver(self, log):
        """
        Get the kernel version of the remote machine
        """
        ret = self.sh_run(log, "/bin/uname -r")
        if ret.cr_exit_status != 0:
            return None
        return ret.cr_stdout.rstrip()

    def sh_kernel_has_rpm(self, log):
        """
        Check whether the current running kernel has RPM installed, if not,
        means the RPM has been uninstalled.
        """
        kernel_version = self.sh_get_kernel_ver(log)
        if kernel_version is None:
            return False

        rpm_name = "kernel-" + kernel_version
        command = "rpm -qi %s" % rpm_name
        retval = self.sh_run(log, command)
        has_rpm = True
        if retval.cr_exit_status:
            has_rpm = False
        return has_rpm

    def sh_rpm_find_and_uninstall(self, log, find_command, option=""):
        """
        Find and uninstall RPM on the host
        """
        command = "rpm -qa | %s" % find_command
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            for rpm in retval.cr_stdout.splitlines():
                log.cl_info("uninstalling RPM [%s] on host [%s]",
                            rpm, self.sh_hostname)
                retval = self.sh_run(log, "rpm -e %s --nodeps %s" % (rpm, option))
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to uninstall RPM [%s] on host [%s], "
                                 "ret = %d, stdout = [%s], stderr = [%s]",
                                 rpm, self.sh_hostname,
                                 retval.cr_exit_status, retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1
        elif (retval.cr_exit_status == 1 and
              len(retval.cr_stdout) == 0):
            log.cl_debug("no rpm can be find by command [%s] on host [%s], "
                         "no need to uninstall",
                         command, self.sh_hostname)
        else:
            log.cl_error("unexpected result of command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_remove_dir(self, log, directory):
        """
        Remove directory recursively
        """
        dangerous_dirs = ["/"]
        for dangerous_dir in dangerous_dirs:
            if dangerous_dir == directory:
                log.cl_error("removing directory [%s] is dangerous",
                             directory)
                return -1

        ret = self.sh_run(log, "rm -fr %s" % (directory))
        if ret.cr_exit_status != 0:
            log.cl_error("failed to remove directory [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         directory, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_remove_file(self, log, fpath):
        """
        Remove file
        """
        ret = self.sh_run(log, "rm -f %s" % (fpath))
        if ret.cr_exit_status != 0:
            log.cl_error("failed to remove file [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         fpath, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_command_job(self, command, timeout=None, stdout_tee=None,
                       stderr_tee=None, stdin=None):
        """
        Return the command job on a host
        """
        # pylint: disable=too-many-arguments
        full_command = ssh_command(self.sh_hostname, command)
        job = utils.CommandJob(full_command, timeout, stdout_tee, stderr_tee,
                               stdin)
        return job

    def sh_detect_device_fstype(self, log, device):
        """
        Return the command job on a host
        """
        command = ("blkid -o value -s TYPE %s" % device)
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None
        if ret.cr_stdout == "":
            return None
        lines = ret.cr_stdout.splitlines()
        if len(lines) != 1:
            log.cl_error("command [%s] on host [%s] has unexpected output "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return None

        return lines[0]

    def sh_mkfs(self, log, device, fstype):
        """
        Format the device to a given fstype
        """
        command = ("mkfs.%s %s" % (fstype, device))
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = %d, stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         ret.cr_exit_status, ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_rmdir_if_exist(self, log, directory):
        """
        Remote an empty directory if it exists
        """
        command = ("test -e %s" % (directory))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            command = ("rmdir %s" % (directory))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            return 0
        elif retval.cr_exit_status == 1:
            return 0
        else:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return -1

    def sh_device_umount_all(self, log, device):
        """
        Umount all mounts of a device
        """
        command = ("cat /proc/mounts | grep \"%s \"" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0 and retval.cr_exit_status != 1:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            fields = line.split()
            assert fields[0] == device
            tmp_mount_point = fields[1]
            ret = self.sh_filesystem_umount(log, tmp_mount_point)
            if ret:
                log.cl_error("failed to umount [%s]", tmp_mount_point)
                return -1
        return 0

    def sh_filesystem_mounted(self, log, device, fstype=None, mount_point=None):
        """
        Check whether the device is mounted

        Device could be symbol link
        """
        if device.startswith("/"):
            command = "realpath %s" % device
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            lines = retval.cr_stdout.strip().splitlines()
            if len(lines) != 1:
                log.cl_error("unexpected output of realpath: [%s]",
                             retval.cr_stdout)
                return -1
            device = lines[0]

        command = ("cat /proc/mounts | grep \"%s \"" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0 and retval.cr_exit_status != 1:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            fields = line.split()
            assert fields[0] == device
            tmp_mount_point = fields[1]
            tmp_fstype = fields[2]
            if mount_point is None or tmp_mount_point == mount_point:
                if fstype and tmp_fstype != fstype:
                    log.cl_error("device [%s] is mounted to [%s] on host "
                                 "[%s] with type [%s], not [%s]", device,
                                 tmp_mount_point, self.sh_hostname,
                                 tmp_fstype, fstype)
                    return -1
                else:
                    return 1
        return 0

    def sh_device_mounted(self, log, device):
        """
        Whether device is mounted
        """
        return self.sh_filesystem_mounted(log, device)

    def sh_filesystem_mount(self, log, device, fstype, mount_point,
                            options=None, check_device=True):
        """
        Mount file system
        """
        # pylint: disable=too-many-return-statements,too-many-arguments
        if check_device:
            retval = self.sh_run(log, "test -b %s" % device)
            if retval.cr_exit_status != 0:
                log.cl_error("device [%s] is not a device", device)
                return -1

        retval = self.sh_run(log, "test -e %s" % mount_point)
        if retval.cr_exit_status != 0:
            retval = self.sh_run(log, "mkdir -p %s" % mount_point)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to create directory [%s]", mount_point)
                return -1

        retval = self.sh_run(log, "test -d %s" % mount_point)
        if retval.cr_exit_status != 0:
            log.cl_error("[%s] is not directory", mount_point)
            return -1

        ret = self.sh_filesystem_mounted(log, device, fstype, mount_point)
        if ret == 1:
            return 0
        elif ret < 0:
            return -1

        option_string = ""
        if options:
            option_string = ("-o %s" % options)
        command = ("mount %s -t %s %s %s" %
                   (option_string, fstype, device, mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_filesystem_umount(self, log, mount_point):
        """
        Mount file system
        """
        command = ("umount %s" % (mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_filesystem_type(self, log, path):
        """
        Mount file system
        """
        fstype = None
        command = ("df --output=fstype %s" % (path))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, fstype
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 2:
            log.cl_error("command [%s] has unexpected output, "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, fstype
        fstype = lines[1]
        return 0, fstype

    def sh_filesystem_df(self, log, mount_point):
        """
        report file system disk space usage
        """
        total = 0
        used = 0
        available = 0
        command = ("df %s" % (mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 2:
            log.cl_error("command [%s] has unexpected output on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        df_pattern = (r"^(?P<device>\S+) +(?P<total>\d+) +(?P<used>\d+) "
                      r"+(?P<available>\d+) +(?P<percentage>\S+) +%s" %
                      (mount_point))
        df_regular = re.compile(df_pattern)
        line = lines[1]
        match = df_regular.match(line)
        if not match:
            log.cl_error("command [%s] has unexpected output on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, total, used, available
        total = int(match.group("total"))
        used = int(match.group("used"))
        available = int(match.group("available"))

        return 0, total, used, available

    def sh_btrfs_df(self, log, mount_point):
        """
        report Btrfs file system disk space usage
        only return used bytes, because total bytes is not "accurate" since
        it will grow when keep on using
        """
        used = 0
        command = ("btrfs file df -b %s" % (mount_point))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, used
        lines = retval.cr_stdout.splitlines()
        if len(lines) != 6:
            log.cl_error("command [%s] has unexpected output, "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, used

        df_pattern = (r"^.+: total=(?P<total>\d+), used=(?P<used>\d+)$")
        df_regular = re.compile(df_pattern)
        # ignore GlobalReserve line
        for line in lines[:5]:
            log.cl_debug("parsing line [%s]", line)
            match = df_regular.match(line)
            if not match:
                log.cl_error("command [%s] has unexpected output, "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1, used
            used += int(match.group("used"))

        return 0, used

    def sh_dumpe2fs(self, log, device):
        """
        Dump ext4 super information
        Return a direction
        """
        info_dict = {}
        command = ("dumpe2fs -h %s" % (device))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, info_dict
        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if line == "":
                continue
            name = ""
            pointer = 0
            for character in line:
                if character != ":":
                    name += character
                    pointer += 1
                else:
                    pointer += 1
                    break

            for character in line[pointer:]:
                if character != " ":
                    break
                else:
                    pointer += 1

            value = line[pointer:]
            info_dict[name] = value
            log.cl_debug("dumpe2fs name: [%s], value: [%s]", name, value)
        return 0, info_dict

    def sh_zfs_get_srvname(self, log, device):
        """
        Get lustre:svname property of ZFS
        """
        property_name = "lustre:svname"
        command = ("zfs get -H %s %s" % (property_name, device))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        fields = retval.cr_stdout.split('\t')
        if len(fields) != 4:
            log.cl_error("invalid output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname,
                         retval.cr_stdout)
            return None

        if fields[0] != device or fields[1] != property_name:
            log.cl_error("invalid output of command [%s] on host [%s], "
                         "stdout = [%s]",
                         command, self.sh_hostname,
                         retval.cr_stdout)
            return None

        if fields[2] == "-" or fields[3] == "-":
            log.cl_error("no property [%s] of device [%s] on host [%s], "
                         "stdout = [%s]",
                         property_name, device, self.sh_hostname,
                         retval.cr_stdout)
            return None

        return fields[2]

    def sh_pkill(self, log, process_cmd, special_signal=None):
        """
        Kill the all processes that are running command
        """
        signal_string = ""
        if special_signal is not None:
            signal_string = " --signal " + special_signal

        command = ("pkill%s -f -x -c '%s'" % (signal_string, process_cmd))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     self.sh_hostname)
        retval = self.sh_run(log, command)
        if (retval.cr_stderr != "" or
                (retval.cr_exit_status != 0 and retval.cr_exit_status != 1)):
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        instance_count = retval.cr_stdout.strip()
        log.cl_debug("killed [%s] instance of multiop [%s]",
                     instance_count, process_cmd)
        return 0

    def sh_md5sum(self, log, fpath):
        """
        Calculate the md5sum of a file
        """
        command = "md5sum %s | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_gunzip_md5sum(self, log, fpath):
        """
        Use gunzip to decompress the file and then calculate the md5sum
        """
        command = "cat %s | gunzip | md5sum | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_unquip_md5sum(self, log, fpath):
        """
        Use quip to decompress the file and then calculate the md5sum
        """
        command = "cat %s | quip -d -o fastq -c | md5sum | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0 or retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_truncate(self, log, fpath, size):
        """
        Truncate the size of a file
        """
        command = "truncate -s %s %s" % (size, fpath)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return retval.cr_exit_status
        return 0

    def sh_fill_random_binary_file(self, log, fpath, size):
        """
        Generate random binary file with the given size
        """
        block_count = (size + 1048575) / 1048576
        if block_count == 1:
            command = "dd if=/dev/urandom of=%s bs=%s count=1" % (fpath, size)
            written = size
        else:
            command = ("dd if=/dev/urandom of=%s bs=1M count=%s" %
                       (fpath, block_count))
            written = 1048576 * block_count

        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return retval.cr_exit_status
        if written == size:
            return 0
        else:
            return self.sh_truncate(log, fpath, size)

    def sh_rpm_query(self, log, rpm_name):
        """
        Find RPM on the host
        """
        command = "rpm -q %s" % rpm_name
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            return -1
        return 0

    def sh_yumdb_info(self, log, rpm_name):
        """
        Get the key/value pairs of a RPM from yumdb
        """
        command = "yumdb info %s" % rpm_name
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        lines = retval.cr_stdout.splitlines()
        output_pattern = (r"^ +(?P<key>\S+) = (?P<value>.+)$")
        output_regular = re.compile(output_pattern)
        infos = {}
        for line in lines:
            match = output_regular.match(line)
            if match:
                log.cl_debug("matched pattern [%s] with line [%s]",
                             output_pattern, line)
                key = match.group("key")
                value = match.group("value")
                infos[key] = value

        return infos

    def sh_yumdb_sha256(self, log, rpm_name):
        """
        Get the SHA256 checksum of a RPM from yumdb
        """
        rpm_infos = self.sh_yumdb_info(log, rpm_name)
        if rpm_infos is None:
            log.cl_error("failed to get YUM info of [%s] on host [%s]",
                         rpm_name, self.sh_hostname)
            return None

        if ("checksum_data" not in rpm_infos or
                "checksum_type" not in rpm_infos):
            log.cl_error("failed to get YUM info of [%s] on host [%s]",
                         rpm_name, self.sh_hostname)
            return None

        if rpm_infos["checksum_type"] != "sha256":
            log.cl_error("unexpected checksum type of RPM [%s] on host [%s], "
                         "expected [sha256], got [%s]",
                         rpm_name, self.sh_hostname,
                         rpm_infos["checksum_type"])
            return None

        return rpm_infos["checksum_data"]

    def sh_sha256sum(self, log, fpath):
        """
        Calculate the sha256sum of a file
        """
        command = "sha256sum %s | awk '{print $1}'" % fpath
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def sh_virsh_dominfo(self, log, hostname):
        """
        Get the virsh dominfo of a domain
        """
        command = ("virsh dominfo %s" % hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_debug("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        lines = retval.cr_stdout.splitlines()
        output_pattern = (r"^(?P<key>.+): +(?P<value>.+)$")
        output_regular = re.compile(output_pattern)
        infos = {}
        for line in lines:
            match = output_regular.match(line)
            if match:
                log.cl_debug("matched pattern [%s] with line [%s]",
                             output_pattern, line)
                key = match.group("key")
                value = match.group("value")
                infos[key] = value

        return infos

    def sh_virsh_dominfo_state(self, log, hostname):
        """
        Get the state of a hostname
        """
        dominfos = self.sh_virsh_dominfo(log, hostname)
        if dominfos is None:
            log.cl_debug("failed to get dominfo of [%s] on host [%s]",
                         hostname, self.sh_hostname)
            return None

        if "State" not in dominfos:
            log.cl_debug("no [State] in dominfo of [%s] on host [%s]",
                         hostname, self.sh_hostname)
            return None
        return dominfos["State"]

    def sh_virsh_detach_domblks(self, log, vm_hostname):
        """
        Detach the disk from the host
        """
        command = ("virsh domblklist --inactive --details %s" %
                   (vm_hostname))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            fields = line.split()
            if (len(fields) != 4 or
                    (fields[0] == "Type" and fields[1] == "Device")):
                continue
            device_type = fields[1]
            target_name = fields[2]
            if device_type != "disk":
                continue

            # Don't detach the disks that can't be detached
            if target_name.startswith("hd"):
                continue

            log.cl_info("detaching disk [%s] of VM [%s]",
                        target_name, vm_hostname)

            command = ("virsh detach-disk %s %s --persistent" %
                       (vm_hostname, target_name))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def sh_selinux_status(self, log):
        """
        Check the current status of SELinux
        """
        command = "getenforce"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0 or retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_disable_selinux(self, log):
        """
        Disable SELinux permanently
        """
        command = "sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0 or retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        status = self.sh_selinux_status(log)
        if status == "Disabled" or status == "Permissive":
            log.cl_debug("SELinux is already [%s] on host [%s]",
                         status, self.sh_hostname)
            return 0

        command = "setenforce 0"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_disable_dns(self, log):
        """
        Disable DNS
        """
        command = "> /etc/resolv.conf"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_enable_dns(self, log, dns):
        """
        Disable DNS
        """
        command = "echo 'nameserver %s' > /etc/resolv.conf" % dns
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_check_network_connection(self, log, remote_host, quiet=False):
        """
        Check whether the Internet connection works well
        """
        command = "ping -c 1 %s" % remote_host
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            if not quiet:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command, self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_check_internet(self, log):
        """
        Check whether the Internet connection works well
        """
        ret = self.sh_check_network_connection(log, "www.bing.com")
        if ret:
            return 0

        return self.sh_check_network_connection(log, "www.baidu.com")

    def sh_ping(self, log, silent=False):
        """
        Check whether local host can ping this host
        """
        command = "ping -c 1 %s" % self.sh_hostname
        retval = utils.run(command, silent=silent)
        if retval.cr_exit_status:
            if not silent:
                log.cl_error("failed to run command [%s] on local host host, "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            return -1
        return 0

    def sh_kernel_set_default(self, log, kernel):
        """
        Set the default boot kernel
        Example of kernel string:
        /boot/vmlinuz-2.6.32-573.22.1.el6_lustre.2.7.15.3.x86_64
        """
        if self.sh_distro(log) == DISTRO_RHEL7:
            # This is not necessary for normal cases, but just in case of
            # broken grubenv file caused by repair
            command = ("grub2-editenv create")
            ret = self.sh_run(log, command)
            if ret.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s]"
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             ret.cr_exit_status,
                             ret.cr_stdout,
                             ret.cr_stderr)
                return -1
        command = ("grubby --set-default=%s" % (kernel))
        ret = self.sh_run(log, command)
        if ret.cr_exit_status != 0 or ret.cr_stderr != "":
            log.cl_error("failed to choose default kernel on host [%s] "
                         "because command [%s] failed, "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         self.sh_hostname,
                         command,
                         ret.cr_exit_status,
                         ret.cr_stdout,
                         ret.cr_stderr)
            return -1
        return 0

    def sh_reboot_issue(self, log, force=False):
        """
        Issuing the reboot command on host
        """
        log.cl_debug("issuing rebooting of host [%s]",
                     self.sh_hostname)

        # Check whether the host is up first, to avoid unncessary wait
        if not self.sh_is_up(log):
            log.cl_error("unable to issue reboot of host [%s] because it is "
                         "not up", self.sh_hostname)
            return -1

        if force:
            retval = self.sh_run(log, "echo b > /proc/sysrq-trigger &")
        else:
            retval = self.sh_run(log, "reboot &")

        # We can't really trust the return value or output, sometimes the
        # reboot is so quick that the pipe might be broken and print
        # "Write failed: Broken pipe", so do not check the return value or
        # outputs at all, just wait and check after running the command.
        log.cl_debug("issued reboot on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     self.sh_hostname, retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return 0

    def sh_uptime_update(self, log):
        """
        update sh_latest_uptime to detect reboot
        """
        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            log.cl_error("failed to get uptime on host [%s]",
                         self.sh_hostname)
            return -1

        self.sh_latest_uptime = uptime
        return 0

    def sh_rebooted(self, log, uptime_before_reboot):
        """
        Check whether the host rebooted
        """
        if uptime_before_reboot == 0:
            log.cl_debug("host [%s] does not even start rebooting",
                         self.sh_hostname)
            return False

        if not self.sh_is_up(log):
            log.cl_debug("host [%s] is not up yet",
                         self.sh_hostname)
            return False

        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            return False

        if (uptime_before_reboot + SHORTEST_TIME_REBOOT >
                uptime):
            log.cl_debug("the uptime of host [%s] doesn't look "
                         "like rebooted, uptime now: [%d], uptime "
                         "before reboot: [%d], keep on waiting",
                         self.sh_hostname, uptime,
                         uptime_before_reboot)
            return False
        return True

    def sh_wait_reboot(self, log, uptime_before_rebooted):
        """
        Wait until host rebooted
        """
        log.cl_info("waiting until host [%s] rebooted", self.sh_hostname)
        time_start = time.time()
        sleep_interval = 1
        while not self.sh_rebooted(log, uptime_before_rebooted):
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed < LONGEST_TIME_REBOOT:
                time.sleep(sleep_interval)
                continue
            log.cl_error("booting of host [%s] takes too long",
                         self.sh_hostname)
            return -1
        return 0

    def sh_reboot(self, log):
        """
        Reboot the host
        """
        # pylint: disable=too-many-return-statements
        log.cl_info("reboot of host [%s] started",
                    self.sh_hostname)
        none_force_issued = False
        uptime = self.sh_get_uptime(log)
        if uptime < 0:
            log.cl_error("failed to get uptime on host [%s]",
                         self.sh_hostname)
            return -1

        force = False
        ret = self.sh_run(log, "sync", timeout=120)
        if ret.cr_exit_status != 0:
            log.cl_error("failed to sync on host [%s], force reboot",
                         self.sh_hostname)
            force = True

        if not force:
            ret = self.sh_reboot_issue(log, force=False)
            if ret:
                log.cl_error("failed to issue none-force reboot on host [%s]",
                             self.sh_hostname)
                return -1

            none_force_issued = True
            if self.sh_wait_reboot(log, uptime):
                log.cl_info("none-force reboot of host [%s] failed, trying "
                            "force reboot", self.sh_hostname)
            else:
                ret = self.sh_uptime_update(log)
                if ret:
                    log.cl_error("failed to update uptime on host [%s]",
                                 self.sh_hostname)
                    return -1
                log.cl_info("none-force reboot of host [%s] finished",
                            self.sh_hostname)
                return 0

        ret = self.sh_reboot_issue(log, force=True)
        if ret:
            if not none_force_issued:
                log.cl_error("failed to issue force reboot on host [%s]",
                             self.sh_hostname)
                return -1
            else:
                log.cl_info("failed to issue force reboot on host [%s], but "
                            "none-force reboot was issued successfully, "
                            "waiting again in case reboot takes longer than "
                            "expected",
                            self.sh_hostname)

        if self.sh_wait_reboot(log, uptime):
            log.cl_error("reboot of host [%s] failed",
                         self.sh_hostname)
            return -1

        ret = self.sh_uptime_update(log)
        if ret:
            log.cl_error("can't update uptime on host [%s]",
                         self.sh_hostname)
            return -1
        log.cl_info("reboot of host [%s] finished",
                    self.sh_hostname)
        return 0

    def sh_lustre_umount(self, log):
        """
        Umount all file systems of Lustre
        """
        retval = self.sh_run(log, "mount | grep 'type lustre' | awk '{print $1}'")
        if retval.cr_exit_status != 0:
            log.cl_error("failed to get lustre mount points on host "
                         "[%s]",
                         self.sh_hostname)
            return -1

        ret = 0
        devices = retval.cr_stdout.splitlines()
        # Umount client first, so as to prevent dependency
        for device in devices[:]:
            if device.startswith("/dev"):
                continue
            ret = self.sh_umount(log, device)
            if ret:
                break
            devices.remove(device)
        if ret == 0:
            for device in devices:
                ret = self.sh_umount(log, device)
                if ret:
                    break
        return ret

    def sh_chattr_has_projid_support(self, log):
        """
        Check whether chattr command has projid support
        """
        command = "man chattr | grep project"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return False
        return True

    def sh_lsscsi(self, log):
        """
        Return the devices
        Format:
        [scsi_host:channel:target_number:LUN tuple] peripheral_type \
        vendor_name model_name revision_string primary_device_node_name
        """
        command = "lsscsi -s"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, []

        lines = retval.cr_stdout.splitlines()
        output_pattern = (r"^.+ +(?P<device>\S+) +(?P<size>\S+)$")
        output_regular = re.compile(output_pattern)
        devices = []
        for line in lines:
            match = output_regular.match(line)
            if match:
                log.cl_debug("matched pattern [%s] with line [%s]",
                             output_pattern, line)
                device = match.group("device")
                devices.append(device)
        return 0, devices

    def sh_device_serial(self, log, device):
        """
        Return the serial of a device
        """
        command = "udevadm info %s | grep ID_SERIAL= | awk -F = '{print $2}'" % device
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_yum_repo_ids(self, log):
        """
        Get the repo IDs of yum
        """
        command = "yum repolist -v | grep Repo-id | awk '{print $3}'"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        repo_ids = []
        lines = retval.cr_stdout.splitlines()
        for line in lines[1:]:
            fields = line.split()
            repo_field = fields[0]
            # Some repo ID looks like "base/7/x86_64"
            repo_fields = repo_field.split("/")
            repo_id = repo_fields[0]
            repo_ids.append(repo_id)
        return repo_ids

    def sh_uuid(self, log):
        """
        Get the uuid of the host
        """
        command = "dmidecode -s system-uuid"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None

        return retval.cr_stdout.strip()

    def sh_file_executable(self, log, fpath):
        """
        Check the file is executable
        """
        command = ("test -f %s && test -x %s " % (fpath, fpath))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_service_stop(self, log, service_name):
        """
        Stop the service if it is running
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status and retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        elif retval.cr_stdout == "unknown\n":
            return 0
        elif retval.cr_stdout == "inactive\n":
            return 0
        elif retval.cr_stdout == "active\n":
            command = ("systemctl stop %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status and retval.cr_stderr != "":
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
            return 0
        log.cl_error("unkown output of command [%s] on host [%s]: [%s]",
                     command, self.sh_hostname, retval.cr_stdout)
        return -1

    def sh_service_disable(self, log, service_name):
        """
        Disable the service from starting automatically
        """
        command = ("systemctl is-active %s" % (service_name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status and retval.cr_stderr != "":
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        elif retval.cr_stdout == "unknown\n":
            return 0
        else:
            command = ("systemctl disable %s" % (service_name))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status and retval.cr_stderr != "":
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def sh_create_inode(self, log, inode_path, inode_type=stat.S_IFREG,
                        major=None, minor=None, path=None):
        """
        Create inode with a specifi type
        """
        # pylint: disable=too-many-arguments
        if inode_type == stat.S_IFREG:
            command = ("touch %s" % (inode_path))
        elif inode_type == stat.S_IFDIR:
            command = ("mkdir %s" % (inode_path))
        elif inode_type == stat.S_IFBLK:
            command = ("mknod %s b %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFCHR:
            command = ("mknod %s c %s %s" % (inode_path, major, minor))
        elif inode_type == stat.S_IFLNK:
            command = ("ln -s %s %s" % (path, inode_path))
        elif inode_type == stat.S_IFSOCK:
            command = ("python -c \"import socket;"
                       "sock = socket.socket(socket.AF_UNIX);"
                       "sock.bind('%s')\"" % (inode_path))
        elif inode_type == stat.S_IFIFO:
            command = ("mknod %s p" % (inode_path))
        else:
            log.cl_error("unkown type [%s]", inode_type)
            return -1

        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_remove_inode(self, log, inode_path, inode_type=stat.S_IFREG):
        """
        Create inode with a specifi type
        """
        if inode_type == stat.S_IFDIR:
            command = ("rmdir %s" % (inode_path))
        else:
            command = ("rm -f %s" % (inode_path))

        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sh_watched_run(self, log, command, stdout_file, stderr_file,
                       silent=False, login_name="root",
                       timeout=None, stdin=None, return_stdout=True,
                       return_stderr=True, quit_func=None, flush_tee=False):
        """
        Run the command and watching the output
        """
        # pylint: disable=too-many-arguments,too-many-locals
        if not silent:
            log.cl_debug("start to run command [%s] on host [%s]", command,
                         self.sh_hostname)
        log.cl_change_config(simple_console=True,
                             resultsdir=log.cl_resultsdir)
        args = {}
        args[watched_io.WATCHEDIO_LOG] = log
        args[watched_io.WATCHEDIO_HOSTNAME] = self.sh_hostname
        stdout_fd = watched_io.watched_io_open(stdout_file,
                                               watched_io.log_watcher_info_simplified,
                                               args)
        stderr_fd = watched_io.watched_io_open(stderr_file,
                                               watched_io.log_watcher_error_simplified,
                                               args)
        retval = self.sh_run(log, command, stdout_tee=stdout_fd,
                             stderr_tee=stderr_fd, silent=silent,
                             login_name=login_name, timeout=timeout,
                             stdin=stdin, return_stdout=return_stdout,
                             return_stderr=return_stderr, quit_func=quit_func,
                             flush_tee=flush_tee)
        log.cl_change_config(simple_console=False,
                             resultsdir=log.cl_resultsdir)
        stdout_fd.close()
        stderr_fd.close()
        if not silent:
            log.cl_debug("finished command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname, retval.cr_exit_status,
                         retval.cr_stdout, retval.cr_stderr)
        return retval

    def sh_get_and_clean_dir(self, log, remote_dir, local_dir):
        """
        Get and clean log
        """
        ret = self.sh_get_file(log, remote_dir, local_dir)
        if ret:
            log.cl_error("failed to get directory [%s] on host [%s] to "
                         "directory [%s] on local host",
                         remote_dir, self.sh_hostname, local_dir)
            log.cl_error("please backup the log [%s] on host "
                         "[%s] manually for debug purpose",
                         remote_dir, self.sh_hostname)
            return -1
        else:
            log.cl_info("got directory [%s] on host [%s] to directory [%s] on "
                        "localhost", remote_dir, self.sh_hostname,
                        local_dir)

        command = ("rm -fr %s" % (remote_dir))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            log.cl_error("a copy of the log dir is under [%s] of localhost, "
                         "so please clean up the log [%s] on host [%s] "
                         "manually to avoid exhaustion of disk space on that "
                         "host", local_dir, self.sh_hostname, remote_dir)
            return -1
        return 0

    def sh_get_file_size(self, log, path):
        """
        Return the file size, return -1 on error
        """
        command = "stat --printf=%s " + path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        try:
            size = int(retval.cr_stdout)
        except ValueError:
            size = -1
            log.cl_error("unexpected result of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
        return size
