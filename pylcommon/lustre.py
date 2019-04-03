# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Lustre file system management
"""
# pylint: disable=too-many-lines
import os
import re
import time

# Local libs
from pylcommon import utils
from pylcommon import ssh_host
from pylcommon import cstr
from pylcommon import rwlock

EPEL_RPM_RHEL6_RPM = ("http://download.fedoraproject.org/pub/epel/6/x86_64/"
                      "epel-release-6-8.noarch.rpm")
# The directory path that has Lustre test script
LUSTRE_TEST_SCRIPT_DIR = "/usr/lib64/lustre/tests"

LUSTRE_SERVICE_TYPE_MGS = "MGS"
LUSTRE_SERVICE_TYPE_MDT = "MDT"
LUSTRE_SERVICE_TYPE_OST = "OST"

JOBID_VAR_PROCNAME_UID = "procname_uid"

PARAM_PATH_OST_IO = "ost.OSS.ost_io"
PARAM_PATH_MDT = "mds.MDS.mdt"
PARAM_PATH_MDT_READPAGE = "mds.MDS.mdt_readpage"
PARAM_PATH_MDT_SETATTR = "mds.MDS.mdt_setattr"
TBF_TYPE_GENERAL = "general"
TBF_TYPE_UID = "uid"
TBF_TYPE_GID = "gid"
TBF_TYPE_JOBID = "jobid"
TBF_TYPE_OPCODE = "opcode"
TBF_TYPE_NID = "nid"

BACKFSTYPE_ZFS = "zfs"
BACKFSTYPE_LDISKFS = "ldiskfs"


def lustre_string2index(index_string):
    """
    Transfer string to index number, e.g.
    "000e" -> 14
    """
    index_number = int(index_string, 16)
    if index_number > 0xffff:
        return -1, ""
    return 0, index_number


def lustre_index2string(index_number):
    """
    Transfer number to index string, e.g.
    14 -> "000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "%04x" % index_number
    return 0, index_string


def lustre_ost_index2string(index_number):
    """
    Transfer number to OST index string, e.g.
    14 -> "OST000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "OST%04x" % index_number
    return 0, index_string


def lustre_mdt_index2string(index_number):
    """
    Transfer number to MDT index string, e.g.
    14 -> "MDT000e"
    """
    if index_number > 0xffff:
        return -1, ""
    index_string = "MDT%04x" % index_number
    return 0, index_string


def version_value(major, minor, patch):
    """
    Return a numeric version code based on a version string.  The version
    code is useful for comparison two version strings to see which is newer.
    """
    value = (major << 16) | (minor << 8) | patch
    return value


class LustreServiceInstance(object):
    """
    A Lustre MGS might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments,too-many-instance-attributes
    def __init__(self, log, service, host, device, mnt, nid,
                 zpool_create=None):
        self.lsi_service = service
        self.lsi_host = host
        self.lsi_device = device
        self.lsi_mnt = mnt
        self.lsi_nid = nid
        self.lsi_lock = rwlock.RWLock()
        self.lsi_service_instance_name = host.sh_hostname + ":" + device
        if zpool_create is None and service.ls_backfstype == BACKFSTYPE_ZFS:
            reason = ("no zpool_create configured for ZFS service instance %s" %
                      (self.lsi_service_instance_name))
            log.cl_error(reason)
            raise Exception(reason)
        self.lsi_zpool_create = zpool_create
        ret = service.ls_instance_add(log, self)
        if ret:
            reason = ("failed to add instance of service")
            log.cl_error(reason)
            raise Exception(reason)

    def _lsi_real_device(self, log):
        """
        Sometimes, the device could be symbol like, so return the real block
        device
        """
        if self.lsi_service.ls_backfstype == BACKFSTYPE_ZFS:
            return 0, self.lsi_device

        command = "readlink -f %s" % self.lsi_device
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.lsi_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        return 0, retval.cr_stdout.strip()

    def _lsi_format(self, log):
        """
        Format this service device
        Read lock of the host and write lock of the instance should be held
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname
        backfstype = service.ls_backfstype

        log.cl_stdout("formatting instance [%s] of service [%s] on host [%s]",
                      instance_name, service_name, hostname)
        service = self.lsi_service
        mgs_nid_string = ""
        if service.ls_service_type == LUSTRE_SERVICE_TYPE_MGS:
            nids = service.ls_nids()
        else:
            nids = service.ls_lustre_fs.lf_mgs_nids()
        for mgs_nid in nids:
            if mgs_nid_string != "":
                mgs_nid_string += ":"
            mgs_nid_string += mgs_nid
        ret, service_string = service.ls_service_string()
        if ret:
            log.cl_stderr("failed to get the service string of service [%s]",
                          service_name)
            return -1

        if backfstype == BACKFSTYPE_ZFS:
            fields = self.lsi_device.split("/")
            if len(fields) != 2:
                log.cl_stderr("unexpected device [%s] for service instance "
                              "[%s], should have the format of [pool/dataset]",
                              self.lsi_device, instance_name)
                return -1
            zfs_pool = fields[0]

            zfs_pools = host.sh_zfspool_list(log)
            if zfs_pools is None:
                log.cl_error("failed to list ZFS pools on host [%s]",
                             host.sh_hostname)
                return -1

            if zfs_pool in zfs_pools:
                command = "zpool destroy %s" % (zfs_pool)
                retval = host.sh_run(log, command, timeout=10)
                if retval.cr_exit_status:
                    log.cl_error("failed to run command [%s] on host [%s], "
                                 "ret = [%d], stdout = [%s], stderr = [%s]",
                                 command,
                                 host.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

            fields = self.lsi_zpool_create.split()
            if len(fields) <= 1:
                log.cl_stderr("unexpected zpool_create command [%s] for "
                              "instance [%s]", self.lsi_zpool_create,
                              self.lsi_service_instance_name)
                return -1

            # Get rid of the symbol links to avoid following error from
            # zpool_create:
            # missing link: ... was partitioned but ... is missing
            zpool_create = fields[0]
            for field in fields[1:]:
                if field.startswith("/"):
                    command = "readlink -f %s" % field
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
                    zpool_create += " " + retval.cr_stdout.strip()
                else:
                    zpool_create += " " + field

            retval = host.sh_run(log, zpool_create)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              zpool_create,
                              hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        if service.ls_service_type == LUSTRE_SERVICE_TYPE_MGS:
            type_argument = "--mgs"
            index_argument = ""
            fsname_argument = ""
        elif service.ls_service_type == LUSTRE_SERVICE_TYPE_OST:
            type_argument = "--ost"
            index_argument = " --index=%s" % service.ls_index
            fsname_argument = " --fsname %s" % service.ls_lustre_fs.lf_fsname
        elif service.ls_service_type == LUSTRE_SERVICE_TYPE_MDT:
            type_argument = "--mdt"
            if service.lmdt_is_mgs:
                type_argument += " --mgs"
            index_argument = " --index=%s" % service.ls_index
            fsname_argument = " --fsname %s" % service.ls_lustre_fs.lf_fsname
        else:
            log.cl_stderr("unsupported service type [%s]",
                          service.ls_service_type)
            return -1
        command = ("mkfs.lustre%s %s %s "
                   "--reformat --backfstype=%s --mgsnode=%s%s" %
                   (fsname_argument, type_argument, service_string,
                    backfstype, mgs_nid_string, index_argument))
        command += " " + self.lsi_device

        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        log.cl_stdout("formatted instance [%s] of service [%s] on host [%s]",
                      instance_name, service_name, hostname)
        return 0

    def lsi_format(self, log):
        """
        Format this Lustre service device
        """
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        host_handle = host.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting formating instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        instance_handle = self.lsi_lock.rwl_writer_acquire(log)
        if instance_handle is None:
            host_handle.rwh_release()
            log.cl_stderr("aborting formating instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        ret = self._lsi_format(log)
        instance_handle.rwh_release()
        host_handle.rwh_release()
        return ret

    def _lsi_mount(self, log):
        """
        Mount this Lustre service device
        Read lock of the host and write lock of the instance should be held
        """
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        log.cl_stdout("mounting Lustre service [%s] on host [%s]",
                      service_name, hostname)
        command = ("mkdir -p %s && mount -t lustre %s %s" %
                   (self.lsi_mnt, self.lsi_device, self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to mounting Lustre service [%s] "
                          "using command [%s] on host [%s], ret = [%d], "
                          "stdout = [%s], stderr = [%s]",
                          service_name, command, hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        log.cl_stdout("mounted Lustre service [%s] on host [%s]", service_name,
                      hostname)
        return 0

    def lsi_mount(self, log):
        """
        Mount this Lustre service device
        """
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        host_handle = host.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting mounting instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        instance_handle = self.lsi_lock.rwl_writer_acquire(log)
        if instance_handle is None:
            host_handle.rwh_release()
            log.cl_stderr("aborting mounting instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        ret = self._lsi_mount(log)
        instance_handle.rwh_release()
        host_handle.rwh_release()
        return ret

    def _lsi_umount(self, log):
        """
        Umount this Lustre service device
        Read lock of the host and write lock of the instance should be held
        """
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        log.cl_stdout("umounting Lustre service [%s] on host [%s]",
                      service_name, hostname)

        command = ("umount %s" % (self.lsi_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        log.cl_stdout("umounted Lustre service [%s] on host [%s]",
                      service_name, hostname)
        return 0

    def lsi_umount(self, log):
        """
        Umount this Lustre service device
        """
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        host_handle = host.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting umounting instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        instance_handle = self.lsi_lock.rwl_writer_acquire(log)
        if instance_handle is None:
            host_handle.rwh_release()
            log.cl_stderr("aborting umounting instance [%s] of service [%s] "
                          "on host [%s]", instance_name, service_name,
                          hostname)
            return -1
        ret = self._lsi_umount(log)
        instance_handle.rwh_release()
        host_handle.rwh_release()
        return ret

    def _lsi_check_mounted(self, log):
        """
        Return 1 when service is mounted
        Return 0 when service is not mounted
        Return negative when error
        Read lock of the host and read lock of the instance should be held
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-return-statements
        host = self.lsi_host
        hostname = host.sh_hostname
        service = self.lsi_service
        service_type = service.ls_service_type
        service_instance_name = self.lsi_service_instance_name

        server_pattern = (r"^(?P<device>\S+) (?P<mount_point>\S+) lustre .+$")
        server_regular = re.compile(server_pattern)

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        if service_type == LUSTRE_SERVICE_TYPE_OST:
            service_pattern = (r"^(?P<fsname>\S+)-OST(?P<index_string>[0-9a-f]{4})$")
            service_regular = re.compile(service_pattern)
        elif service_type == LUSTRE_SERVICE_TYPE_MDT:
            service_pattern = (r"^(?P<fsname>\S+)-MDT(?P<index_string>[0-9a-f]{4})$")
            service_regular = re.compile(service_pattern)

        mgs_pattern = (r"MGS")
        mgs_regular = re.compile(mgs_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret, real_device = self._lsi_real_device(log)
        if ret:
            log.cl_error("failed to get the real service device of instance "
                         "[%s] on host [%s]",
                         service_instance_name, hostname)
            return -1

        ret = 0
        for line in retval.cr_stdout.splitlines():
            # log.cl_debug("checking line [%s]", line)
            match = server_regular.match(line)
            if not match:
                continue

            device = match.group("device")
            mount_point = match.group("mount_point")

            # Skip the Clients
            match = client_regular.match(line)
            if match:
                continue

            if device == real_device:
                if mount_point != self.lsi_mnt:
                    log.cl_error("Lustre service device [%s] is mounted on "
                                 "host [%s], but on mount point [%s], not "
                                 "on [%s]",
                                 device, hostname,
                                 mount_point, self.lsi_mnt)
                    return -1
            else:
                if mount_point == self.lsi_mnt:
                    log.cl_error("A Lustre service [%s] is mounted on "
                                 "mount point [%s] of host [%s], but that "
                                 "is not instance [%s]", device, mount_point,
                                 hostname, service_instance_name)
                    return -1
                continue

            ret, label = host.lsh_lustre_device_label(log, device)
            if ret:
                log.cl_error("failed to get the label of device [%s] on "
                             "host [%s]", device, hostname)
                return -1

            if service_type == LUSTRE_SERVICE_TYPE_MGS:
                match = mgs_regular.match(label)
                if match:
                    log.cl_debug("MGS [%s] mounted on dir [%s] of host [%s]",
                                 device, mount_point, hostname)
                    ret = 1
                    break
            elif (service_type == LUSTRE_SERVICE_TYPE_OST or
                  service_type == LUSTRE_SERVICE_TYPE_MDT):
                fsname = service.ls_lustre_fs.lf_fsname
                match = service_regular.match(label)
                if match:
                    device_fsname = match.group("fsname")
                    index_string = match.group("index_string")
                    ret, device_index = lustre_string2index(index_string)
                    if ret:
                        log.cl_error("invalid label [%s] of device [%s] on "
                                     "host [%s]", label, device, hostname)
                        return -1

                    if device_fsname != fsname:
                        log.cl_error("unexpected fsname [%s] of device [%s] on "
                                     "host [%s], expected fsname is [%s]",
                                     device_fsname, device, hostname,
                                     fsname)
                        return -1

                    if device_index != service.ls_index:
                        log.cl_error("unexpected index [%s] of device [%s] on "
                                     "host [%s], expected index is [%s]",
                                     device_index, device, hostname,
                                     service.ls_index)
                        return -1
                    log.cl_debug("service of file system [%s] mounted on "
                                 "dir [%s] of host [%s]",
                                 fsname, mount_point, hostname)
                    ret = 1
                    break
            else:
                log.cl_error("unsupported service type [%s]",
                             service_type)

        return ret

    def lsi_check_mounted(self, log):
        """
        Return 1 when service is mounted
        Return 0 when service is not mounted
        Return negative when error
        """
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        host_handle = host.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting checking whether instance [%s] of "
                          "service [%s] is moutned on host [%s]",
                          instance_name, service_name, hostname)
            return -1
        instance_handle = self.lsi_lock.rwl_reader_acquire(log)
        if instance_handle is None:
            host_handle.rwh_release()
            log.cl_stderr("aborting checking whether instance [%s] of "
                          "service [%s] is moutned on host [%s]",
                          instance_name, service_name, hostname)
            return -1

        ret = self._lsi_check_mounted(log)
        instance_handle.rwh_release()
        host_handle.rwh_release()
        return ret

    def lsi_encode(self, need_status, status_funct, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        # pylint: disable=unused-argument
        if not need_status and not need_structure:
            return []
        encoded = {cstr.CSTR_SERVICE_INSTANCE_NAME: self.lsi_service_instance_name}
        if need_structure:
            encoded[cstr.CSTR_HOSTNAME] = self.lsi_host.sh_hostname
            encoded[cstr.CSTR_DEVICE] = self.lsi_device
            encoded[cstr.CSTR_NID] = self.lsi_nid
        if need_status:
            service = self.lsi_service
            service_name = service.ls_service_name
            service_status = status_funct(service_name)
            if service_status is None:
                encoded[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_UNKNOWN
            else:
                mounted = bool(service_status.lss_mounted_instance == self)
                if mounted:
                    encoded[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_TRUE
                else:
                    encoded[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_FALSE
        return encoded


class LustreServiceStatus(object):
    """
    The status of a Lustre service
    """
    def __init__(self, service):
        self.lss_service = service
        # The time the status is updated
        self.lss_update_time = time.time()
        self.lss_mounted_instance = None

    def lss_check(self, log):
        """
        Check the status of the service and update the update time
        """
        service = self.lss_service
        instance = service.ls_mounted_instance(log)
        if instance is not None:
            log.cl_debug("service [%s] is mounted on host [%s]",
                         service.ls_service_name,
                         instance.lsi_host.sh_hostname)
        self.lss_mounted_instance = instance
        self.lss_update_time = time.time()

    def lss_has_problem(self):
        """
        If the status of the service has problem, return True
        Else, return False
        """
        return bool(self.lss_mounted_instance is None)

    def lss_fix_problem(self, log):
        """
        Fix the problem of the service
        """
        service = self.lss_service
        service_type = service.ls_service_type
        not_mgs = bool(service_type != LUSTRE_SERVICE_TYPE_MGS)
        if not_mgs:
            lustrefs = service.ls_lustre_fs
        service_name = service.ls_service_name
        log.cl_info("fixing the service [%s]", service_name)

        if self.lss_mounted_instance is None:
            if not_mgs:
                if lustrefs.lf_mgs is not None:
                    mgs_lock_handle = lustrefs.lf_mgs.ls_lock.rwl_reader_acquire(log)
                    if mgs_lock_handle is None:
                        log.cl_stderr("aborting fixing service [%s]",
                                      service_name)
                        return -1
                fs_lock_handle = lustrefs.lf_lock.rwl_reader_acquire(log)
                if fs_lock_handle is None:
                    if lustrefs.lf_mgs is not None:
                        mgs_lock_handle.rwh_release()
                    log.cl_stderr("aborting fixing service [%s]",
                                  service_name)
                    return -1
            ret = service.ls_mount(log)
            if not_mgs:
                fs_lock_handle.rwh_release()
                if lustrefs.lf_mgs is not None:
                    mgs_lock_handle.rwh_release()
            if ret:
                log.cl_stderr("failed to fix service [%s] by mounting it",
                              service_name)
                return ret
        log.cl_info("fixed the service [%s]", service_name)
        return 0

    def lss_encode(self, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        instance = self.lss_mounted_instance
        if instance is not None:
            mounted_instance = instance.lsi_encode(False, None, need_structure)
            mounted = cstr.CSTR_TRUE
        else:
            mounted_instance = None
            mounted = cstr.CSTR_FALSE
        encoded = {cstr.CSTR_SERVICE_NAME: self.lss_service.ls_service_name,
                   cstr.CSTR_UPDATE_TIME: self.lss_update_time,
                   cstr.CSTR_IS_MOUNTED: mounted}

        if need_structure and mounted == cstr.CSTR_TRUE:
            encoded[cstr.CSTR_MOUNTED_INSTANCE] = mounted_instance

        return encoded


class LustreService(object):
    """
    Lustre service parent class for MDT/MGS/OST
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, log, lustre_fs, service_type, index,
                 backfstype):
        # pylint: disable=too-many-arguments
        self.ls_lustre_fs = lustre_fs
        # Keys are lsi_service_instance_name, values are LustreServiceInstance
        self.ls_instances = {}
        self.ls_service_type = service_type
        self.ls_lock = rwlock.RWLock()
        if service_type == LUSTRE_SERVICE_TYPE_MGS:
            assert index == 0
            self.ls_index_string = "MGS"
        elif service_type == LUSTRE_SERVICE_TYPE_MDT:
            ret, index_string = lustre_mdt_index2string(index)
            if ret:
                reason = ("invalid MDT index [%s]" % (index))
                log.cl_error(reason)
                raise Exception(reason)
            self.ls_index_string = index_string
        elif service_type == LUSTRE_SERVICE_TYPE_OST:
            ret, index_string = lustre_ost_index2string(index)
            if ret:
                reason = ("invalid OST index [%s]" % (index))
                log.cl_error(reason)
                raise Exception(reason)
            self.ls_index_string = index_string
        else:
            reason = ("unsupported service type [%s]" % service_type)
            log.cl_error(reason)
            raise Exception(reason)
        self.ls_index = index
        # BACKFSTYPE_ZFS or BACKFSTYPE_LDISKFS
        self.ls_backfstype = backfstype
        if lustre_fs is None:
            # MGS
            self.ls_service_name = None
        else:
            self.ls_service_name = lustre_fs.lf_fsname + "-" + self.ls_index_string

    def ls_service_string(self):
        """
        Return the service string used by mkfs.lustre or tunefs.lustre
        """
        service_string = ""
        for instance in self.ls_instances.values():
            if instance.lsi_nid is None:
                return -1, None
            if service_string != "":
                service_string += " "
            service_string += "--servicenode=%s" % instance.lsi_nid
        return 0, service_string

    def ls_nids(self):
        """
        Return the nid array of the service
        """
        nids = []
        for instance in self.ls_instances.values():
            assert instance.lsi_nid is not None
            nids.append(instance.lsi_nid)
        return nids

    def ls_hosts(self):
        """
        Return the host array of the service
        """
        hosts = []
        for instance in self.ls_instances.values():
            host = instance.lsi_host
            if host not in hosts:
                hosts.append(instance.lsi_host)
        return hosts

    def ls_instance_add(self, log, instance):
        """
        Add instance of this service
        """
        service_instance_name = instance.lsi_service_instance_name
        if service_instance_name in self.ls_instances:
            log.cl_error("instance [%s] is already added",
                         service_instance_name)
            return -1
        self.ls_instances[service_instance_name] = instance
        return 0

    def ls_mount_nolock(self, log):
        """
        Mount this service, lock should be held
        """
        log.cl_stdout("mounting service [%s]", self.ls_service_name)
        if len(self.ls_instances) == 0:
            return -1

        instance = self._ls_mounted_instance(log)
        if instance is not None:
            log.cl_stdout("service [%s] is already mounted on host [%s], no "
                          "need to mount again", self.ls_service_name,
                          instance.lsi_host.sh_hostname)
            return 0

        again = False
        for instance in self.ls_instances.values():
            if again:
                log.cl_stdout("trying to mount another service instance of "
                              "[%s]", self.ls_service_name)
            ret = instance.lsi_mount(log)
            if ret == 0:
                log.cl_stdout("mounted service [%s]", self.ls_service_name)
                return 0
            else:
                again = True

        log.cl_stderr("failed to mount service [%s]",
                      self.ls_service_name)
        return -1

    def ls_mount(self, log):
        """
        Mount this service
        """
        handle = self.ls_lock.rwl_writer_acquire(log)
        if handle is None:
            log.cl_stderr("aborting mounting service [%s]",
                          self.ls_service_name)
            return -1
        ret = self.ls_mount_nolock(log)
        handle.rwh_release()

        return ret

    def _ls_mounted_instance(self, log):
        """
        Return the instance that has been mounted
        If no instance is mounted, return None
        Read lock of the service should be held when calling this function
        """
        if len(self.ls_instances) == 0:
            return None

        mounted_instances = []
        for instance in self.ls_instances.values():
            ret = instance.lsi_check_mounted(log)
            if ret < 0:
                log.cl_error("failed to check whether service "
                             "[%s] is mounted on host [%s]",
                             self.ls_service_name,
                             instance.lsi_host.sh_hostname)
            elif ret > 0:
                log.cl_debug("service [%s] is mounted on host "
                             "[%s]", self.ls_service_name,
                             instance.lsi_host.sh_hostname)
                mounted_instances.append(instance)

        if len(mounted_instances) == 0:
            return None
        else:
            assert len(mounted_instances) == 1
            return mounted_instances[0]

    def ls_mounted_instance(self, log):
        """
        Return the instance that has been mounted
        If no instance is mounted, return None
        """
        handle = self.ls_lock.rwl_reader_acquire(log)
        if handle is None:
            log.cl_stderr("aborting checking mounted instance of service [%s]",
                          self.ls_service_name)
            return -1
        instance = self._ls_mounted_instance(log)
        handle.rwh_release()
        return instance

    def ls_umount_nolock(self, log):
        """
        Umount this service, lock should be held
        """
        service_name = self.ls_service_name

        log.cl_stdout("umounting service [%s]", service_name)
        instance = self._ls_mounted_instance(log)
        if instance is None:
            log.cl_stdout("service [%s] is not mounted on any host, no need "
                          "to umount again", self.ls_service_name)
            return 0

        ret = instance.lsi_umount(log)
        if ret == 0:
            log.cl_stdout("umounted service [%s]", service_name)
        else:
            log.cl_stdout("failed to umount service [%s]", service_name)
        return ret

    def ls_umount(self, log):
        """
        Umount this service
        """
        handle = self.ls_lock.rwl_writer_acquire(log)
        if handle is None:
            log.cl_stderr("aborting umounting service [%s]",
                          self.ls_service_name)
            return -1
        ret = self.ls_umount_nolock(log)
        handle.rwh_release()

        return ret

    def ls_format_nolock(self, log):
        """
        Format this service.
        Service should have been umounted, Lock should be held.
        """
        if len(self.ls_instances) == 0:
            return -1

        log.cl_stdout("formatting service [%s]", self.ls_service_name)
        for instance in self.ls_instances.values():
            ret = instance.lsi_format(log)
            if ret == 0:
                log.cl_stdout("formatted service [%s]", self.ls_service_name)
                return 0
        log.cl_stderr("failed to format service [%s]",
                      self.ls_service_name)
        return -1

    def ls_format(self, log):
        """
        Format this service
        """
        handle = self.ls_lock.rwl_writer_acquire(log)
        if handle is None:
            log.cl_stderr("aborting formating service [%s]",
                          self.ls_service_name)
            return -1
        ret = self.ls_format_nolock(log)
        handle.rwh_release()

        return ret

    def ls_encode(self, need_status, status_funct, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        # pylint: disable=too-many-branches
        service_name = self.ls_service_name
        if need_status:
            status = status_funct(service_name)

        if not need_structure:
            if need_status:
                if status is None:
                    return {cstr.CSTR_SERVICE_NAME: service_name,
                            cstr.CSTR_STATUS: None}
                else:
                    return status.lss_encode(False)
            else:
                instance_names = []
                for instance in self.ls_instances.values():
                    instance_names.append(instance.lsi_service_instance_name)
                return instance_names

        service_code = {}
        instance_codes = []
        for service_instance in self.ls_instances.values():
            instance_code = service_instance.lsi_encode(need_status, status_funct,
                                                        need_structure)
            if need_status:
                if status is None:
                    instance_code[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_UNKNOWN
                else:
                    if service_instance == status.lss_mounted_instance:
                        mounted = cstr.CSTR_TRUE
                    else:
                        mounted = cstr.CSTR_FALSE
                    instance_code[cstr.CSTR_IS_MOUNTED] = mounted
            instance_codes.append(instance_code)

        service_code[cstr.CSTR_INSTANCES] = instance_codes

        if need_status:
            if status is None:
                service_code[cstr.CSTR_STATUS] = cstr.CSTR_UNKNOWN
            else:
                if status.lss_mounted_instance is None:
                    service_code[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_FALSE
                else:
                    service_code[cstr.CSTR_IS_MOUNTED] = cstr.CSTR_TRUE
                service_code[cstr.CSTR_UPDATE_TIME] = status.lss_update_time

        if self.ls_service_type != LUSTRE_SERVICE_TYPE_MGS:
            service_code[cstr.CSTR_INDEX] = self.ls_index
        return service_code


class LustreMGS(LustreService):
    """
    Lustre MGS service not combined to MDT
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, log, mgs_id, backfstype):
        super(LustreMGS, self).__init__(log, None, LUSTRE_SERVICE_TYPE_MGS, 0,
                                        backfstype)
        # Key is file system name, value is LustreFilesystem
        self.lmgs_filesystems = {}
        self.ls_service_name = mgs_id

    def lmgs_add_fs(self, log, lustre_fs):
        """
        Add file system to this MGS
        """
        fsname = lustre_fs.lf_fsname
        if fsname in self.lmgs_filesystems:
            log.cl_error("file system [%s] is already in MGS [%s]",
                         fsname, self.ls_service_name)
            return -1

        ret = lustre_fs.lf_mgs_init(log, self)
        if ret:
            log.cl_error("failed to init MGS for file system [%s]",
                         lustre_fs.lf_fsname)
            return ret
        return 0


class LustreMGSInstance(LustreServiceInstance):
    """
    A Lustre MGS might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments
    def __init__(self, log, mgs, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreMGSInstance, self).__init__(log, mgs, host, device,
                                                mnt, nid, zpool_create=zpool_create)
        if add_to_host:
            ret = host.lsh_mgsi_add(log, self)
            if ret:
                reason = ("failed to add MGS instance of file system [%s] to "
                          "host [%s]" %
                          (mgs.ls_lustre_fs.lf_fsname, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)


class LustreFilesystem(object):
    """
    Information about Lustre file system
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, fsname):
        self.lf_fsname = fsname
        # Key is the service name, value is LustreOST
        self.lf_osts = {}
        # Key is the service name, value is LustreMDT
        self.lf_mdts = {}
        self.lf_clients = {}
        self.lf_mgs = None
        self.lf_mgs_mdt = None
        self.lf_lock = rwlock.RWLock()
        self.lf_qos = None

    def lf_qos_add(self, qos):
        """
        Add QoS control into this file system
        """
        if self.lf_qos is not None:
            return -1
        self.lf_qos = qos
        return 0

    def lf_mgs_init(self, log, mgs, combined=False):
        """
        Init a seperate or combined MGS
        """
        if self.lf_mgs is not None:
            log.cl_error("file system [%s] alreay has a MGS", self.lf_fsname)
            return -1

        if self.lf_mgs_mdt is not None:
            log.cl_error("file system [%s] alreay has a combined MGS",
                         self.lf_fsname)
            return -1

        if combined:
            self.lf_mgs_mdt = mgs
        else:
            self.lf_mgs = mgs
        return 0

    def lf_ost_add(self, service_name, ost):
        """
        Add OST into this file system
        """
        if service_name in self.lf_osts:
            return -1
        self.lf_osts[service_name] = ost
        return 0

    def lf_mdt_add(self, log, service_name, mdt):
        """
        Add MDT into this file system
        """
        if service_name in self.lf_mdts:
            return -1
        if mdt.lmdt_is_mgs:
            ret = self.lf_mgs_init(log, mdt, combined=True)
            if ret:
                log.cl_error("failed to init MGS for file system [%s]",
                             self.lf_fsname)
                return -1
        self.lf_mdts[service_name] = mdt
        return 0

    def lf_mgs_nids(self):
        """
        Return the nid array of the MGS
        """
        if self.lf_mgs_mdt is None:
            assert self.lf_mgs is not None
            return self.lf_mgs.ls_nids()
        else:
            assert self.lf_mgs is None
            return self.lf_mgs_mdt.ls_nids()

    def lf_set_jobid_var(self, log, jobid_var):
        """
        Set the job ID var
        """
        fsname = self.lf_fsname

        if self.lf_mgs is not None:
            mgs_lock_handle = self.lf_mgs.ls_lock.rwl_reader_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting set jobid on file system [%s]",
                              fsname)
                return -1

            fs_lock_handle = self.lf_lock.rwl_reader_acquire(log)
            if fs_lock_handle is None:
                mgs_lock_handle.rwh_release()
                log.cl_stderr("aborting set jobid on file system [%s]",
                              fsname)
                return -1
            service = self.lf_mgs
        else:
            fs_lock_handle = self.lf_lock.rwl_reader_acquire(log)
            if fs_lock_handle is None:
                log.cl_stderr("aborting set jobid on file system [%s]",
                              fsname)
                return -1

            mgs_lock_handle = self.lf_mgs_mdt.ls_lock.rwl_reader_acquire(log)
            if mgs_lock_handle is None:
                fs_lock_handle.rwh_release()
                log.cl_stderr("aborting set jobid on file system [%s]",
                              fsname)
                return -1
            service = self.lf_mgs_mdt

        for instance in service.ls_instances.values():
            host = instance.lsi_host
            ret = host.lsh_set_jobid_var(log, fsname, jobid_var)
            if ret == 0:
                log.cl_stdout("set jobid var of file system [%s] to [%s]",
                              fsname, jobid_var)
                break
        if ret:
            log.cl_stderr("failed to set jobid var of file system [%s] to [%s]",
                          fsname, jobid_var)

        if self.lf_mgs is not None:
            fs_lock_handle.rwh_release()
            mgs_lock_handle.rwh_release()
        else:
            mgs_lock_handle.rwh_release()
            fs_lock_handle.rwh_release()
        return ret

    def lf_services(self, mgs=False):
        """
        Return the service list of this file system
        """
        services = []
        for mdt in self.lf_mdts.values():
            services.append(mdt)
        for ost in self.lf_osts.values():
            services.append(ost)
        if mgs:
            if self.lf_mgs is not None:
                services.append(self.lf_mgs)
        return services

    def lf_oss_list(self):
        """
        Return the host list that could run OSS service
        """
        hosts = []
        for ost in self.lf_osts.values():
            ost_hosts = ost.ls_hosts()
            for ost_host in ost_hosts:
                if ost_host not in hosts:
                    hosts.append(ost_host)
        return hosts

    def lf_mds_list(self):
        """
        Return the host list that could run MDS service
        """
        hosts = []
        for mds in self.lf_mdts.values():
            mds_hosts = mds.ls_hosts()
            for mds_host in mds_hosts:
                if mds_host not in hosts:
                    hosts.append(mds_host)
        return hosts

    def lf_oss_and_mds_list(self):
        """
        Return the host list that could run MDS/OSS service
        """
        hosts = []
        for mds in self.lf_mdts.values():
            mds_hosts = mds.ls_hosts()
            for mds_host in mds_hosts:
                if mds_host not in hosts:
                    hosts.append(mds_host)
        for ost in self.lf_osts.values():
            ost_hosts = ost.ls_hosts()
            for ost_host in ost_hosts:
                if ost_host not in hosts:
                    hosts.append(ost_host)
        return hosts

    def lf_format_nolock(self, log):
        """
        Format the whole file system, not including the MGS
        Filesystem and MGS should already been umounted
        Write lock of the MGS and file system should be held
        """
        log.cl_stdout("formatting file system [%s]", self.lf_fsname)
        if len(self.lf_mgs_nids()) == 0:
            log.cl_stderr("the MGS nid of Lustre file system [%s] is not "
                          "configured, not able to format", self.lf_fsname)
            return -1

        for service_name, mdt in self.lf_mdts.iteritems():
            ret = mdt.ls_format(log)
            if ret:
                log.cl_stderr("failed to format MDT [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1

        for service_name, ost in self.lf_osts.iteritems():
            ret = ost.ls_format(log)
            if ret:
                log.cl_stderr("failed to format OST [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1
        log.cl_stdout("formatted file system [%s]", self.lf_fsname)
        return 0

    def _lf_mount(self, log):
        """
        Mount the whole file system
        Write lock of the file system should be held
        """
        log.cl_stdout("mounting file system [%s]", self.lf_fsname)
        if self.lf_mgs is not None:
            ret = self.lf_mgs.ls_mount_nolock(log)
            if ret:
                log.cl_stderr("failed to mount MGS of Lustre file "
                              "system [%s]", self.lf_fsname)
                return -1

        for service_name, mdt in self.lf_mdts.iteritems():
            ret = mdt.ls_mount(log)
            if ret:
                log.cl_stderr("failed to mount MDT [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1

        for service_name, ost in self.lf_osts.iteritems():
            ret = ost.ls_mount(log)
            if ret:
                log.cl_stderr("failed to mount OST [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1

        for client_index, client in self.lf_clients.iteritems():
            ret = client.lc_mount(log)
            if ret:
                log.cl_stderr("failed to mount client [%s] of Lustre file "
                              "system [%s]", client_index, self.lf_fsname)
                return -1
        log.cl_stdout("mounted file system [%s]", self.lf_fsname)
        return 0

    def lf_mount(self, log):
        """
        Mount the whole file system, including the MGS if necessary.
        First hold write lock of the MGS, and then hold the write lock of
        the file system.
        Write lock of the MGS might not be necessary if MGS is already
        mounted. It can be replace with read lock. This could be improved.
        """
        if self.lf_mgs is not None:
            mgs_lock_handle = self.lf_mgs.ls_lock.rwl_writer_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting mounting file system [%s]",
                              self.lf_fsname)
                return -1
        fs_lock_handle = self.lf_lock.rwl_writer_acquire(log)
        if fs_lock_handle is None:
            if self.lf_mgs is not None:
                mgs_lock_handle.rwh_release()
            log.cl_stderr("aborting mounting file system [%s]",
                          self.lf_fsname)
            return -1
        ret = self._lf_mount(log)
        fs_lock_handle.rwh_release()
        if self.lf_mgs is not None:
            mgs_lock_handle.rwh_release()
        return ret

    def _lf_mount_or_umount_service(self, log, service, mount=True):
        """
        Mount/Umount a service of a file system.
        Lock of MGS/fs/service will be handled properly
        """
        # Do not allow umounting stand alone MGS
        if mount:
            operation = "mount"
        else:
            operation = "umount"
        if service.ls_service_type == LUSTRE_SERVICE_TYPE_MGS:
            log.cl_error("%sing MGS using this interface is not allowed",
                         operation)
            return -1

        fsname = self.lf_fsname
        service_name = service.ls_service_name

        # Make sure the service belongs to this lustrefs
        if service.ls_lustre_fs != self:
            log.cl_error("service [%s] doen't belong to file system [%s]",
                         service_name, fsname)
            return -1

        # Only acquire read lock of MGS is fine, since MGS won't be
        # umounted/mounted in this operation
        if self.lf_mgs is not None:
            mgs_lock_handle = self.lf_mgs.ls_lock.rwl_reader_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting %sing service [%s]",
                              operation, service_name)
                return -1
        fs_lock_handle = self.lf_lock.rwl_writer_acquire(log)
        if fs_lock_handle is None:
            if self.lf_mgs is not None:
                mgs_lock_handle.rwh_release()
            log.cl_stderr("aborting %sing service [%s]",
                          operation, service_name)
            return -1

        if mount:
            ret = service.ls_mount(log)
        else:
            ret = service.ls_umount(log)

        fs_lock_handle.rwh_release()
        if self.lf_mgs is not None:
            mgs_lock_handle.rwh_release()
        return ret

    def lf_mount_service(self, log, service):
        """
        Mount a service of a file system.
        Lock of MGS/fs/service will be handled properly
        """
        return self._lf_mount_or_umount_service(log, service, mount=True)

    def lf_umount_service(self, log, service):
        """
        Umount a service of a file system.
        Lock of MGS/fs/service will be handled properly
        """
        return self._lf_mount_or_umount_service(log, service, mount=False)

    def lf_umount_nolock(self, log):
        """
        Umount the whole file system
        Write lock of the file system should be held
        """
        # pylint: disable=too-many-branches
        for client_index, client in self.lf_clients.iteritems():
            ret = client.lc_umount(log)
            if ret:
                log.cl_stderr("failed to umount client [%s] of Lustre file "
                              "system [%s]", client_index, self.lf_fsname)
                return -1

        for service_name, ost in self.lf_osts.iteritems():
            ret = ost.ls_umount(log)
            if ret:
                log.cl_stderr("failed to umount OST [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1

        for service_name, mdt in self.lf_mdts.iteritems():
            ret = mdt.ls_umount(log)
            if ret:
                log.cl_stderr("failed to umount MDT [%s] of Lustre file "
                              "system [%s]", service_name, self.lf_fsname)
                return -1
        return 0

    def lf_umount(self, log):
        """
        Umount the whole file system, but not MGS.
        First hold read lock of the MGS, and then hold the write lock of
        the file system.
        """
        if self.lf_mgs is not None:
            mgs_lock_handle = self.lf_mgs.ls_lock.rwl_reader_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting umounting file system [%s]",
                              self.lf_fsname)
                return -1
        fs_lock_handle = self.lf_lock.rwl_writer_acquire(log)
        if fs_lock_handle is None:
            if self.lf_mgs is not None:
                mgs_lock_handle.rwh_release()
            log.cl_stderr("aborting mounting file system [%s]",
                          self.lf_fsname)
            return -1
        ret = self.lf_umount_nolock(log)
        fs_lock_handle.rwh_release()
        if self.lf_mgs is not None:
            mgs_lock_handle.rwh_release()
        return ret

    def lf_encode(self, need_status, status_funct, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if not need_structure:
            if not need_status:
                children = [cstr.CSTR_CLIENTS, cstr.CSTR_OSTS, cstr.CSTR_MDTS]
                if self.lf_mgs is not None:
                    children.append(cstr.CSTR_MGS)
                children.append(cstr.CSTR_QOS)
                return children

        encoded = {cstr.CSTR_FSNAME: self.lf_fsname}

        if self.lf_mgs is not None:
            encoded[cstr.CSTR_MGS] = self.lf_mgs.ls_encode(need_status,
                                                           status_funct,
                                                           need_structure)

        mdts = []
        for mdt in self.lf_mdts.values():
            mdts.append(mdt.ls_encode(need_status,
                                      status_funct,
                                      need_structure))
        encoded[cstr.CSTR_MDTS] = mdts

        osts = []
        for ost in self.lf_osts.values():
            osts.append(ost.ls_encode(need_status,
                                      status_funct,
                                      need_structure))
        encoded[cstr.CSTR_OSTS] = osts

        clients = []
        for client in self.lf_clients.values():
            clients.append(client.lc_encode(need_status,
                                            status_funct,
                                            need_structure))
        encoded[cstr.CSTR_CLIENTS] = clients
        if self.lf_qos is not None:
            qos = self.lf_qos
            encoded[cstr.CSTR_QOS] = qos.cdqos_encode(need_status,
                                                      need_structure)
        return encoded


class LustreMDTInstance(LustreServiceInstance):
    """
    A Lustre MDT might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments
    def __init__(self, log, mdt, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreMDTInstance, self).__init__(log, mdt, host, device,
                                                mnt, nid,
                                                zpool_create=zpool_create)
        self.mdti_nid = nid
        if add_to_host:
            ret = host.lsh_mdti_add(self)
            if ret:
                reason = ("MDT instance [%s] of file system [%s] already "
                          "exists on host [%s]" %
                          (mdt.ls_lustre_fs.lf_fsname,
                           mdt.ls_index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)

    def mdti_enable_hsm_control(self, log):
        """
        Enable HSM control
        """
        mdt = self.lsi_service

        # Improve: first disable hsm_control to cleanup actions/agents
        command = ("lctl get_param mdt.%s-%s.hsm_control" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        expected_output = ("mdt.%s-%s.hsm_control=enabled\n" %
                           (mdt.ls_lustre_fs.lf_fsname,
                            mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to get HSM control status for file system "
                         "[%s] on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname,
                         retval.cr_stderr)
            return -1
        elif retval.cr_stdout == expected_output:
            return 0

        command = ("lctl set_param mdt.%s-%s.hsm_control=enabled" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to enable HSM control for file system [%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname, retval.cr_stderr)
            return -1
        return 0

    def mdti_enable_raolu(self, log):
        """
        Enable remove_archive_on_last_unlink
        """
        mdt = self.lsi_service

        command = ("lctl get_param mdt.%s-%s.hsm."
                   "remove_archive_on_last_unlink" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        expected_output = ("mdt.%s-%s.hsm."
                           "remove_archive_on_last_unlink=1\n" %
                           (mdt.ls_lustre_fs.lf_fsname,
                            mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_info("no remove_archive_on_last_unlink support in file "
                        "system [%s] on host [%s], %s",
                        mdt.ls_lustre_fs.lf_fsname,
                        self.lsi_host.sh_hostname, retval.cr_stderr)
            return 1
        elif retval.cr_stdout == expected_output:
            return 0

        command = ("lctl set_param mdt.%s-%s.hsm."
                   "remove_archive_on_last_unlink=1" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to enable remove_archive_on_last_unlink[%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         self.lsi_host.sh_hostname, retval.cr_stderr)
            return -1
        return 0

    def mdti_changelog_register(self, log):
        """
        Register changelog user
        """
        mdt = self.lsi_service

        command = ("lctl --device %s-%s changelog_register -n" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to register changelog user of [%s-%s] "
                         "on host [%s] with command [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         mdt.ls_index_string,
                         self.lsi_host.sh_hostname, command,
                         retval.cr_stderr)
            return None
        return retval.cr_stdout.strip()

    def mdti_changelog_deregister(self, log, user_id):
        """
        Deregister changelog user
        """
        mdt = self.lsi_service

        command = ("lctl --device %s-%s changelog_deregister %s" %
                   (mdt.ls_lustre_fs.lf_fsname,
                    mdt.ls_index_string, user_id))
        retval = self.lsi_host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to deregister changelog user of [%s-%s] "
                         "on host [%s], %s",
                         mdt.ls_lustre_fs.lf_fsname,
                         mdt.ls_index_string,
                         self.lsi_host.sh_hostname,
                         retval.cr_stderr)
            return -1
        return 0

    def _mdti_prevent_congestion_by_tbf_nolock(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        Lock should be held
        """

        name = "congestion"
        host = self.lsi_host

        ret = host.lsh_stop_mdt_tbf_rule(log, name)
        if ret:
            log.cl_debug("failed to stop rule [%s]", name)

        expression = "uid={0} warning=1"
        ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                          rpc_limit)
        if ret:
            return -1

        # Make sure ls -l won't be affected
        name = "ldlm_enqueue"
        ret = host.lsh_stop_mdt_tbf_rule(log, name)
        if ret:
            log.cl_debug("failed to stop rule [%s]", name)

        expression = "opcode={ldlm_enqueue}"
        ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                          10000)
        if ret:
            return -1

        return 0

    def mdti_prevent_congestion_by_tbf(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        """
        instance_name = self.lsi_service_instance_name
        service = self.lsi_service
        service_name = service.ls_service_name
        host = self.lsi_host
        hostname = host.sh_hostname

        host_handle = host.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting preventing congestion of instance [%s] "
                          "of service [%s] on host [%s]", instance_name,
                          service_name, hostname)
            return -1
        instance_handle = self.lsi_lock.rwl_writer_acquire(log)
        if instance_handle is None:
            host_handle.rwh_release()
            log.cl_stderr("aborting preventing congestion instance [%s] of "
                          "service [%s] on host [%s]", instance_name,
                          service_name, hostname)
            return -1
        ret = self._mdti_prevent_congestion_by_tbf_nolock(log, rpc_limit)
        instance_handle.rwh_release()
        host_handle.rwh_release()
        return ret


class LustreMDT(LustreService):
    """
    Lustre MDT service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, log, lustre_fs, index, backfstype, is_mgs=False):
        # pylint: disable=too-many-arguments
        super(LustreMDT, self).__init__(log, lustre_fs, LUSTRE_SERVICE_TYPE_MDT,
                                        index, backfstype)
        self.lmdt_is_mgs = is_mgs

        ret = lustre_fs.lf_mdt_add(log, self.ls_service_name, self)
        if ret:
            reason = ("failed to add MDT [%d] into file system [%s]" %
                      (self.ls_service_name, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)

    def _lmt_prevent_congestion_by_tbf_nolock(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        Lock should be held
        """
        if len(self.ls_instances) == 0:
            return -1

        instance = self._ls_mounted_instance(log)
        if instance is None:
            log.cl_stdout("service [%s] is not mounted on any host, "
                          "not able to prevent congestion",
                          self.ls_service_name)
            return -1

        return instance.mdti_prevent_congestion_by_tbf(log, rpc_limit)

    def lmt_prevent_congestion_by_tbf(self, log, rpc_limit):
        """
        Prevent congestion by defining TBF rules to the server
        """
        handle = self.ls_lock.rwl_writer_acquire(log)
        if handle is None:
            log.cl_stderr("aborting mounting service [%s]",
                          self.ls_service_name)
            return -1
        ret = self._lmt_prevent_congestion_by_tbf_nolock(log, rpc_limit)
        handle.rwh_release()

        return ret


class LustreOSTInstance(LustreServiceInstance):
    """
    A Lustre OST might has multiple instances on multiple hosts,
    which are usually for HA
    """
    # pylint: disable=too-many-arguments
    def __init__(self, log, ost, host, device, mnt, nid,
                 add_to_host=False, zpool_create=None):
        super(LustreOSTInstance, self).__init__(log, ost, host, device,
                                                mnt, nid, zpool_create=zpool_create)
        if add_to_host:
            ret = host.lsh_osti_add(self)
            if ret:
                reason = ("OST instance [%s] of file system [%s] already "
                          "exists on host [%s]" %
                          (ost.ls_lustre_fs.lf_fsname,
                           ost.ls_index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)


class LustreOST(LustreService):
    """
    Lustre OST service
    """
    # pylint: disable=too-few-public-methods
    # index: 0, 1, etc.
    def __init__(self, log, lustre_fs, index, backfstype):
        # pylint: disable=too-many-arguments
        super(LustreOST, self).__init__(log, lustre_fs, LUSTRE_SERVICE_TYPE_OST,
                                        index, backfstype)
        ret = lustre_fs.lf_ost_add(self.ls_service_name, self)
        if ret:
            reason = ("OST [%s] already exists in file system [%s]" %
                      (self.ls_index_string, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)


class LustreClient(object):
    """
    Lustre client
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, log, lustre_fs, host, mnt, add_to_host=False):
        # pylint: disable=too-many-arguments
        self.lc_lustre_fs = lustre_fs
        self.lc_host = host
        self.lc_mnt = mnt
        index = ("%s:%s" % (host.sh_hostname, mnt))
        if index in lustre_fs.lf_clients:
            reason = ("client [%s] already exists in file system [%s]" %
                      (index, lustre_fs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)
        lustre_fs.lf_clients[index] = self
        self.lc_client_name = index
        if add_to_host:
            ret = host.lsh_client_add(lustre_fs.lf_fsname, mnt, self)
            if ret:
                reason = ("client [%s] already exists on host [%s]" %
                          (index, host.sh_hostname))
                log.cl_error(reason)
                raise Exception(reason)

    def _lc_check_mounted(self, log):
        """
        Return 1 when client is mounted
        Return 0 when client is not mounted
        Return negative when error
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-return-statements
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname
        mount_point = self.lc_mnt

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        ret = 0
        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            # Skip the Clients
            match = client_regular.match(line)
            if not match:
                continue

            mounted_fsname = match.group("fsname")
            mounted_mnt = match.group("mount_point")

            if mounted_fsname == fsname:
                if mount_point != mounted_mnt:
                    log.cl_debug("Lustre client [%s] is mounted on "
                                 "host [%s], but on mount point [%s], not "
                                 "on [%s]", fsname, hostname, mounted_mnt,
                                 mount_point)
                    continue
            else:
                if mount_point == mounted_mnt:
                    log.cl_error("one Lustre client is mounted on mount "
                                 "point [%s] of host [%s], but file system "
                                 "name is [%s], expected [%s]",
                                 mount_point, hostname, mounted_fsname,
                                 fsname)
                    return -1
                continue

            log.cl_debug("Lustre client of file system [%s] is already "
                         "mounted on dir [%s] of host [%s]",
                         fsname, mount_point, hostname)
            ret = 1
            break
        return ret

    def lc_mount(self, log):
        """
        Mount this client
        """
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname

        log.cl_stdout("mounting client file system [%s] to mount point [%s] of "
                      "host [%s]", fsname, self.lc_mnt,
                      hostname)

        ret = self._lc_check_mounted(log)
        if ret < 0:
            log.cl_stderr("failed to check whether Lustre client "
                          "[%s] is mounted on host [%s]",
                          fsname, hostname)
        elif ret > 0:
            log.cl_stdout("Lustre client [%s] is already mounted on host "
                          "[%s], no need to mount again", fsname, hostname)
            return 0

        nid_string = ""
        for mgs_nid in self.lc_lustre_fs.lf_mgs_nids():
            if nid_string != "":
                nid_string += ":"
            nid_string += mgs_nid
        command = ("mkdir -p %s && mount -t lustre %s:/%s %s" %
                   (self.lc_mnt, nid_string,
                    fsname, self.lc_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1
        log.cl_stdout("mounted Lustre client [%s] to mount point [%s] of "
                      "host [%s]", fsname, self.lc_mnt,
                      hostname)
        return 0

    def lc_umount(self, log):
        """
        Umount this client
        """
        host = self.lc_host
        hostname = host.sh_hostname
        fsname = self.lc_lustre_fs.lf_fsname

        if log.cl_abort:
            log.cl_stderr("aborting umounting client file system [%s] from "
                          "mount point [%s] on host [%s]", fsname,
                          self.lc_mnt, hostname)
            return -1

        log.cl_stdout("umounting client file system [%s] from mount point "
                      "[%s] on host [%s]",
                      fsname, self.lc_mnt, hostname)

        ret = self._lc_check_mounted(log)
        if ret < 0:
            log.cl_stderr("failed to check whether Lustre client "
                          "[%s] is mounted on host [%s]",
                          fsname, hostname)
        elif ret == 0:
            log.cl_stdout("Lustre client [%s] is not mounted on dir [%s] of "
                          "host [%s], no need to umount again", fsname,
                          self.lc_mnt, hostname)
            return 0

        command = ("umount %s" % (self.lc_mnt))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command, hostname, retval.cr_exit_status,
                          retval.cr_stdout, retval.cr_stderr)
            return -1
        log.cl_stdout("umounted client file system [%s] from mount point [%s]",
                      fsname, self.lc_mnt)
        return 0

    def lc_encode(self, need_status, status_funct, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        # pylint: disable=unused-argument
        if not need_structure:
            if need_status:
                return {cstr.CSTR_CLIENT_NAME: self.lc_client_name,
                        cstr.CSTR_STATUS: cstr.CSTR_UNKNOWN}
            else:
                return []
        else:
            encoded = {cstr.CSTR_CLIENT_NAME: self.lc_client_name,
                       cstr.CSTR_HOST_ID: self.lc_host.sh_host_id,
                       cstr.CSTR_MNT: self.lc_mnt}
            if need_status:
                encoded[cstr.CSTR_STATUS] = cstr.CSTR_UNKNOWN
        return encoded


class LustreVersion(object):
    """
    RPM version of Lustre
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, name, rpm_git_pattern, rpm_patterns,
                 kernel_major_version):
        # pylint: disable=too-few-public-methods,too-many-arguments
        self.lv_name = name
        self.lv_rpm_git_pattern = rpm_git_pattern
        self.lv_rpm_patterns = rpm_patterns
        self.lv_kernel_major_version = kernel_major_version

RPM_KERNEL = "kernel"
RPM_KERNEL_FIRMWARE = "kernel-firmware"
RPM_LUSTRE = "lustre"
RPM_IOKIT = "iokit"
RPM_KMOD = "kmod"
RPM_OSD_LDISKFS = "osd_ldiskfs"
RPM_OSD_LDISKFS_MOUNT = "osd_ldiskfs_mount"
RPM_OSD_ZFS = "osd_zfs"
RPM_OSD_ZFS_MOUNT = "osd_zfs_mount"
RPM_TESTS = "tests"
RPM_TESTS_KMOD = "tests_kmod"
RPM_MLNX_OFA = "mlnx_ofa"
RPM_MLNX_KMOD = "mlnx_ofa_modules"

# The order should be proper for the dependency of RPMs
LUSTRE_RPM_TYPES = [RPM_KMOD, RPM_OSD_LDISKFS_MOUNT, RPM_OSD_LDISKFS,
                    RPM_OSD_ZFS_MOUNT, RPM_OSD_ZFS,
                    RPM_LUSTRE, RPM_IOKIT, RPM_TESTS_KMOD, RPM_TESTS]

ES3_PATTERNS = {
    RPM_KERNEL: r"^(kernel-3.+\.x86_64\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2\.7.+\.x86_64\.rpm)$",
    RPM_IOKIT: r"^(lustre-iokit-2\.7.+\.x86_64\.rpm)$",
    RPM_KMOD: r"^(lustre-modules-2\.7.+\.x86_64\.rpm)$",
    RPM_OSD_LDISKFS: r"^(lustre-osd-ldiskfs-2\.7.+\.x86_64\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2\.7.+\.x86_64\.rpm)$",
    RPM_OSD_ZFS: r"^(lustre-osd-zfs-2\.7.+\.x86_64\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2\.7.+\.x86_64\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2\.7.+\.x86_64\.rpm)$",
    RPM_MLNX_OFA: r"^(mlnx-ofa_kernel-3.+\.x86_64\.rpm)$",
    RPM_MLNX_KMOD: r"^(mlnx-ofa_kernel-modules-3.+\.x86_64\.rpm)$"}

LUSTRE_VERSION_ES3 = LustreVersion("es3",
                                   r".+\.x86_64_g(.+)\.x86_64\.rpm$",
                                   ES3_PATTERNS,  # rpm_patterns
                                   "3")  # kernel_major_version

MASTER_PATTERNS = {
    RPM_KERNEL: r"^(kernel-3.+\.x86_64\.rpm)$",
    RPM_LUSTRE: r"^(lustre-2.+\.x86_64\.rpm)$",
    RPM_IOKIT: r"^(lustre-iokit-2.+\.x86_64\.rpm)$",
    RPM_KMOD: r"^(kmod-lustre-2.+\.x86_64\.rpm)$",
    RPM_OSD_LDISKFS: r"^(kmod-lustre-osd-ldiskfs-2.+\.x86_64\.rpm)$",
    RPM_OSD_LDISKFS_MOUNT: r"^(lustre-osd-ldiskfs-mount-2.+\.x86_64\.rpm)$",
    RPM_OSD_ZFS: r"^(kmod-lustre-osd-zfs-2.+\.x86_64\.rpm)$",
    RPM_OSD_ZFS_MOUNT: r"^(lustre-osd-zfs-mount-2.+\.x86_64\.rpm)$",
    RPM_TESTS: r"^(lustre-tests-2.+\.x86_64\.rpm)$",
    RPM_TESTS_KMOD: r"^(kmod-lustre-tests-2.+\.x86_64\.rpm)$",
}

LUSTRE_VERSION_MASTER = LustreVersion("master",
                                      r".+\_g(.+)-.+\.rpm$",
                                      MASTER_PATTERNS,  # rpm_patterns
                                      "3")  # kernel_major_version

LUSTRE_VERSIONS = [LUSTRE_VERSION_ES3, LUSTRE_VERSION_MASTER]


def match_rpm_patterns(log, data, rpm_dict, possible_versions):
    """
    Match a rpm pattern
    """
    matched_versions = []
    rpm_type = None
    rpm_name = None
    for version in possible_versions:
        patterns = version.lv_rpm_patterns
        matched = False
        for key in patterns.keys():
            match = re.search(patterns[key], data)
            if match:
                value = match.group(1)
                if rpm_type is not None and rpm_type != key:
                    log.cl_error("RPM [%s] can be matched to both type [%s] "
                                 "and [%s]", value, rpm_type, key)
                    return -1

                if rpm_name is not None and rpm_name != value:
                    log.cl_error("RPM [%s] can be matched as both name [%s] "
                                 "and [%s]", value, rpm_name, value)
                    return -1

                rpm_type = key
                rpm_name = value
                matched = True
                log.cl_debug("match of key [%s]: [%s] by data [%s]",
                             key, value, data)
        if matched:
            matched_versions.append(version)

    if len(matched_versions) != 0:
        if rpm_type in rpm_dict:
            log.cl_error("multiple match of RPM type [%s], both from [%s] "
                         "and [%s]", rpm_type, rpm_name, rpm_dict[rpm_type])
            return -1
        for version in possible_versions[:]:
            if version not in matched_versions:
                possible_versions.remove(version)
        rpm_dict[rpm_type] = rpm_name

    return 0


class LustreRPMs(object):
    """
    Lustre OST service
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, distribution_id, lustre_rpm_dir, e2fsprogs_rpm_dir):
        self.lr_distribution_id = distribution_id
        self.lr_rpm_dir = lustre_rpm_dir
        self.lr_rpm_names = {}
        self.lr_lustre_version = None
        self.lr_kernel_version = None
        self.lr_zfs_support = True
        self.lr_ldiskfs_support = True
        self.lr_e2fsprogs_rpm_dir = e2fsprogs_rpm_dir

    def lr_prepare(self, log):
        """
        Prepare the RPMs
        """
        rpm_files = os.listdir(self.lr_rpm_dir)

        possible_versions = LUSTRE_VERSIONS[:]
        for rpm_file in rpm_files:
            log.cl_debug("found file [%s] in directory [%s]",
                         rpm_file, self.lr_rpm_dir)
            ret = match_rpm_patterns(log, rpm_file, self.lr_rpm_names,
                                     possible_versions)
            if ret:
                log.cl_error("failed to match pattern for file [%s]",
                             rpm_file)
                return -1

        if len(possible_versions) != 1:
            log.cl_error("the possible RPM version is %d, should be 1",
                         len(possible_versions))
            return -1
        self.lr_lustre_version = possible_versions[0]

        for key in self.lr_lustre_version.lv_rpm_patterns.keys():
            if key not in self.lr_rpm_names:
                if key == RPM_OSD_LDISKFS or key == RPM_OSD_LDISKFS_MOUNT:
                    log.cl_info("failed to get RPM name of [%s], "
                                "disabling ldiskfs suport", key)
                    self.lr_ldiskfs_support = False
                elif key == RPM_OSD_ZFS or key == RPM_OSD_ZFS_MOUNT:
                    log.cl_info("failed to get RPM name of [%s], "
                                "disabling ZFS suport", key)
                    self.lr_zfs_support = False
                else:
                    log.cl_error("failed to get RPM name of [%s]", key)
                    return -1

        kernel_rpm_name = self.lr_rpm_names[RPM_KERNEL]
        kernel_rpm_path = (self.lr_rpm_dir + '/' + kernel_rpm_name)
        command = ("rpm -qpl %s | grep /lib/modules |"
                   "sed 1q | awk -F '/' '{print $4}'" %
                   kernel_rpm_path)
        retval = utils.run(command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         retval.cr_exit_status, retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        self.lr_kernel_version = retval.cr_stdout.strip()
        return 0


def failure_caused_by_ksym(log, retval):
    """
    Check whether the kmod RPM installation failed because of dependency
    on ksym
    """
    lines = retval.cr_stderr.split('\n')
    if len(lines) < 1:
        log.cl_debug("line number doesn't match: [%d]", len(lines))
        return False
    if lines[0] != "error: Failed dependencies:":
        log.cl_debug("first line doesn't match: [%s]", lines[0])
        return False
    ksym_pattern = r"^.+ksym.+ is needed by .+$"
    for line in lines[1:]:
        if line == "":
            continue
        matched = re.match(ksym_pattern, line, re.M)
        if not matched:
            log.cl_debug("line doesn't match: [%s]", line)
            return False
    return True


def lustre_client_id(fsname, mnt):
    """
    Return the Lustre client ID
    """
    return "%s:%s" % (fsname, mnt)


def lustre_ost_id(fsname, ost_index):
    """
    Return the Lustre client ID
    """
    return "%s:%s" % (fsname, ost_index)


def lustre_mdt_id(fsname, mdt_index):
    """
    Return the Lustre client ID
    """
    return "%s:%s" % (fsname, mdt_index)


class LustreServerHost(ssh_host.SSHHost):
    # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    Each host being used to run Lustre tests has an object of this
    """
    def __init__(self, hostname, lustre_rpms=None, identity_file=None,
                 local=False, host_id=None):
        # pylint: disable=too-many-arguments
        super(LustreServerHost, self).__init__(hostname,
                                               identity_file=identity_file,
                                               local=local,
                                               host_id=host_id)
        # key: $fsname:$mnt, value: LustreClient object
        self.lsh_clients = {}
        # Key: ls_service_name, value: LustreOSTInstance object
        self.lsh_ost_instances = {}
        # Key: ls_service_name, value: LustreMDTInstance object
        self.lsh_mdt_instances = {}
        self.lsh_cached_has_fuser = None
        self.lsh_fuser_install_failed = False
        self.lsh_mgsi = None
        self.lsh_lock = rwlock.RWLock()
        self.lsh_lustre_rpms = lustre_rpms
        self.lsh_lustre_version_major = None
        self.lsh_lustre_version_minor = None
        self.lsh_lustre_version_patch = None
        self.lsh_version_value = None

    def lsh_detect_lustre_version(self, log):
        """
        Detect the Lustre version
        """
        command = ("lctl lustre_build_version")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        lustre_version_string = retval.cr_stdout.strip()
        version_pattern = (r"^.+(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+).+$")
        version_regular = re.compile(version_pattern)
        match = version_regular.match(lustre_version_string)
        if match:
            self.lsh_lustre_version_major = int(match.group("major"))
            self.lsh_lustre_version_minor = int(match.group("minor"))
            self.lsh_lustre_version_patch = int(match.group("patch"))
        else:
            log.cl_error("unexpected version string format: [%s]",
                         lustre_version_string)
            return -1

        self.lsh_version_value = version_value(self.lsh_lustre_version_major,
                                               self.lsh_lustre_version_minor,
                                               self.lsh_lustre_version_patch)
        log.cl_debug("version_string: %s %d", lustre_version_string,
                     self.lsh_version_value)
        return 0

    def _lsh_enable_tbf(self, log, param_path, tbf_type):
        """
        Change the NRS policy to TBF
        param_path example: ost.OSS.ost_io
        """
        if tbf_type == TBF_TYPE_GENERAL:
            command = ('lctl set_param %s.nrs_policies="tbf"' %
                       (param_path))
        else:
            command = ('lctl set_param %s.nrs_policies="tbf %s"' %
                       (param_path, tbf_type))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_enable_ost_io_tbf(self, log, tbf_type):
        """
        Change the OST IO NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_OST_IO, tbf_type)

    def lsh_enable_mdt_tbf(self, log, tbf_type):
        """
        Change the mdt NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT, tbf_type)

    def lsh_enable_mdt_readpage_tbf(self, log, tbf_type):
        """
        Change the mdt_readpage NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT_READPAGE, tbf_type)

    def lsh_enable_mdt_setattr_tbf(self, log, tbf_type):
        """
        Change the mdt_setattr NRS policy to TBF
        """
        return self._lsh_enable_tbf(log, PARAM_PATH_MDT_SETATTR, tbf_type)

    def lsh_enable_mdt_all_tbf(self, log, tbf_type):
        """
        Change the all MDT related NRS policies to TBF
        """
        ret = self.lsh_enable_mdt_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT, self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_readpage_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_READPAGE,
                         self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_setattr_tbf(log, tbf_type)
        if ret:
            log.cl_error("failed to enable TBF policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_SETATTR,
                         self.sh_hostname)
            return ret
        return 0

    def _lsh_enable_fifo(self, log, param_path):
        """
        Change the policy to FIFO
        param_path example: ost.OSS.ost_io
        """
        command = ('lctl set_param %s.nrs_policies="fifo"' % param_path)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_enable_ost_io_fifo(self, log):
        """
        Change the OST IO NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_OST_IO)

    def lsh_enable_mdt_fifo(self, log):
        """
        Change the mdt NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT)

    def lsh_enable_mdt_readpage_fifo(self, log):
        """
        Change the mdt_readpage NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT_READPAGE)

    def lsh_enable_mdt_setattr_fifo(self, log):
        """
        Change the mdt_setattr NRS policy to FIFO
        """
        return self._lsh_enable_fifo(log, PARAM_PATH_MDT_SETATTR)

    def lsh_enable_mdt_all_fifo(self, log):
        """
        Change the all MDT related NRS policies to FIFO
        """
        ret = self.lsh_enable_mdt_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT, self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_readpage_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_READPAGE,
                         self.sh_hostname)
            return ret

        ret = self.lsh_enable_mdt_setattr_fifo(log)
        if ret:
            log.cl_error("failed to enable FIFO policy on path [%s] of host "
                         "[%s]", PARAM_PATH_MDT_SETATTR,
                         self.sh_hostname)
            return ret
        return 0

    def _lsh_set_jobid_var(self, log, fsname, jobid_var):
        """
        Set the job ID variable
        """
        command = ("lctl conf_param %s.sys.jobid_var=%s" %
                   (fsname, jobid_var))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_set_jobid_var(self, log, fsname, jobid_var):
        """
        Prepare the host for running Lustre
        Lock should be hold
        """
        host_handle = self.lsh_lock.rwl_reader_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting set jobid var of file system [%s] on "
                          "host [%s]", fsname, self.sh_hostname)
            return -1
        ret = self._lsh_set_jobid_var(log, fsname, jobid_var)
        host_handle.rwh_release()
        return ret

    def _lsh_get_tbf_rule_list(self, log, param_path):
        """
        Get the rule list
        param_path example: ost.OSS.ost_io
        """
        rule_list = []
        command = "lctl get_param -n %s.nrs_tbf_rule" % param_path
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, rule_list

        cpt_pattern = (r"^CPT \d+:$")
        cpt_regular = re.compile(cpt_pattern)

        rule_pattern = (r"^(?P<name>\S+) .+$")
        rule_regular = re.compile(rule_pattern)

        lines = retval.cr_stdout.splitlines()
        for line in lines:
            if line == "regular_requests:":
                continue
            if line == "high_priority_requests:":
                continue
            match = cpt_regular.match(line)
            if match:
                continue
            match = rule_regular.match(line)
            if not match:
                log.cl_error("failed to parse line [%s]", line)
                return -1, rule_list
            name = match.group("name")
            if name == "default":
                continue
            if name not in rule_list:
                rule_list.append(name)

        return 0, rule_list

    def lsh_get_ost_io_tbf_rule_list(self, log):
        """
        Get the rule list on ost_io
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_OST_IO)

    def lsh_get_mdt_tbf_rule_list(self, log):
        """
        Get the rule list on mdt
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT)

    def lsh_get_mdt_readpage_tbf_rule_list(self, log):
        """
        Get the rule list on mdt_readpage
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT_READPAGE)

    def lsh_get_mdt_setattr_tbf_rule_list(self, log):
        """
        Get the rule list on mdt_setattr
        """
        return self._lsh_get_tbf_rule_list(log, PARAM_PATH_MDT_SETATTR)

    def _lsh_start_tbf_rule(self, log, param_path, name, expression, rate):
        # pylint: disable=too-many-arguments
        """
        Start a TBF rule
        param_path example: ost.OSS.ost_io
        name: rule name
        """
        if self.lsh_version_value is None:
            ret = self.lsh_detect_lustre_version(log)
            if ret:
                log.cl_error("failed to detect Lustre version on host [%s]",
                             self.sh_hostname)
                return -1

        if self.lsh_version_value >= version_value(2, 8, 54):
            command = ('lctl set_param %s.nrs_tbf_rule='
                       '"start %s %s rate=%d"' %
                       (param_path, name, expression, rate))
        else:
            log.cl_error("TBF is not supported properly in this Lustre "
                         "version")
            return -1
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_start_ost_io_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on ost.OSS.ost_io
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_OST_IO, name,
                                        expression, rate)

    def lsh_start_mdt_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT, name,
                                        expression, rate)

    def lsh_start_mdt_readpage_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT readpage service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT_READPAGE, name,
                                        expression, rate)

    def lsh_start_mdt_setattr_tbf_rule(self, log, name, expression, rate):
        """
        Start a TBF rule on MDT readpage service
        """
        return self._lsh_start_tbf_rule(log, PARAM_PATH_MDT_SETATTR, name,
                                        expression, rate)

    def _lsh_stop_tbf_rule(self, log, param_path, name):
        """
        Start a TBF rule
        """
        command = ('lctl set_param %s.nrs_tbf_rule='
                   '"stop %s"' % (param_path, name))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_stop_ost_io_tbf_rule(self, log, name):
        """
        Stop a TBF rule on ost.OSS.ost_io
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_OST_IO, name)

    def lsh_stop_mdt_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT, name)

    def lsh_stop_mdt_readpage_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS readpage service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT_READPAGE, name)

    def lsh_stop_mdt_setattr_tbf_rule(self, log, name):
        """
        Stop a TBF rule on MDS readpage service
        """
        return self._lsh_stop_tbf_rule(log, PARAM_PATH_MDT_SETATTR, name)

    def lsh_change_tbf_rate(self, log, name, rate):
        """
        Change the TBF rate of a rule
        """
        if self.lsh_version_value is None:
            ret = self.lsh_detect_lustre_version(log)
            if ret:
                log.cl_error("failed to detect Lustre version on host [%s]",
                             self.sh_hostname)
                return -1

        if self.lsh_version_value >= version_value(2, 8, 54):
            command = ('lctl set_param ost.OSS.ost_io.nrs_tbf_rule='
                       '"change %s rate=%d"' % (name, rate))
        else:
            command = ('lctl set_param ost.OSS.ost_io.nrs_tbf_rule='
                       '"change %s %d"' % (name, rate))
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def lsh_fuser_kill(self, log, fpath):
        """
        Run "fuser -km" to a fpath
        """
        if not self.sh_has_command(log, "fuser") and not self.lsh_fuser_install_failed:
            log.cl_debug("host [%s] doesnot have fuser, trying to install",
                         self.sh_hostname)
            ret = self.sh_run(log, "yum install psmisc -y")
            if ret.cr_exit_status:
                log.cl_error("failed to install fuser")
                self.lsh_fuser_install_failed = True
                return -1
            self.sh_cached_has_commands["fuser"] = True

        command = ("fuser -km %s" % (fpath))
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

    def lsh_osti_add(self, osti):
        """
        Add OST into this host
        """
        service_name = osti.lsi_service.ls_service_name
        if service_name in self.lsh_ost_instances:
            return -1
        self.lsh_ost_instances[service_name] = osti
        return 0

    def lsh_mgsi_add(self, log, mgsi):
        """
        Add MDT into this host
        """
        if self.lsh_mgsi is not None:
            log.cl_error("MGS already exits on host [%s]", self.sh_hostname)
            return -1
        self.lsh_mgsi = mgsi
        return 0

    def lsh_mdti_add(self, mdti):
        """
        Add MDT into this host
        """
        service_name = mdti.lsi_service.ls_service_name
        if service_name in self.lsh_mdt_instances:
            return -1
        self.lsh_mdt_instances[service_name] = mdti
        return 0

    def lsh_client_add(self, fsname, mnt, client):
        """
        Add MDT into this host
        """
        client_id = lustre_client_id(fsname, mnt)
        if client_id in self.lsh_clients:
            return -1
        self.lsh_clients[client_id] = client
        return 0

    def lsh_lustre_device_label(self, log, device):
        """
        Run e2label on a lustre device
        """
        # try to handle as ldiskfs first
        command = ("e2label %s" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status == 0:
            return 0, retval.cr_stdout.strip()

        # fall back to handle as zfs then
        command = ("zfs get -H lustre:svname %s | awk {'print $3'}" % device)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1, None
        return 0, retval.cr_stdout.strip()

    def lsh_lustre_detect_services(self, log, clients, osts, mdts, add_found=False):
        """
        Detect mounted Lustre services (MDT/OST/clients) from the host
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # pylint: disable=too-many-arguments
        server_pattern = (r"^(?P<device>\S+) (?P<mount_point>\S+) lustre .+$")
        server_regular = re.compile(server_pattern)

        client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
        client_regular = re.compile(client_pattern)

        ost_pattern = (r"^(?P<fsname>\S+)-OST(?P<index_string>[0-9a-f]{4})$")
        ost_regular = re.compile(ost_pattern)

        mdt_pattern = (r"^(?P<fsname>\S+)-MDT(?P<index_string>[0-9a-f]{4})$")
        mdt_regular = re.compile(mdt_pattern)

        mgt_pattern = (r"MGS")
        mgt_regular = re.compile(mgt_pattern)

        # Detect Lustre services
        command = ("cat /proc/mounts")
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for line in retval.cr_stdout.splitlines():
            log.cl_debug("checking line [%s]", line)
            match = server_regular.match(line)
            if not match:
                continue

            device = match.group("device")
            mount_point = match.group("mount_point")

            match = client_regular.match(line)
            if match:
                fsname = match.group("fsname")
                client_id = lustre_client_id(fsname, mount_point)
                if client_id in self.lsh_clients:
                    client = self.lsh_clients[client_id]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    client = LustreClient(log, lustre_fs, self, mount_point,
                                          add_to_host=add_found)
                clients[client_id] = client
                log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

            ret, label = self.lsh_lustre_device_label(log, device)
            if ret:
                log.cl_error("failed to get the label of device [%s] on "
                             "host [%s]", device, self.sh_hostname)
                return -1

            match = ost_regular.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                ret, ost_index = lustre_string2index(index_string)
                if ret:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                ost_id = lustre_ost_id(fsname, ost_index)
                if ost_id in self.lsh_ost_instances:
                    osti = self.lsh_ost_instances[ost_id]
                else:
                    lustre_fs = LustreFilesystem(fsname)

                    ost = LustreOST(log, lustre_fs, ost_index, None)
                    osti = LustreOSTInstance(log, ost, self, device, mount_point,
                                             None, add_to_host=add_found)
                osts[ost_id] = osti
                log.cl_debug("OST [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

            match = mdt_regular.match(label)
            if match:
                fsname = match.group("fsname")
                index_string = match.group("index_string")
                ret, mdt_index = lustre_string2index(index_string)
                if ret:
                    log.cl_error("invalid label [%s] of device [%s] on "
                                 "host [%s]", label, device, self.sh_hostname)
                    return -1
                mdt_id = lustre_mdt_id(fsname, mdt_index)
                if mdt_id in self.lsh_mdt_instances:
                    mdti = self.lsh_mdt_instances[mdt_id]
                else:
                    lustre_fs = LustreFilesystem(fsname)
                    mdt = LustreMDT(log, lustre_fs, mdt_index, None)
                    mdti = LustreMDTInstance(log, mdt, self, device, mount_point,
                                             None, add_to_host=add_found)
                mdts[mdt_id] = mdti
                log.cl_debug("MDT [%s] mounted on dir [%s] of host [%s]",
                             fsname, mount_point, self.sh_hostname)
                continue

            match = mgt_regular.match(label)
            if match:
                log.cl_debug("MGT mounted on dir [%s] of host [%s]",
                             mount_point, self.sh_hostname)
                continue

            log.cl_error("unable to detect service mounted on dir [%s] of "
                         "host [%s]", mount_point, self.sh_hostname)
            return -1

        return 0

    def lsh_lustre_umount_services(self, log, client_only=False):
        """
        Umount Lustre OSTs/MDTs/clients on the host
        """
        # pylint: disable=too-many-return-statements
        clients = {}
        osts = {}
        mdts = {}
        ret = self.lsh_lustre_detect_services(log, clients, osts, mdts)
        if ret:
            log.cl_stderr("failed to detect Lustre services on host [%s]",
                          self.sh_hostname)
            return -1

        for client in clients.values():
            command = ("umount -f %s" % client.lc_mnt)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_debug("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             self.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
            else:
                continue

            # Kill the user of Lustre client so that umount won't be stopped
            ret = self.lsh_fuser_kill(log, client.lc_mnt)
            if ret:
                log.cl_stderr("failed to kill processes using [%s]",
                              client.lc_mnt)
                return -1

            command = ("umount -f %s" % client.lc_mnt)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        if client_only:
            return 0

        for mdt in mdts.values():
            command = ("umount %s" % mdt.lsi_mnt)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        for ost in osts.values():
            command = ("umount %s" % ost.lsi_mnt)
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1
        return 0

    def lsh_lustre_uninstall(self, log):
        # pylint: disable=too-many-return-statements,too-many-branches
        # pylint: disable=too-many-statements
        """
        Uninstall Lustre RPMs
        """
        log.cl_stdout("uninstalling Lustre RPMs on host [%s]", self.sh_hostname)

        ret = self.sh_run(log, "rpm --rebuilddb")
        if ret.cr_exit_status != 0:
            log.cl_stderr("failed to run 'rpm --rebuilddb' on host "
                          "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
            return -1

        log.cl_stdout("killing all processes that run yum commands "
                      "on host [%s]", self.sh_hostname)
        ret = self.sh_run(log, "ps aux | grep -v grep | grep yum | "
                          "awk '{print $2}'")
        if ret.cr_exit_status != 0:
            log.cl_stderr("failed to kill yum processes on host "
                          "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
            return -1

        for pid in ret.cr_stdout.splitlines():
            log.cl_stdout("killing pid [%s] on host [%s]",
                          pid, self.sh_hostname)
            ret = self.sh_run(log, "kill -9 %s" % pid)
            if ret.cr_exit_status != 0:
                log.cl_stderr("failed to kill pid [%s] on host "
                              "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                              pid, self.sh_hostname, ret.cr_exit_status,
                              ret.cr_stdout, ret.cr_stderr)
                return -1

        log.cl_stdout("running yum-complete-transaction in case of broken yum "
                      "on host [%s]", self.sh_hostname)
        ret = self.sh_run(log, "which yum-complete-transaction")
        if ret.cr_exit_status != 0:
            ret = self.sh_run(log, "yum install yum-utils -y")
            if ret.cr_exit_status != 0:
                log.cl_stderr("failed to install yum-utils on host "
                              "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                              self.sh_hostname, ret.cr_exit_status,
                              ret.cr_stdout, ret.cr_stderr)
                return -1

        ret = self.sh_run(log, "yum-complete-transaction")
        if ret.cr_exit_status != 0:
            log.cl_stderr("failed to run yum-complete-transaction on host "
                          "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
            return -1

        log.cl_stdout("installing backup kernel in case something bad happens "
                      "to Lustre kernel on host [%s]", self.sh_hostname)
        ret = self.sh_run(log, "package-cleanup --oldkernels --count=2 -y")
        if ret.cr_exit_status != 0:
            log.cl_stderr("failed to cleanup old kernels on host [%s], "
                          "ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
            return -1

        ret = self.sh_run(log, "yum install kernel -y", timeout=1800)
        if ret.cr_exit_status != 0:
            log.cl_stderr("failed to install backup kernel on host [%s], "
                          "ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
            return -1

        log.cl_stdout("uninstalling Lustre RPMs on host [%s]",
                      self.sh_hostname)
        ret = self.sh_rpm_find_and_uninstall(log, "grep lustre | grep -v clownfish")
        if ret != 0:
            log.cl_stderr("failed to uninstall Lustre RPMs on host "
                          "[%s]", self.sh_hostname)
            return -1

        zfs_rpms = ["libnvpair1", "libuutil1", "libzfs2", "libzpool2",
                    "kmod-spl", "kmod-zfs", "spl", "zfs"]
        rpm_string = ""
        for zfs_rpm in zfs_rpms:
            retval = self.sh_run(log, "rpm -qi %s" % zfs_rpm)
            if retval.cr_exit_status == 0:
                if rpm_string != "":
                    rpm_string += " "
                rpm_string += zfs_rpm

        if rpm_string != "":
            retval = self.sh_run(log, "rpm -e --nodeps %s" % rpm_string)
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to uninstall ZFS RPMs on host "
                              "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        log.cl_stdout("uninstalled RPMs on host [%s]", self.sh_hostname)
        return 0

    def lsh_lustre_utils_install(self, log):
        # pylint: disable=too-many-return-statements,too-many-branches
        """
        Install other util RPMs required by running Lustre tests
        """
        log.cl_stdout("installing requested utils of Lustre on host [%s]",
                      self.sh_hostname)

        # attr, bc, dbench: lustre test RPM
        # lsof: mlnx-ofa_kernel and mlnx-ofa_kernel-modules RPM
        # net-snmp-libs, net-snmp-agent-libs: lustre RPM
        # pciutils: ?
        # pdsh: lustre test RPM
        # procps: ?
        # sg3_utils: ?
        # nfs, nfs-utils: LATEST itself.
        dependent_rpms = ["attr", "bc", "dbench", "bzip2", "lsof",
                          "net-snmp-libs", "pciutils", "pdsh", "procps",
                          "sg3_utils", "nfs-utils", "sysstat"]

        distro = self.sh_distro(log)
        if distro == ssh_host.DISTRO_RHEL7:
            dependent_rpms += ["net-snmp-agent-libs"]
        else:
            log.cl_stderr("unsupported distro of host [%s]",
                          self.sh_hostname)
            return -1

        retval = self.sh_run(log, "rpm -qa | grep epel-release")
        if retval.cr_exit_status != 0:
            if distro == ssh_host.DISTRO_RHEL6:
                retval = self.sh_run(log, "rpm -Uvh %s" % EPEL_RPM_RHEL6_RPM)
            else:
                retval = self.sh_run(log, "yum install epel-release -y")
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to install EPEL RPM on host [%s]",
                              self.sh_hostname)
                return -1

        command = "yum install -y"
        for rpm in dependent_rpms:
            command += " " + rpm

        retval = self.sh_run(log, command, timeout=ssh_host.LONGEST_TIME_YUM_INSTALL)
        if retval.cr_exit_status != 0:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        host_key_config = "StrictHostKeyChecking no"
        retval = self.sh_run(log, r"grep StrictHostKeyChecking /etc/ssh/ssh_config "
                             r"| grep -v \#")
        if retval.cr_exit_status != 0:
            retval = self.sh_run(log, "echo '%s' >> /etc/ssh/ssh_config" %
                                 host_key_config)
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to change ssh config on host [%s]",
                              self.sh_hostname)
                return -1
        elif retval.cr_stdout != host_key_config + "\n":
            log.cl_stderr("unexpected StrictHostKeyChecking config on host "
                          "[%s], expected [%s], got [%s]",
                          self.sh_hostname, host_key_config, retval.cr_stdout)
            return -1

        # RHEL6 doesn't has perl-File-Path in yum by default
        if distro == ssh_host.DISTRO_RHEL7:
            # perl-File-Path is reqired by lustre-iokit
            retval = self.sh_run(log, "yum install perl-File-Path -y")
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to install perl-File-Path on host [%s]",
                              self.sh_hostname)
                return -1

        log.cl_stdout("installed requested utils of Lustre on host [%s]",
                      self.sh_hostname)
        return 0

    def lsh_install_e2fsprogs(self, log, workspace):
        """
        Install e2fsprogs RPMs for Lustre
        """
        # pylint: disable=too-many-return-statements,too-many-locals
        lustre_rpms = self.lsh_lustre_rpms
        e2fsprogs_dir = lustre_rpms.lr_e2fsprogs_rpm_dir
        command = ("mkdir -p %s" % workspace)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        basename = os.path.basename(e2fsprogs_dir)
        host_copying_rpm_dir = workspace + "/" + basename
        host_e2fsprogs_rpm_dir = workspace + "/" + "e2fsprogs_rpms"

        ret = self.sh_send_file(log, e2fsprogs_dir, workspace)
        if ret:
            log.cl_stderr("failed to send Lustre RPMs [%s] on local host to "
                          "directory [%s] on host [%s]",
                          e2fsprogs_dir, workspace,
                          self.sh_hostname)
            return -1

        if host_copying_rpm_dir != host_e2fsprogs_rpm_dir:
            command = ("mv %s %s" % (host_copying_rpm_dir, host_e2fsprogs_rpm_dir))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        retval = self.sh_run(log, r"rpm -qp %s/`ls %s | grep '.rpm$' | grep "
                             r"'^e2fsprogs-[0-9]' | head -1` "
                             r"--queryformat '%%{version} %%{url}'" %
                             (host_e2fsprogs_rpm_dir, host_e2fsprogs_rpm_dir))
        if retval.cr_exit_status != 0:
            log.cl_stderr("no e2fsprogs rpms is provided under "
                          "directory [%s] on host [%s]",
                          host_e2fsprogs_rpm_dir, self.sh_hostname)
            return -1

        info = retval.cr_stdout.strip().split(" ")
        pattern = re.compile(r'hpdd.intel|whamcloud')
        if ('wc' not in info[0]) or (not re.search(pattern, info[1])):
            log.cl_stderr("e2fsprogs rpms provided under directory [%s] on "
                          "host [%s] don't have proper version, expected it"
                          "comes from hpdd.intel or whamcloud",
                          host_e2fsprogs_rpm_dir, self.sh_hostname)
            return -1
        rpm_version = info[0]

        need_install = False
        retval = self.sh_run(log, "rpm -q e2fsprogs "
                             "--queryformat '%{version} %{url}'")
        if retval.cr_exit_status != 0:
            need_install = True
        else:
            info = retval.cr_stdout.strip().split(" ")
            pattern = re.compile(r'hpdd.intel|whamcloud')
            if ('wc' not in info[0]) or (not re.search(pattern, info[1])):
                need_install = True

            current_version = info[0]
            if rpm_version != current_version:
                need_install = True

        if not need_install:
            log.cl_info("e2fsprogs RPMs under [%s] on host [%s] is already "
                        "installed", host_e2fsprogs_rpm_dir, self.sh_hostname)
            return 0

        log.cl_stdout("installing e2fsprogs RPMs under [%s] on host [%s]",
                      host_e2fsprogs_rpm_dir, self.sh_hostname)
        retval = self.sh_run(log, "rpm -Uvh %s/*.rpm" % host_e2fsprogs_rpm_dir)
        if retval.cr_exit_status != 0:
            log.cl_stderr("failed to install RPMs under [%s] of e2fsprogs on "
                          "host [%s], ret = %d, stdout = [%s], stderr = [%s]",
                          host_e2fsprogs_rpm_dir, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout, retval.cr_stderr)
            return -1

        log.cl_stdout("installed e2fsprogs RPMs under [%s] on host [%s]",
                      host_e2fsprogs_rpm_dir, self.sh_hostname)
        return 0

    def lsh_lustre_install(self, log, workspace):
        """
        Install Lustre RPMs on a host
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        # pylint: disable=too-many-statements
        lustre_rpms = self.lsh_lustre_rpms
        command = ("mkdir -p %s" % workspace)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_stderr("failed to run command [%s] on host [%s], "
                          "ret = [%d], stdout = [%s], stderr = [%s]",
                          command,
                          self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        basename = os.path.basename(lustre_rpms.lr_rpm_dir)
        host_copying_rpm_dir = workspace + "/" + basename
        host_lustre_rpm_dir = workspace + "/" + "lustre_rpms"

        ret = self.sh_send_file(log, lustre_rpms.lr_rpm_dir, workspace)
        if ret:
            log.cl_stderr("failed to send Lustre RPMs [%s] on local host to "
                          "directory [%s] on host [%s]",
                          lustre_rpms.lr_rpm_dir, workspace,
                          self.sh_hostname)
            return -1

        if host_copying_rpm_dir != host_lustre_rpm_dir:
            command = ("mv %s %s" % (host_copying_rpm_dir, host_lustre_rpm_dir))
            retval = self.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_stderr("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        if self.lsh_lustre_utils_install(log) != 0:
            log.cl_stderr("failed to install requested utils of Lustre on host [%s]",
                          self.sh_hostname)
            return -1

        # always update dracut-kernel first
        log.cl_stdout("installing dracut-kernel RPM on host [%s]",
                      self.sh_hostname)
        retval = self.sh_run(log, "yum update dracut-kernel -y")
        if retval.cr_exit_status != 0:
            log.cl_stderr("failed to install dracut-kernel RPM on "
                          "host [%s], ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, retval.cr_exit_status,
                          retval.cr_stdout, retval.cr_stderr)
            return -1

        log.cl_stdout("installing kernel RPM on host [%s]",
                      self.sh_hostname)
        if RPM_KERNEL_FIRMWARE in lustre_rpms.lr_rpm_names:
            rpm_name = lustre_rpms.lr_rpm_names[RPM_KERNEL_FIRMWARE]
            retval = self.sh_run(log, "rpm -ivh --force %s/%s" %
                                 (host_lustre_rpm_dir, rpm_name),
                                 timeout=ssh_host.LONGEST_TIME_RPM_INSTALL)
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to install kernel RPM on host [%s], "
                              "ret = %d, stdout = [%s], stderr = [%s]",
                              self.sh_hostname, retval.cr_exit_status,
                              retval.cr_stdout, retval.cr_stderr)
                return -1

        rpm_name = lustre_rpms.lr_rpm_names[RPM_KERNEL]
        retval = self.sh_run(log, "rpm -ivh --force %s/%s" %
                             (host_lustre_rpm_dir, rpm_name),
                             timeout=ssh_host.LONGEST_TIME_RPM_INSTALL)
        if retval.cr_exit_status != 0:
            log.cl_stderr("failed to install kernel RPM on host [%s], "
                          "ret = %d, stdout = [%s], stderr = [%s]",
                          self.sh_hostname, retval.cr_exit_status,
                          retval.cr_stdout, retval.cr_stderr)
            return -1

        if self.sh_distro(log) == ssh_host.DISTRO_RHEL6:
            # Since the VM might not have more than 8G memory, crashkernel=auto
            # won't save any memory for Kdump
            log.cl_stdout("changing boot argument of crashkernel on host [%s]",
                          self.sh_hostname)
            retval = self.sh_run(log, "sed -i 's/crashkernel=auto/"
                                 "crashkernel=128M/g' /boot/grub/grub.conf")
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to change boot argument of crashkernel "
                              "on host [%s], ret = %d, stdout = [%s], "
                              "stderr = [%s]",
                              self.sh_hostname, retval.cr_exit_status,
                              retval.cr_stdout, retval.cr_stderr)
                return -1
        else:
            # Somehow crashkernel=auto doen't work for RHEL7 sometimes
            log.cl_info("changing boot argument of crashkernel on host [%s]",
                        self.sh_hostname)
            retval = self.sh_run(log, "sed -i 's/crashkernel=auto/"
                                 "crashkernel=128M/g' /boot/grub2/grub.cfg")
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to change boot argument of crashkernel "
                              "on host [%s], ret = %d, stdout = [%s], "
                              "stderr = [%s]",
                              self.sh_hostname, retval.cr_exit_status,
                              retval.cr_stdout, retval.cr_stderr)
                return -1

        # install ofed if necessary
        log.cl_info("installing OFED RPM on host [%s]", self.sh_hostname)
        retval = self.sh_run(log, "ls %s | grep mlnx-ofa_kernel" %
                             host_lustre_rpm_dir)

        if retval.cr_exit_status == 0:
            log.cl_stdout("installing OFED RPM on host [%s]",
                          self.sh_hostname)
            retval = self.sh_run(log, "rpm -ivh --force "
                                 "%s/mlnx-ofa_kernel*.rpm" %
                                 host_lustre_rpm_dir)
            if retval.cr_exit_status != 0:
                retval = self.sh_run(log, "yum localinstall -y --nogpgcheck "
                                     "%s/mlnx-ofa_kernel*.rpm" %
                                     host_lustre_rpm_dir)
                if retval.cr_exit_status != 0:
                    log.cl_stderr("failed to install OFED RPM on host [%s], "
                                  "ret = %d, stdout = [%s], stderr = [%s]",
                                  self.sh_hostname, retval.cr_exit_status,
                                  retval.cr_stdout, retval.cr_stderr)
                    return -1

        if self.lsh_install_e2fsprogs(log, workspace):
            return -1

        # Remove any files under the test directory to avoid FID problem
        log.cl_stdout("removing directory [%s] on host [%s]",
                      LUSTRE_TEST_SCRIPT_DIR, self.sh_hostname)
        retval = self.sh_run(log, "rm %s -fr" % LUSTRE_TEST_SCRIPT_DIR)
        if retval.cr_exit_status != 0:
            log.cl_stderr("failed to remove [%s] on host "
                          "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                          LUSTRE_TEST_SCRIPT_DIR, self.sh_hostname,
                          retval.cr_exit_status,
                          retval.cr_stdout,
                          retval.cr_stderr)
            return -1

        if lustre_rpms.lr_zfs_support:
            log.cl_info("installing ZFS RPMs on host [%s]", self.sh_hostname)
            install_timeout = ssh_host.LONGEST_SIMPLE_COMMAND_TIME * 2
            version = lustre_rpms.lr_lustre_version
            kernel_major_version = version.lv_kernel_major_version
            retval = self.sh_run(log, "cd %s && rpm -ivh libnvpair1-* libuutil1-* "
                                 "libzfs2-0* libzpool2-0* kmod-spl-%s* "
                                 "kmod-zfs-%s* spl-0* zfs-0*" %
                                 (host_lustre_rpm_dir, kernel_major_version,
                                  kernel_major_version),
                                 timeout=install_timeout)
            if retval.cr_exit_status != 0:
                log.cl_stderr("failed to install ZFS RPMs on host "
                              "[%s], ret = %d, stdout = [%s], stderr = [%s]",
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1

        log.cl_stdout("installing RPMs on host [%s]", self.sh_hostname)
        for rpm_type in LUSTRE_RPM_TYPES:
            if rpm_type not in lustre_rpms.lr_rpm_names:
                continue
            install_timeout = ssh_host.LONGEST_SIMPLE_COMMAND_TIME * 2
            retval = self.sh_run(log, "rpm -ivh --force %s/%s" %
                                 (host_lustre_rpm_dir,
                                  lustre_rpms.lr_rpm_names[rpm_type]),
                                 timeout=install_timeout)
            if retval.cr_exit_status != 0:
                if failure_caused_by_ksym(log, retval):
                    retval = self.sh_run(log, "rpm -ivh --force --nodeps %s/%s" %
                                         (host_lustre_rpm_dir,
                                          lustre_rpms.lr_rpm_names[rpm_type]),
                                         timeout=install_timeout)
                    if retval.cr_exit_status == 0:
                        continue

                retval = self.sh_run(log, "yum localinstall -y --nogpgcheck %s/%s" %
                                     (host_lustre_rpm_dir,
                                      lustre_rpms.lr_rpm_names[rpm_type]),
                                     timeout=install_timeout)
                if retval.cr_exit_status != 0:
                    log.cl_stderr("failed to install [%s] RPM on host [%s], "
                                  "ret = %d, stdout = [%s], stderr = [%s]",
                                  rpm_type, self.sh_hostname,
                                  retval.cr_exit_status, retval.cr_stdout,
                                  retval.cr_stderr)
                    return -1

        log.cl_stdout("installed RPMs under [%s] on host [%s]",
                      host_lustre_rpm_dir, self.sh_hostname)

        return 0

    def lsh_lustre_reinstall(self, log, workspace):
        """
        Reinstall Lustre RPMs
        """
        log.cl_stdout("reinstalling Lustre RPMs on host [%s]", self.sh_hostname)

        ret = self.lsh_lustre_uninstall(log)
        if ret:
            log.cl_stderr("failed to uninstall Lustre RPMs on host [%s]",
                          self.sh_hostname)
            return -1

        ret = self.lsh_lustre_install(log, workspace)
        if ret != 0:
            log.cl_stderr("failed to install RPMs on host [%s]",
                          self.sh_hostname)
            return -1

        log.cl_stdout("reinstalled Lustre RPMs on host [%s]", self.sh_hostname)
        return 0

    def lsh_can_skip_install(self, log):
        """
        Check whether the install of Lustre RPMs could be skipped
        """
        lustre_rpms = self.lsh_lustre_rpms
        for rpm_name in lustre_rpms.lr_rpm_names.values():
            log.cl_debug("checking whether RPM [%s] is installed on "
                         "host [%s]", rpm_name, self.sh_hostname)
            name, ext = os.path.splitext(rpm_name)
            if ext != ".rpm":
                log.cl_debug("RPM [%s] does not have .rpm subfix,"
                             "go on anyway", rpm_name)
            retval = self.sh_run(log, "rpm -qi %s" % name)
            if retval.cr_exit_status != 0:
                log.cl_stdout("RPM [%s] is not installed on host [%s], "
                              "will not skip install",
                              rpm_name, self.sh_hostname)
                return False
        return True

    def lsh_lustre_check_clean(self, log):
        """
        Check whether the host is clean for running Lustre
        """
        log.cl_stdout("checking whether host [%s] is clean for running "
                      "Lustre", self.sh_hostname)
        kernel_version = self.lsh_lustre_rpms.lr_kernel_version
        # Check whether kernel is installed kernel
        if not self.sh_is_up(log):
            log.cl_stderr("host [%s] is not up", self.sh_hostname)
            return -1

        if kernel_version != self.sh_get_kernel_ver(log):
            log.cl_stderr("host [%s] has a wrong kernel version, expected "
                          "[%s], got [%s]", self.sh_hostname, kernel_version,
                          self.sh_get_kernel_ver(log))
            return -1

        # Run some fundamental command to check Lustre is installed correctly
        check_commands = ["lustre_rmmod", "depmod -a", "modprobe lustre"]
        for command in check_commands:
            retval = self.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_stdout("failed to run command [%s] on host [%s], "
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command,
                              self.sh_hostname,
                              retval.cr_exit_status,
                              retval.cr_stdout,
                              retval.cr_stderr)
                return -1
        log.cl_stdout("host [%s] is clean to run Lustre",
                      self.sh_hostname)
        return 0

    def _lsh_lustre_prepare(self, log, workspace, lazy_prepare=False):
        """
        Prepare the host for running Lustre
        Write lock should be hold
        """
        # pylint: disable=too-many-branches
        log.cl_stdout("preparing host [%s] for Lustre", self.sh_hostname)
        lustre_rpms = self.lsh_lustre_rpms
        if lazy_prepare and self.lsh_can_skip_install(log):
            log.cl_stdout("skipping installation of Lustre RPMs on host [%s]",
                          self.sh_hostname)
        else:
            ret = self.lsh_lustre_reinstall(log, workspace)
            if ret:
                log.cl_stderr("failed to reinstall Lustre RPMs on host [%s]",
                              self.sh_hostname)
                return -1

        # Generate the /etc/hostid so that mkfs.lustre with ZFS won't complain
        command = "genhostid"
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        need_reboot = False
        ret = self.lsh_lustre_umount_services(log)
        if ret:
            log.cl_stdout("failed to umount Lustre clients and servers on "
                          "host [%s], reboot is needed",
                          self.sh_hostname)
            need_reboot = True

        if lazy_prepare and not need_reboot:
            ret = self.lsh_lustre_check_clean(log)
            if ret:
                log.cl_debug("host [%s] need a reboot to change the kernel "
                             "or cleanup the status of Lustre",
                             self.sh_hostname)
                need_reboot = True

        if not lazy_prepare:
            need_reboot = True

        if need_reboot:
            ret = self.sh_kernel_set_default(log, lustre_rpms.lr_kernel_version)
            if ret:
                log.cl_stderr("failed to set default kernel of host [%s] to [%s]",
                              self.sh_hostname, lustre_rpms.lr_kernel_version)
                return -1

            ret = self.sh_reboot(log)
            if ret:
                log.cl_stderr("failed to reboot host [%s]", self.sh_hostname)
                return -1

            ret = self.lsh_lustre_check_clean(log)
            if ret:
                log.cl_stderr("failed to check Lustre status after reboot on host [%s]",
                              self.sh_hostname)
                return -1

        log.cl_stdout("prepared host [%s] for Lustre", self.sh_hostname)
        return 0

    def lsh_lustre_prepare(self, log, workspace, lazy_prepare=False):
        """
        Prepare the host for running Lustre
        Lock should be held
        """
        host_handle = self.lsh_lock.rwl_writer_acquire(log)
        if host_handle is None:
            log.cl_stderr("aborting preparing host [%s]",
                          self.sh_hostname)
            return -1
        ret = self._lsh_lustre_prepare(log, workspace,
                                       lazy_prepare=lazy_prepare)
        if ret:
            log.cl_error("failed to prepare host [%s] to run lustre",
                         self.sh_hostname)
        host_handle.rwh_release()
        return ret

    def lsh_encode(self, need_status, status_func, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if not need_structure and not need_status:
            children = []
            if len(self.lsh_ost_instances) > 0:
                children.append(cstr.CSTR_OSTS)
            if len(self.lsh_mdt_instances) > 0:
                children.append(cstr.CSTR_MDTS)
            if len(self.lsh_clients) > 0:
                children.append(cstr.CSTR_CLIENTS)
            if self.lsh_mgsi is not None:
                children.append(cstr.CSTR_MGS)
            return children

        encoded = {cstr.CSTR_HOST_ID: self.sh_host_id}
        if need_structure:
            encoded[cstr.CSTR_HOSTNAME] = self.sh_hostname

        mdt_instances = []
        for mdti in self.lsh_mdt_instances.values():
            mdt_instances.append(mdti.lsi_encode(need_status, status_func,
                                                 need_structure))
        if len(mdt_instances) > 0:
            encoded[cstr.CSTR_MDT_INSTANCES] = mdt_instances

        ost_instances = []
        for osti in self.lsh_ost_instances.values():
            ost_instances.append(osti.lsi_encode(need_status, status_func,
                                                 need_structure))
        if len(ost_instances) > 0:
            encoded[cstr.CSTR_OST_INSTANCES] = ost_instances

        clients = []
        for client in self.lsh_clients.values():
            clients.append(client.lc_encode(need_status, status_func,
                                            need_structure))
        if len(clients) > 0:
            encoded[cstr.CSTR_CLIENTS] = clients
        return encoded

    def lsh_getname(self, log, path):
        """
        return the Lustre client name on a path
        If error, return None
        """
        command = ("lfs getname %s" % (path))
        log.cl_debug("start to run command [%s] on host [%s]", command,
                     self.sh_hostname)
        retval = self.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], "
                         "stdout = [%s], stderr = [%s]",
                         command,
                         self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return None
        output = retval.cr_stdout.strip()
        name_pattern = (r"^(?P<client_name>\S+) %s$" % path)
        name_regular = re.compile(name_pattern)
        match = name_regular.match(output)
        client_name = None
        if match:
            client_name = match.group("client_name")
        else:
            log.cl_error("failed to parse output [%s] to get name" % output)
            return None
        return client_name

    def lsh_detect_mgs_filesystems(self, log):
        """
        Detect Lustre file system managed by a MGS
        """
        command = ("cat /proc/fs/lustre/mgs/MGS/filesystems")
        filesystems = []
        retval = self.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, self.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return filesystems
        for line in retval.cr_stdout.splitlines():
            filesystem = line.strip()
            filesystems.append(filesystem)
        return filesystems


class LustreHostStatus(object):
    """
    The status of a Lustre host
    """
    def __init__(self, host):
        self.lhs_host = host
        # The time the status is updated
        self.lhs_update_time = time.time()
        self.lhs_ping_success = cstr.CSTR_UNKNOWN
        self.lhs_ssh_success = cstr.CSTR_UNKNOWN

    def lhs_check(self, log):
        """
        Check the status of the service and update the update time
        """
        host = self.lhs_host
        ret = host.sh_ping(log)
        if ret:
            self.lhs_ping_success = cstr.CSTR_FALSE
        else:
            self.lhs_ping_success = cstr.CSTR_TRUE

        is_up = host.sh_is_up(log)
        if is_up:
            self.lhs_ssh_success = cstr.CSTR_TRUE
        else:
            self.lhs_ssh_success = cstr.CSTR_FALSE
        self.lhs_update_time = time.time()

    def lhs_has_problem(self):
        """
        If the status of the service has problem, return True
        Else, return False
        """
        if self.lhs_ping_success != cstr.CSTR_TRUE:
            return True
        if self.lhs_ssh_success != cstr.CSTR_TRUE:
            return True
        return False


def get_fsname_from_service_name(service_name):
    """
    Extract fsname from the volume name
    """
    fields = service_name.split('-')
    if len(fields) != 2:
        return None
    return fields[0]


def detect_lustre_clients(log, host):
    """
    Detect Lustre clients from the host
    """
    client_pattern = (r"^.+:/(?P<fsname>\S+) (?P<mount_point>\S+) lustre .+$")
    client_regular = re.compile(client_pattern)

    # Detect Lustre client
    command = ("cat /proc/mounts | grep lustre")
    retval = host.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return []

    clients = []
    for line in retval.cr_stdout.splitlines():
        log.cl_debug("checking line [%s]", line)
        match = client_regular.match(line)
        if match:
            mount_point = match.group("mount_point")
            fsname = match.group("fsname")
            lustre_fs = LustreFilesystem(fsname)
            client = LustreClient(log, lustre_fs, host, mount_point)
            clients.append(client)
            log.cl_debug("client [%s] mounted on dir [%s] of host [%s]",
                         fsname, mount_point, host.sh_hostname)
    return clients


def lfs_fid2path(log, fid, fsname_rootpath):
    """
    Transfer FID to fpath
    """
    command = ("lfs fid2path %s %s" % (fsname_rootpath, fid))
    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    return retval.cr_stdout.strip()


def fid_path(fid, fsname_rootpath):
    """
    Get the fid path of a file
    """
    return "%s/.lustre/fid/%s" % (fsname_rootpath, fid)


def lfs_hsm_archive(log, fpath, archive_id, host=None):
    """
    HSM archive
    """
    command = ("lfs hsm_archive --archive %s %s" % (archive_id, fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = (" on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_restore(log, fpath, host=None):
    """
    HSM restore
    """
    command = ("lfs hsm_restore %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_release(log, fpath, host=None):
    """
    HSM release
    """
    command = ("lfs hsm_release %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_remove(log, fpath, host=None):
    """
    HSM remove
    """
    command = ("lfs hsm_remove %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lfs_hsm_cancel(log, fpath, host=None):
    """
    HSM remove
    """
    command = ("lfs hsm_cancel %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


def lustre_unlink(log, path, host=None):
    """
    Basic file remove
    Only remove general files and empty dirs.
    """
    # ignore the remove if fpath is root dir
    if path == "/":
        log.cl_error("trying to remove root dir, skipping it.")
        return -1

    command = "if [ -d {0} ];then rmdir {0};else rm -f {0};fi".format(path)
    extra_string = ""

    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    return 0


class HSMState(object):
    """
    The HSM state
    """
    # pylint: disable=too-few-public-methods
    HS_NONE = 0x00000000
    HS_EXISTS = 0x00000001
    HS_DIRTY = 0x00000002
    HS_RELEASED = 0x00000004
    HS_ARCHIVED = 0x00000008
    HS_NORELEASE = 0x00000010
    HS_NOARCHIVE = 0x00000020
    HS_LOST = 0x00000040

    def __init__(self, states, archive_id=0):
        self.hs_states = states
        self.hs_archive_id = archive_id

    def __eq__(self, other):
        return (self.hs_states == other.hs_states and
                self.hs_archive_id == other.hs_archive_id)

    def hs_string(self):
        """
        return string of the status
        """
        output = hex(self.hs_states)
        if self.hs_states & HSMState.HS_RELEASED:
            output += " released"
        if self.hs_states & HSMState.HS_EXISTS:
            output += " exists"
        if self.hs_states & HSMState.HS_DIRTY:
            output += " dirty"
        if self.hs_states & HSMState.HS_ARCHIVED:
            output += " archived"
        # Display user-settable flags
        if self.hs_states & HSMState.HS_NORELEASE:
            output += " never_release"
        if self.hs_states & HSMState.HS_NOARCHIVE:
            output += " never_archive"
        if self.hs_states & HSMState.HS_LOST:
            output += " lost_from_hsm"
        if self.hs_archive_id != 0:
            output += (", archive_id:%d" % self.hs_archive_id)
        return output


HSM_STATE_PATTERN = (r"^\: \((?P<states>.+)\).*$")
HSM_STATE_REGULAR = re.compile(HSM_STATE_PATTERN)
HSM_ARCHIVE_ID_PATTERN = (r"^\: \((?P<states>.+)\).+, archive_id:(?P<archive_id>.+)$")
HSM_ARCHIVE_ID_REGULAR = re.compile(HSM_ARCHIVE_ID_PATTERN)


def lfs_hsm_state(log, fpath, host=None):
    """
    HSM state
    """
    command = ("lfs hsm_state %s" % (fpath))
    extra_string = ""
    if host is None:
        retval = utils.run(command)
    else:
        retval = host.sh_run(log, command)
        extra_string = ("on host [%s]" % host.sh_hostname)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s]%s, "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, extra_string,
                     retval.cr_exit_status, retval.cr_stdout,
                     retval.cr_stderr)
        return None

    output = retval.cr_stdout.strip()
    if not output.startswith(fpath):
        log.cl_error("unexpected output [%s]", output)
        return None

    fpath_len = len(fpath)
    output = output[fpath_len:]
    match = HSM_STATE_REGULAR.match(output)
    if not match:
        log.cl_error("output [%s] doesn't mather pattern [%s]",
                     output, HSM_STATE_PATTERN)
        return None

    states = int(match.group("states"), 16)
    archive_id = 0
    match = HSM_ARCHIVE_ID_REGULAR.match(output)
    if match:
        archive_id = int(match.group("archive_id"))
    return HSMState(states, archive_id)


def check_hsm_state(log, fpath, states, archive_id=0, host=None):
    """
    Check the current HSM state
    """
    expected_state = HSMState(states, archive_id=archive_id)
    state = lfs_hsm_state(log, fpath, host=host)
    if state is None:
        log.cl_debug("failed to get HSM state")
        return -1
    if state == expected_state:
        log.cl_debug("successfully got expected HSM states [%s]",
                     expected_state.hs_string())
        return 0
    log.cl_debug("got HSM state [%s], expected [%s]",
                 state.hs_string(), expected_state.hs_string())
    return -1


def wait_hsm_state(log, fpath, states, archive_id=0, host=None,
                   timeout=90, sleep_interval=1):
    """
    Wait util the HSM state changes to the expected state
    """
    # pylint: disable=too-many-arguments
    waited = 0
    expected_state = HSMState(states, archive_id=archive_id)
    while True:
        state = lfs_hsm_state(log, fpath, host=host)
        if state is None:
            return -1

        if state == expected_state:
            log.cl_debug("expected HSM states [%s]", expected_state.hs_string())
            return 0

        if waited < timeout:
            waited += sleep_interval
            time.sleep(sleep_interval)
            continue
        log.cl_error("timeout when waiting the hsm state, expected [%s], "
                     "got [%s]", expected_state.hs_string(), state.hs_string())
        return -1
    return -1


def get_fsname(log, host, device_path):
    """
    Get file system name either from ldiskfs or ZFS.
    """
    ret, info_dict = host.sh_dumpe2fs(log, device_path)
    if ret:
        srv_name = host.sh_zfs_get_srvname(log, device_path)
        if srv_name is None:
            log.cl_error("failed to get service name from device [%s] "
                         "on host [%s] either as ZFS or as ldiskfs",
                         device_path, host.sh_hostname)
            return -1, None
    else:
        srv_name = info_dict["Filesystem volume name"]
    fsname = get_fsname_from_service_name(srv_name)
    if fsname is None:
        log.cl_error("failed to get fsname from service name [%s] of "
                     "device [%s] on host [%s]", srv_name,
                     device_path, host.sh_hostname)
        return -1, None
    return 0, fsname


def lfs_path2fid(log, host, fpath):
    """
    Transfer fpath to FID string
    """
    command = ("lfs path2fid %s" % (fpath))
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

    fid = retval.cr_stdout.strip()
    if len(fid) < 2 or fid[0] != '[' or fid[-1] != ']':
        log.error("invalid fid [%s]", fid)
        return None
    fid = fid[1:-1]
    return fid


class LustreFID(object):
    """
    FID
    """
    # pylint: disable=too-few-public-methods
    # fid_string: 0x200000400:0xaa1:0x0
    def __init__(self, log, fid_string):
        self.lf_fid_string = fid_string
        fields = fid_string.split(':')
        if len(fields) != 3:
            reason = ("invalid FID %s" % fid_string)
            log.cl_error(reason)
            raise Exception(reason)

        self.lf_seq = int(fields[0], 16)
        self.lf_oid = int(fields[1], 16)
        self.lf_ver = int(fields[2], 16)

    def lf_posix_archive_path(self, archive_dir):
        """
        Get the posix archive path
        """
        return ("%s/%04x/%04x/%04x/%04x/%04x/%04x/%s" %
                (archive_dir, self.lf_oid & 0xFFFF,
                 self.lf_oid >> 16 & 0xFFFF,
                 self.lf_seq & 0xFFFF,
                 self.lf_seq >> 16 & 0xFFFF,
                 self.lf_seq >> 32 & 0xFFFF,
                 self.lf_seq >> 48 & 0xFFFF,
                 self.lf_fid_string))


def host_lustre_prepare(log, workspace, host, lazy_prepare=False):
    """
    wrapper of lsh_lustre_prepare for parrallism
    """
    return host.lsh_lustre_prepare(log, workspace,
                                   lazy_prepare=lazy_prepare)


def lustre_file_setstripe(log, host, fpath, stripe_index=-1, stripe_count=1):
    """
    use lfs_setstripe to create a file
    """
    command = ("lfs setstripe -i %s -c %s %s" %
               (stripe_index, stripe_count, fpath))
    retval = host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1, None
    return 0
