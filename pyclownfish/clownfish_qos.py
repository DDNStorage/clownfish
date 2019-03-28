# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
QoS Library for clownfish
Clownfish is an automatic management system for Lustre
"""
import time
import httplib
import re
import os
import threading
import hashlib

from pylustre import utils
from pylustre import lustre
from pylustre import cstr
from pyclownfish import esmon_influxdb


INFLUXDB_DATABASE_NAME = "esmon_database"


class ClownfishDecayQoSUser(object):
    """
    Each user has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, uid, interval, mbps_threshold, throttled_oss_rpc_rate,
                 iops_threshold, throttled_mds_rpc_rate):
        self.cdqosu_uid = uid
        self.cdqosu_mbps_threshold = mbps_threshold
        self.cdqosu_throughput_threshold = mbps_threshold * interval
        self.cdqosu_throttled_oss_rpc_rate = throttled_oss_rpc_rate
        self.cdqosu_iops_threshold = iops_threshold
        self.cdqosu_throttled_mds_rpc_rate = throttled_mds_rpc_rate
        self.cdqosu_metadata_threshold = iops_threshold * interval

    def cdqosu_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if not need_structure and not need_status:
            return [cstr.CSTR_UID,
                    cstr.CSTR_MBPS_THRESHOLD,
                    cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                    cstr.CSTR_IOPS_THRESHOLD,
                    cstr.CSTR_THROTTLED_MDS_RPC_RATE]
        else:
            return {cstr.CSTR_UID: self.cdqosu_uid,
                    cstr.CSTR_MBPS_THRESHOLD: self.cdqosu_mbps_threshold,
                    cstr.CSTR_THROTTLED_OSS_RPC_RATE: self.cdqosu_throttled_oss_rpc_rate,
                    cstr.CSTR_IOPS_THRESHOLD: self.cdqosu_iops_threshold,
                    cstr.CSTR_THROTTLED_MDS_RPC_RATE: self.cdqosu_throttled_mds_rpc_rate}


class ClownfishDecayQosClientOst(object):
    """
    The information of a OST collected from Lustre client of a file system
    for QoS
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, client, ost):
        self.cdqco_ost = ost
        self.cdqco_client = client
        self.cdqco_lustre_client = client.cdqc_lustre_client
        self.cdqco_host = self.cdqco_lustre_client.lc_host
        self.cdqco_client = client
        self.cdqco_test_dir = client.cdqc_test_dir + "/" + ost.ls_service_name

    def cdqco_latency_check(self, log):
        """
        Check the latency of this OST on the client
        """
        host = self.cdqco_host
        command = ("dd if=/dev/zero of=%s/file bs=1048576 count=100 done" %
                   (self.cdqco_test_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command, host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0


def lcreatemany_path(log):
    """
    Find the path of lcreatemany binary file
    """
    path = "src/lcreatemany"
    command = "test -x %s" % path
    retval = utils.run(command)
    if retval.cr_exit_status != 0:
        log.cl_info("failed to run command [%s]")
        path = "/usr/bin/lcreatemany"
        command = "test -x %s" % path
        retval = utils.run(command)
        if retval.cr_exit_status != 0:
            log.cl_error("no lcreatemany is found")
            return None
    return path


class ClownfishDecayQosClientMdt(object):
    """
    The information of a MDT collected from Lustre client of a file system
    for QoS
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, client, mdt):
        self.cdqcm_mdt = mdt
        self.cdqcm_client = client
        self.cdqcm_lustre_client = client.cdqc_lustre_client
        self.cdqcm_host = self.cdqcm_lustre_client.lc_host
        self.cdqcm_test_dir = client.cdqc_test_dir + "/" + mdt.ls_service_name
        self.cdqcm_file_number = 10000
        self.cdqcm_congestion_rpc_limit = None

    def _cqdcm_has_files(self, log):
        """
        Check whether the file system has files
        """
        host = self.cdqcm_host

        command = ("ls %s/0" % (self.cdqcm_test_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            return False

        command = ("ls %s/%s" %
                   (self.cdqcm_test_dir, self.cdqcm_file_number - 1))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            return False
        return True

    def _cdqcm_create_files(self, log):
        """
        Create the files
        """
        host = self.cdqcm_host

        local_lcreatemany_path = lcreatemany_path(log)
        if local_lcreatemany_path is None:
            return -1

        command = ("mkdir -p %s" % (self.cdqcm_test_dir))
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

        command = ("lfs getdirstripe -i %s" % (self.cdqcm_test_dir))
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

        lmv_stripe_offset = retval.cr_stdout.strip()
        if lmv_stripe_offset != str(self.cdqcm_mdt.ls_index):
            command = ("rm -fr %s" % (self.cdqcm_test_dir))
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

            command = ("lfs setdirstripe -c 1 -i %s %s" %
                       (self.cdqcm_mdt.ls_index, self.cdqcm_test_dir))
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

        ret = host.sh_send_file(log, local_lcreatemany_path, self.cdqcm_test_dir)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         local_lcreatemany_path, self.cdqcm_test_dir,
                         host.sh_hostname)
            return -1

        cmd_path = self.cdqcm_test_dir + "/lcreatemany"
        # Come on, the file system should be quicker than 1 file per second
        command = ("%s %s %s %s" % (cmd_path, self.cdqcm_test_dir,
                                    self.cdqcm_file_number,
                                    self.cdqcm_file_number))
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

    def cdqcm_latency_check(self, log):
        """
        Check the latency of this MDT on the client
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        host = self.cdqcm_host
        hostname = host.sh_hostname
        lustre_client = self.cdqcm_lustre_client
        client_mnt = lustre_client.lc_mnt
        fsname = lustre_client.lc_lustre_fs.lf_fsname
        mdt = self.cdqcm_mdt

        if not self._cqdcm_has_files(log):
            log.cl_info("creating files for latency check of MDT [%s] on "
                        "host [%s]", self.cdqcm_mdt.ls_service_name,
                        hostname)
            ret = self._cdqcm_create_files(log)
            if ret:
                log.cl_error("failed to create files")

        client_name = host.lsh_getname(client_mnt)
        if client_name is None:
            log.cl_error("failed to get the client name of dir [%s] on host "
                         "[%s]", client_mnt, hostname)
            return -1

        leading = fsname + "-"
        if not client_name.startswith(leading):
            log.cl_error("client name [%s] of dir [%s] on host [%s] doesn't start "
                         "with [%s]", client_name, client_mnt,
                         hostname, leading)
            return -1
        fields = client_name.split("-")
        if len(fields) != 2:
            log.cl_error("invalid client name [%s] of dir [%s] on host [%s]",
                         client_name, client_mnt, hostname)
            return -1
        client_uuid = fields[1]

        param_path = ("ldlm.namespaces.%s-mdc-%s.lru_size" %
                      (mdt.ls_service_name, client_uuid))
        command = ("lctl set_param %s=clear" % param_path)
        log.cl_info("dropping cache for latency check of MDT [%s] on "
                    "host [%s]", self.cdqcm_mdt.ls_service_name,
                    host.sh_hostname)
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        #ret = mdt.lmt_prevent_congestion_by_tbf(log, 10)
        #if ret:
        #    log.cl_error("failed to prevent congestion by TBF")
        #    return -1

        log.cl_info("listing files for latency check of MDT [%s] on "
                    "host [%s]", self.cdqcm_mdt.ls_service_name,
                    host.sh_hostname)
        command = ("time -f %%E /usr/bin/ls -l %s | wc -l" % (self.cdqcm_test_dir))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        fields = retval.cr_stderr.split(":")
        if len(fields) == 2:
            hour = 0.0
            minute = float(fields[0])
            second = float(fields[1])
        elif len(fields) == 3:
            hour = float(fields[0])
            minute = float(fields[1])
            second = float(fields[2])
        else:
            log.cl_error("unexpected output of command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        second = hour * 3600 + minute * 60 + second

        number = float(retval.cr_stdout.strip())
        rate = number / second

        # XXX
        log.cl_info("ls time: %s seconds, rate %s", second, rate)
        rate_threshold = 10
        if rate < rate_threshold:
            rpc_limit = self.cdqcm_congestion_rpc_limit
            if rpc_limit is None:
                rpc_limit = 10
            log.cl_info("ls rate is too slow (< %s), trying to prevent "
                        "congestion by enforcing TBF rules", rate_threshold)
            ret = mdt.lmt_prevent_congestion_by_tbf(log, rpc_limit)
            if ret:
                log.cl_error("failed to prevent congestion by TBF")
                return -1
            self.cdqcm_congestion_rpc_limit = rpc_limit
        return 0


class ClownfishDecayQosClient(object):
    """
    The information collected from Lustre client of a file system for QoS
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, lustre_client):
        self.cdqc_lustre_client = lustre_client
        lustre_fs = lustre_client.lc_lustre_fs
        # Key is service names of OSTs
        self.cdqc_osts = {}
        self.cdqc_host = lustre_client.lc_host
        # Add md5 of the mnt to prevent conflict of two mnts on the same host
        mnt_md5 = hashlib.md5(lustre_client.lc_mnt).hexdigest()
        self.cdqc_test_dir = (lustre_client.lc_mnt + "/clownfish_qos/" +
                              self.cdqc_host.sh_hostname + "/" + mnt_md5)
        for service_name, ost in lustre_fs.lf_osts.iteritems():
            client_ost = ClownfishDecayQosClientOst(self, ost)
            self.cdqc_osts[service_name] = client_ost

        # Key is service names of MDTs
        self.cdqc_mdts = {}
        for service_name, mdt in lustre_fs.lf_mdts.iteritems():
            client_mdt = ClownfishDecayQosClientMdt(self, mdt)
            self.cdqc_mdts[service_name] = client_mdt

    def cdqc_latency_check(self, log):
        """
        Check the latency of MDTs and OSTs
        """
        client_name = self.cdqc_lustre_client.lc_client_name
        retval = 0
        #for service_name, client_ost in self.cdqc_osts.iteritems():
        #    ret = client_ost.cdqco_latency_check(log)
        #    if ret:
        #        log.cl_error("failed to check the latency of OST [%s] on "
        #                     "client [%s]", service_name, client_name)
        #        retval = ret

        for service_name, client_mdt in self.cdqc_mdts.iteritems():
            ret = client_mdt.cdqcm_latency_check(log)
            if ret:
                log.cl_error("failed to check the latency of MDT [%s] on "
                             "client [%s]", service_name, client_name)
                retval = ret

        return retval


class ClownfishDecayQoS(object):
    """
    At the start of each time interval, all TBF limitations will be cleared.
    The I/O throughput of each user will be accumulated during this time
    interval continuously. If the total I/O throughput exceeds the upper
    limit of this user, a decay factor will be enforced on this user on
    all OSTs. That decay rate is then enforced, thus the the user can only do
    very limited I/O on the whole file system. At the start of next time
    interval, all of the I/O limitations will be removed.
    """
    # pylint: disable=too-many-instance-attributes,too-few-public-methods
    def __init__(self, log, lustrefs, esmon_server_hostname,
                 interval, mbps_threshold, throttled_oss_rpc_rate,
                 iops_threshold, throttled_mds_rpc_rate,
                 esmon_collect_interval, users, enabled, global_workspace):
        # pylint: disable=too-many-arguments
        self.cdqos_lustrefs = lustrefs
        self.cdqos_global_workspace = global_workspace
        ret = lustrefs.lf_qos_add(self)
        if ret:
            reason = ("QoS already configured in file system [%s]" %
                      (lustrefs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)
        # The time interval during which the I/O throughputs will be accumulated
        self.cdqos_interval = interval
        self.cdqos_mbps_threshold = mbps_threshold
        # The upper threshold of I/O throughput during the interval
        self.cdqos_throughput_threshold = mbps_threshold * interval
        self.cdqos_throttled_oss_rpc_rate = throttled_oss_rpc_rate
        self.cdqos_iops_threshold = iops_threshold
        # The upper threshold of metadata operations during the interval
        self.cdqos_metadata_threshold = iops_threshold * interval
        self.cdqos_throttled_mds_rpc_rate = throttled_mds_rpc_rate
        self.cdqos_esmon_collect_interval = esmon_collect_interval
        self.cdqos_esmon_server_hostname = esmon_server_hostname
        self.cdqos_influxdb_client = esmon_influxdb.InfluxdbClient(esmon_server_hostname,
                                                                   INFLUXDB_DATABASE_NAME)
        self.cdqos_oss_throttled_uids = []
        self.cdqos_mds_throttled_uids = []
        self.cdqos_users = users
        self.cdqos_log = log
        self.cdqos_enabled = enabled
        # Protect the enabling/disabling process
        self.cdqos_condition = threading.Condition()
        self.cdqos_thread = None
        self.cdqos_thread_log = None
        # The information collected from Lustre clients, key is lc_client_name
        self.cdqos_clients = {}
        ret = self._cdqos_init(log)
        if ret:
            reason = ("failed to init the QoS of file system [%s]" %
                      (self.cdqos_lustrefs.lf_fsname))
            log.cl_error(reason)
            raise Exception(reason)
        if enabled:
            ret = self.cqqos_enable(log)
            if ret:
                log.cl_error("failed to enable the QoS of file system [%s]",
                             self.cdqos_lustrefs.lf_fsname)

    def _cdqos_init_clients(self):
        """
        Init Lustre clients information
        """
        for client_index, client in self.cdqos_lustrefs.lf_clients.iteritems():
            qos_client = ClownfishDecayQosClient(client)
            self.cdqos_clients[client_index] = qos_client
        return 0

    def _cdqos_init(self, log):
        """
        Init the QoS for the file system
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname

        name = "thread_qos_%s" % fsname
        thread_workspace = self.cdqos_global_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                log.cl_error("failed to create directory [%s] on local host",
                             thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.cdqos_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        self.cdqos_thread_log = log.cl_get_child(name,
                                                 resultsdir=thread_workspace)

        ret = self._cdqos_init_clients()
        if ret:
            log.cl_error("failed to init QoS Lustre clients for file system [%s]",
                         fsname)
            return -1
        return 0

    def cqqos_enable(self, log):
        """
        Enable QoS management
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname
        ret = 0

        self.cdqos_condition.acquire()
        self.cdqos_enabled = True
        if self.cdqos_thread is None:
            ret = self.cdqos_start(log)
            if ret:
                log.cl_error("failed to start QoS for file system [%s]",
                             fsname)
            else:
                log.cl_stdout("started QoS for file system [%s]",
                              fsname)
        else:
            log.cl_stdout("QoS of file system [%s] is already enabled",
                          fsname)
        if ret:
            self.cdqos_enabled = False
        self.cdqos_condition.release()
        return ret

    def cqqos_disable(self, log):
        """
        Disable QoS management
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname
        ret = 0

        self.cdqos_condition.acquire()
        self.cdqos_enabled = False
        if self.cdqos_thread is None:
            log.cl_stdout("QoS of file system [%s] is already disabled",
                          fsname)
        else:
            ret = self.cdqos_stop(log)
            if ret:
                log.cl_error("failed to stop QoS for file system [%s]", fsname)
            else:
                log.cl_stdout("stopped QoS for file system [%s]",
                              fsname)
        self.cdqos_condition.release()
        return ret

    def cdqos_encode(self, need_status, need_structure):
        """
        Return the encoded structure which can be dumped to Json/YAML string
        """
        if not need_structure and not need_status:
            return [cstr.CSTR_ENABLED,
                    cstr.CSTR_INTERVAL,
                    cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                    cstr.CSTR_MBPS_THRESHOLD,
                    cstr.CSTR_USERS,
                    cstr.CSTR_ESMON_SERVER_HOSTNAME,
                    cstr.CSTR_ESMON_COLLECT_INTERVAL]
        else:
            encoded_users = []
            for qos_user in self.cdqos_users.values():
                encoded_users.append(qos_user.cdqosu_encode(need_status, need_structure))
            encoded = {cstr.CSTR_ENABLED: self.cdqos_enabled,
                       cstr.CSTR_INTERVAL: self.cdqos_interval,
                       cstr.CSTR_THROTTLED_OSS_RPC_RATE: self.cdqos_throttled_oss_rpc_rate,
                       cstr.CSTR_MBPS_THRESHOLD: self.cdqos_mbps_threshold,
                       cstr.CSTR_USERS: encoded_users,
                       cstr.CSTR_ESMON_SERVER_HOSTNAME: self.cdqos_esmon_server_hostname,
                       cstr.CSTR_ESMON_COLLECT_INTERVAL: self.cdqos_esmon_collect_interval}
        return encoded

    def cdqos_clear_limitations(self, log):
        """
        Clear all TBF limitations
        """
        # pylint: disable=too-many-branches
        log.cl_info("clearing all TBF limiations")
        lustrefs = self.cdqos_lustrefs
        for host in lustrefs.lf_oss_list():
            ret, rules = host.lsh_get_ost_io_tbf_rule_list(log)
            if ret:
                return -1

            for name in rules:
                ret = host.lsh_stop_ost_io_tbf_rule(log, name)
                if ret:
                    return -1
        self.cdqos_oss_throttled_uids = []

        for host in lustrefs.lf_mds_list():
            ret, rules = host.lsh_get_mdt_tbf_rule_list(log)
            if ret:
                return -1

            for name in rules:
                ret = host.lsh_stop_mdt_tbf_rule(log, name)
                if ret:
                    return -1
        self.cdqos_mds_throttled_uids = []
        return 0

    def cdqos_enforce_oss_tbf(self, log, uid, rpc_limit):
        """
        Enforce TBF limiations for the uid on all OSS
        """
        uid_string = str(uid)
        if uid_string in self.cdqos_oss_throttled_uids:
            return 0

        name = "uid_" + uid_string
        lustrefs = self.cdqos_lustrefs
        for host in lustrefs.lf_oss_list():
            ret = host.lsh_stop_ost_io_tbf_rule(log, name)
            if ret:
                log.cl_debug("failed to stop rule [%s]", name)

            expression = "uid={%s}" % uid_string
            ret = host.lsh_start_ost_io_tbf_rule(log, name, expression,
                                                 rpc_limit)
            if ret:
                return -1

        self.cdqos_oss_throttled_uids.append(uid_string)
        return 0

    def cdqos_enforce_mds_tbf(self, log, uid, rpc_limit):
        """
        Enforce TBF limiations for the uid on all MDS
        """
        uid_string = str(uid)
        if uid_string in self.cdqos_mds_throttled_uids:
            return 0

        name = "uid_" + uid_string
        lustrefs = self.cdqos_lustrefs
        for host in lustrefs.lf_mds_list():
            ret = host.lsh_stop_mdt_tbf_rule(log, name)
            if ret:
                log.cl_debug("failed to stop rule [%s]", name)

            expression = "uid={%s} warning=1" % uid_string
            ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                              rpc_limit)
            if ret:
                return -1

        name = "ldlm_enqueue"
        lustrefs = self.cdqos_lustrefs
        for host in lustrefs.lf_mds_list():
            ret = host.lsh_stop_mdt_tbf_rule(log, name)
            if ret:
                log.cl_debug("failed to stop rule [%s]", name)

            expression = "opcode={ldlm_enqueue}"
            ret = host.lsh_start_mdt_tbf_rule(log, name, expression,
                                              10000)
            if ret:
                return -1

        self.cdqos_mds_throttled_uids.append(uid_string)
        return 0

    def cdqos_start(self, log):
        """
        Start the QoS for the file system
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname

        ret = lustrefs.lf_set_jobid_var(log, lustre.JOBID_VAR_PROCNAME_UID)
        if ret:
            log.cl_error("failed to set the jobid_var to [%s]",
                         lustre.JOBID_VAR_PROCNAME_UID)
            return -1

        for host in lustrefs.lf_oss_list():
            ret = host.lsh_enable_ost_io_tbf(log, lustre.TBF_TYPE_GENERAL)
            if ret:
                log.cl_error("failed to enable TBF for ost_io on file system "
                             "[%s]", fsname)
                return -1

        for host in lustrefs.lf_mds_list():
            ret = host.lsh_enable_mdt_tbf(log, lustre.TBF_TYPE_GENERAL)
            if ret:
                log.cl_error("failed to enable TBF for all MDT services on file system "
                             "[%s]", fsname)
                return -1

        self.cdqos_thread = utils.thread_start(self.cdqos_thread_main, ())
        return 0

    def cdqos_stop(self, log):
        """
        Stop the QoS for the file system
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname

        while self.cdqos_thread.is_alive():
            try:
                self.cdqos_thread.join()
            except:
                log.cl_debug("interrupt recieved when stopping QoS thread of "
                             "file system", fsname)
                return -1

        self.cdqos_thread = None
        for host in lustrefs.lf_oss_list():
            ret = host.lsh_enable_ost_io_fifo(log)
            if ret:
                log.cl_error("failed to enable FIFO NRS policy for ost_io on "
                             "file system [%s]", fsname)
                return -1

        for host in lustrefs.lf_mds_list():
            ret = host.lsh_enable_mdt_all_fifo(log)
            if ret:
                log.cl_error("failed to enable FIFO NRS policy for all MDT "
                             "services on file system [%s]", fsname)
                return -1

        return 0

    def _cdqos_throughput_check(self, log, fsname, start_time):
        # pylint: disable=too-many-branches,too-many-locals
        """
        Check whether any user exceeds througput threshold
        """
        query = ("SELECT ost_index,job_id,value FROM ost_jobstats_bytes "
                 "WHERE fs_name = '%s' AND "
                 "(optype = 'sum_write_bytes' OR optype = 'sum_read_bytes') "
                 "AND value > 0 AND time > %ss" %
                 (fsname, start_time))
        response = self.cdqos_influxdb_client.ic_query(log, query)
        if response is None:
            log.cl_error("failed to create continuous query with query "
                         "[%s]", query)
            return -1

        if response.status_code != httplib.OK:
            log.cl_error("got InfluxDB status [%d] when creating "
                         "continuous query with query [%s]",
                         response.status_code, query)
            return -1
        data = response.json()
        results = data["results"]
        result = results[0]
        if "series" not in result:
            log.cl_info("no I/O throughput on file system [%s] since "
                        "epoch time of [%s]", fsname, start_time)
            return 0
        series = result["series"]
        serie = series[0]
        data_values = serie["values"]
        pattern = (r"^(?P<proc_name>\S+)\.(?P<uid>\d+)$")
        regular = re.compile(pattern)
        uid_speeds = {}
        for data_value in data_values:
            timestamp = data_value[0]
            ost_index = data_value[1]
            job_id = data_value[2]
            value = int(data_value[3])
            match = regular.match(job_id)
            if not match:
                continue
            uid = match.group("uid")
            if uid in uid_speeds:
                uid_speeds[uid] += value
            else:
                uid_speeds[uid] = value
            log.cl_debug("timestamp %s, ost_index %s, job_id %s, "
                         "proc_name %s, uid %s, value %s",
                         timestamp, ost_index, job_id,
                         match.group("proc_name"), uid, value)

        for uid, speed in uid_speeds.iteritems():
            throughput = speed * self.cdqos_esmon_collect_interval / 1048576
            if uid in self.cdqos_users:
                qos_user = self.cdqos_users[uid]
                throughput_threshold = qos_user.cdqosu_throughput_threshold
                rpc_limit = qos_user.cdqosu_throttled_oss_rpc_rate
            else:
                throughput_threshold = self.cdqos_throughput_threshold
                rpc_limit = self.cdqos_throttled_oss_rpc_rate

            if throughput > throughput_threshold:
                log.cl_info("uid [%s] has throughput of [%s] MB (> %s MB) since "
                            "epoch time of [%d], enforcing TBF limiation",
                            uid, throughput, throughput_threshold,
                            start_time)
                ret = self.cdqos_enforce_oss_tbf(log, uid, rpc_limit)
                if ret:
                    log.cl_error("failed to enforce limiation on uid [%s]", uid)
                    continue
            else:
                log.cl_info("uid [%s] has throughput of [%s] MB (<= %s MB) since "
                            "epoch time of [%d], no TBF limiation",
                            uid, throughput, throughput_threshold,
                            start_time)
        return 0

    def _cdqos_metadata_check(self, log, fsname, start_time):
        # pylint: disable=too-many-branches,too-many-locals
        """
        Check whether any user exceeds metadata rate threshold
        """
        query = ('SELECT job_id,sum FROM "cqm_mdt_jobstats_samples-fs_name-job_id" '
                 "WHERE fs_name = '%s' AND sum > 0 AND time > %ss" %
                 (fsname, start_time))
        response = self.cdqos_influxdb_client.ic_query(log, query)
        if response is None:
            log.cl_error("failed to create continuous query with query "
                         "[%s]", query)
            return -1

        if response.status_code != httplib.OK:
            log.cl_error("got InfluxDB status [%d] when creating "
                         "continuous query with query [%s]",
                         response.status_code, query)
            return -1
        data = response.json()
        results = data["results"]
        result = results[0]
        if "series" not in result:
            log.cl_info("no metadata operation on file system [%s] since "
                        "epoch time of [%s]", fsname, start_time)
            return 0
        series = result["series"]
        serie = series[0]
        data_values = serie["values"]
        pattern = (r"^(?P<proc_name>\S+)\.(?P<uid>\d+)$")
        regular = re.compile(pattern)
        uid_iops = {}
        for data_value in data_values:
            timestamp = data_value[0]
            job_id = data_value[1]
            value = int(data_value[2])
            match = regular.match(job_id)
            if not match:
                continue
            uid = match.group("uid")
            if uid in uid_iops:
                uid_iops[uid] += value
            else:
                uid_iops[uid] = value
            log.cl_debug("timestamp %s, job_id %s, "
                         "proc_name %s, uid %s, value %s",
                         timestamp, job_id,
                         match.group("proc_name"), uid, value)

        for uid, iops in uid_iops.iteritems():
            metadata_operations = iops * self.cdqos_esmon_collect_interval
            if uid in self.cdqos_users:
                qos_user = self.cdqos_users[uid]
                metadata_threshold = qos_user.cdqosu_metadata_threshold
                rpc_limit = qos_user.cdqosu_throttled_mds_rpc_rate
            else:
                metadata_threshold = self.cdqos_throughput_threshold
                rpc_limit = self.cdqos_throttled_mds_rpc_rate

            if metadata_operations > metadata_threshold:
                log.cl_info("uid [%s] has [%s] metadata operations (> %s "
                            "operations) since epoch time of [%d], iops %s, "
                            "enforcing RPC throttling on all MDS",
                            uid, metadata_operations, metadata_threshold, iops,
                            start_time)
                ret = self.cdqos_enforce_mds_tbf(log, uid, rpc_limit)
                if ret:
                    log.cl_error("failed to enforce limiation on uid [%s]", uid)
                    continue
            else:
                log.cl_info("uid [%s] has [%s] metadata operations (<= %s "
                            "operations) , iops %s, since epoch time of [%d], "
                            "no RPC throttling yet", uid, metadata_operations,
                            metadata_threshold, iops, start_time)
        return 0

    def _cdqos_mds_congestion_check(self, log):
        """
        Check whether MDS is under congestion, if so enforce TBF limit to default rule
        """
        lustrefs = self.cdqos_lustrefs
        fsname = lustrefs.lf_fsname
        if not lustrefs.lf_clients:
            return 0

        retval = 0
        for client_index, client in self.cdqos_clients.iteritems():
            ret = client.cdqc_latency_check(log)
            if ret:
                log.cl_error("failed to check latency on client [%s] of Lustre file "
                             "system [%s]", client_index, fsname)
                retval = ret

        return retval

    def cdqos_thread_main(self):
        """
        Query the performance of service and manage QoS
        """
        # pylint: disable=too-many-locals,too-many-statements,too-many-branches
        fsname = self.cdqos_lustrefs.lf_fsname
        log = self.cdqos_thread_log

        current_interval_index = None
        first = True
        while self.cdqos_enabled:
            if not first:
                time.sleep(1)
            if not self.cdqos_enabled:
                break
            first = False
            time_now = int(time.time())
            interval_index = time_now / self.cdqos_interval
            if interval_index != current_interval_index:
                ret = self.cdqos_clear_limitations(log)
                if ret:
                    log.cl_error("failed to clear limitations, try next time")
                    continue

                current_interval_index = interval_index

            start_time = interval_index * self.cdqos_interval

            self._cdqos_throughput_check(log, fsname, start_time)
            self._cdqos_metadata_check(log, fsname, start_time)
            # self._cdqos_mds_congestion_check(log)
        log.cl_info("quiting QoS thread")
        return 0
