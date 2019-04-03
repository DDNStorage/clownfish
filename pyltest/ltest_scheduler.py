# Copyright (c) 2016 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
The scheduler manages the usage of test hosts. All test launchers
need to allocate hosts from the scheduler.
"""
# pylint: disable=too-many-lines
import SimpleXMLRPCServer
import threading
import signal
import time
import os
import re
import traceback
import socket
import yaml

# Local libs
from pylcommon import utils
from pylcommon import time_util
from pylcommon import cstr
from pylcommon import cmd_general
from pylcommon import lvirt
from pylcommon import ssh_host

TEST_SCHEDULER_PORT = 1234
TEST_SCHEDULER_LOG_DIR = "/var/log/ltest_scheduler"
TEST_SCHEDULER_CONFIG = "/etc/ltest_scheduler.conf"


PURPOSE_BUILD = "build"
PURPOSE_TEST = "test"

RESOURCE_TYPE_HOST = "host"
RESOURCE_TYPE_IP_ADDRESS = "ip_address"

GLOBAL_LOG = None
SHUTTING_DOWN = False
MIN_GOOD_RES_CHECK_INTERVAL = 7200
MIN_BAD_RES_CHECK_INTERVAL = 3600
# Need to wait at least this long time before assert that the IP is not
# used by any host
IP_MAX_FAILOVER_TIME = 60
# The interval to check whether a IP is being used or not
IP_CHECK_INTERVAL = 3
# Need to check at least this times before assert that the IP is not
# used by any host
IP_MIN_CHECK_TIMES = 5

# The heatbeat interval
TEST_HEARTBEAT_INTERVAL = 10
# The heatbeat timeout. Both scheduler and client will abort the job if
# heatbeat is not recived/sent correctly for this long time.
TEST_HEARTBEAT_TIMEOUT = 20


class ScheduledResource(object):
    """
    Each resource has this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # This return value signs that the resource is being checked
    RESOURCE_IS_BUSY = 1

    def __init__(self, name, resource_type, concurrency):
        self.sr_is_clean = False
        # The time when cleaning up the resource
        self.sr_check_time = 0
        self.sr_max_concurrency = concurrency
        self.sr_concurrency = 0
        self.sr_job_sequence = None
        self.rr_resource_type = resource_type
        self.sr_error = 0
        self.sr_name = name
        self.sr_cleaning = False

    def sr_dirty(self):
        """
        Dirty the resource so as to check later
        """
        self.sr_check_time = 0
        self.sr_is_clean = False


class RPCResouce(object):
    """
    The resource for transfering between scheduler and its clients
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, resource_type):
        self.rr_resource_type = resource_type


class RPCIPAddress(RPCResouce):
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    The IP address to manage in this scheduler
    """
    def __init__(self, address, bindnetaddr):
        super(RPCIPAddress, self).__init__(RESOURCE_TYPE_IP_ADDRESS)
        self.ripa_address = address
        self.ripa_bindnetaddr = bindnetaddr


class RPCHost(RPCResouce):
    """
    The host for transfering between scheduler and its clients
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, hostname, kvm_server_hostname=None,
                 expected_distro=None, ipv4_addresses=None,
                 kvm_template_ipv4_address=None, kvm_template=None):
        # pylint: disable=too-many-arguments
        super(RPCHost, self).__init__(RESOURCE_TYPE_HOST)
        self.lrh_hostname = hostname
        self.lrh_kvm_server_hostname = kvm_server_hostname
        self.lrh_expected_distro = expected_distro
        self.lrh_ipv4_addresses = ipv4_addresses
        self.lrh_kvm_template_ipv4_address = kvm_template_ipv4_address
        # Only server side kvm_template uses to send info to client
        self.lrh_kvm_template = kvm_template


class TestHost(ScheduledResource):
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    The host that is managed by scheduler
    """
    def __init__(self, hostname, distro, purpose, tag, concurrency,
                 ipv4_addresses=None, kvm_server_hostname=None,
                 kvm_template_ipv4_address=None,
                 kvm_template=None):
        # pylint: disable=too-many-arguments
        super(TestHost, self).__init__(hostname, RESOURCE_TYPE_HOST, concurrency)
        self.th_hostname = hostname
        self.th_purpose = purpose
        self.th_distro = distro
        self.th_tag = tag
        self.th_kvm_server_hostname = kvm_server_hostname
        self.th_kvm_template_ipv4_address = kvm_template_ipv4_address
        self.th_ipv4_addresses = ipv4_addresses
        self.th_kvm_template = kvm_template
        self.th_host = ssh_host.SSHHost(hostname)

    def th_print_info(self, log):
        """
        Print the info of this host
        """
        log.cl_debug("added host [%s], purpose [%s], distro [%s], tag [%s], "
                     "kvm server [%s]",
                     self.th_hostname,
                     self.th_purpose,
                     self.th_distro,
                     self.th_tag,
                     self.th_kvm_server_hostname)

    def _sr_cleanup(self, log, scheduler):
        """
        Clean up the host

        Improvement: call shared functions in lvirt directly
        Improvement: cleanup directories for spaces
        """
        # pylint: disable=unused-argument
        if self.th_purpose == PURPOSE_BUILD:
            return 0

        host = self.th_host
        service_names = ["corosync", "pacemaker"]
        for service_name in service_names:
            ret = host.sh_service_stop(log, service_name)
            if ret:
                log.cl_error("failed to stop service [%s] on host [%s]",
                             service_name, host.sh_hostname)
                return -1

            ret = host.sh_service_disable(log, service_name)
            if ret:
                log.cl_error("failed to disable service [%s] on host [%s]",
                             service_name, host.sh_hostname)
                return -1

        return 0

    def sr_cleanup(self, log, scheduler):
        """
        Clean up the host

        Improvement: call shared functions in lvirt directly
        Improvement: cleanup directories for spaces
        """
        log.cl_info("cleaning up host [%s]", self.th_hostname)
        self.sr_cleaning = True
        ret = self._sr_cleanup(log, scheduler)
        self.sr_cleaning = False
        if ret:
            log.cl_info("failed to clean up host [%s]", self.th_hostname)
        else:
            log.cl_info("cleaned up host [%s]", self.th_hostname)
        return ret


def _wait_disconnected(log, host):
    """
    Check whether host is disconnected from this host
    """
    ret = host.sh_ping(log, slient=True)
    if ret:
        return 0
    log.cl_info("still able to connect to host [%s] from local host",
                host.sh_hostname)
    return -1


def wait_disconnected(log, host, timeout=10, sleep_interval=1):
    """
    Wait until the host can not be connected from this host
    """
    return utils.wait_condition(log, _wait_disconnected,
                                (host, ),
                                timeout=timeout,
                                sleep_interval=sleep_interval)


class IPAddress(ScheduledResource):
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    The IP address to manage in this scheduler
    """
    def __init__(self, address, bindnetaddr):
        super(IPAddress, self).__init__(address, RESOURCE_TYPE_IP_ADDRESS, 1)
        self.ipa_address = address
        self.ipa_bindnetaddr = bindnetaddr
        self.ipa_host = ssh_host.SSHHost(address)

    def _sr_cleanup(self, log, scheduler):
        """
        Cleanup the IP adress by stopping the corosync/pacemaker on any
        host that is using the IP
        """
        ip_host = self.ipa_host
        idle_time = None
        checked_times = 0

        while True:
            now_time = time.time()
            ret = ip_host.sh_ping(log, silent=True)
            if ret:
                log.cl_debug("can not ping IP [%s]", self.ipa_address)
                if idle_time is None:
                    idle_time = now_time
                    checked_times = 0
                checked_times += 1
                # The IP has not been used for a long time, and enough times
                # have been chcked, so clean to use
                if (idle_time + IP_MAX_FAILOVER_TIME < now_time and
                        checked_times > IP_MIN_CHECK_TIMES):
                    return 0
                # Not long enough to decide, sleep a while and check later
                time.sleep(IP_CHECK_INTERVAL)
                continue
            else:
                log.cl_debug("can ping IP [%s]", self.ipa_address)
                idle_time = None
                checked_times = 0

            command = "hostname"
            retval = ip_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_info("failed to run command [%s] on host [%s], "
                            "ret = [%d], stdout = [%s], stderr = [%s]",
                            command,
                            ip_host.sh_hostname,
                            retval.cr_exit_status,
                            retval.cr_stdout,
                            retval.cr_stderr)
                log.cl_info("maybe the host with IP [%s] has been cleaned up, "
                            "will check in the next loop", self.ipa_address)
                continue

            lines = retval.cr_stdout.splitlines()
            if len(lines) != 1:
                log.cl_error("unexpected output of command [%s] on host [%s]: "
                             "[%s]", command, ip_host.sh_hostname,
                             retval.cr_stdout)
                return -1
            hostname = lines[0]

            res = scheduler.ts_find_host(hostname)
            if res is None:
                log.cl_error("host [%s] is not managed by the scheduler but "
                             "is using IP [%s]", hostname, self.ipa_address)
                return -1

            # The host is being cleaned, so the IP might be released soon.
            # Check that in the next loop.
            if res.sr_cleaning:
                log.cl_info("host [%s] is being cleaned, will check IP [%s] "
                            "in next loop", hostname, self.ipa_address)
                continue

            ret = scheduler.ts_resource_cleanup(res)
            if ret == ScheduledResource.RESOURCE_IS_BUSY:
                log.cl_info("host [%s] is busy, checking in next "
                            "loop", hostname, self.ipa_address)
                continue
            elif ret:
                log.cl_error("failed to cleanup host [%s]",
                             hostname)
                return -1

            ret = wait_disconnected(log, ip_host)
            if ret:
                log.cl_error("still be able to connect to [%s] after fixing "
                             "host [%s]", ip_host.sh_hostname, hostname)
                return -1
        return 0

    def sr_cleanup(self, log, scheduler):
        """
        Cleanup the IP adress by stopping the corosync/pacemaker on any
        host that is using the IP
        """
        log.cl_info("cleaning up IP address [%s]", self.ipa_address)
        self.sr_cleaning = True
        ret = self._sr_cleanup(log, scheduler)
        self.sr_cleaning = False
        if ret:
            log.cl_info("failed to clean up IP address [%s]", self.ipa_address)
        else:
            log.cl_info("cleaned up IP address [%s]", self.ipa_address)
        return ret


class ResourceDescriptor(object):
    # pylint: disable=too-few-public-methods
    """
    Used when trying to allocate a resource
    """
    def __init__(self, resource_type, number_min=1, number_max=1,
                 resources=None):
        self.rd_type = resource_type
        self.rd_number_min = number_min
        self.rd_number_max = number_max
        if resources is None:
            self.rd_resources = []
        else:
            self.rd_resources = list(resources)


class ResourceDescriptorIPAddress(ResourceDescriptor):
    # pylint: disable=too-few-public-methods
    """
    Used when trying to allocate a host
    """
    def __init__(self, number_min=1, number_max=1, rpc_addresses=None):
        super(ResourceDescriptorIPAddress, self).__init__(RESOURCE_TYPE_IP_ADDRESS,
                                                          number_min=number_min,
                                                          number_max=number_max,
                                                          resources=rpc_addresses)


class ResourceDescriptorHost(ResourceDescriptor):
    # pylint: disable=too-few-public-methods
    """
    Used when trying to allocate a host
    """
    def __init__(self, purpose, distro=ssh_host.DISTRO_RHEL7,
                 same_kvm_server=False, tag=None, number_min=1, number_max=1,
                 hosts=None):
        # pylint: disable=too-many-arguments
        super(ResourceDescriptorHost, self).__init__(RESOURCE_TYPE_HOST,
                                                     number_min=number_min,
                                                     number_max=number_max,
                                                     resources=hosts)
        self.rdh_distro = distro
        self.rdh_purpose = purpose
        self.rdh_same_kvm_server = same_kvm_server
        self.rdh_tag = tag


def resource_compare(host_x, host_y):
    """
    Sort the resource according to cleanup time
    """
    return host_x.sr_check_time > host_y.sr_check_time


class TestSchedulerJob(object):
    """
    Each test client allocates a job in the scheduler. Hosts could be
    allocated into the job afterwards.
    """
    def __init__(self, scheduler, jobid, sequence):
        self.laj_jobid = jobid
        self.laj_hosts = []
        self.laj_scheduler = scheduler
        self.laj_sequence = sequence
        self.laj_check_time = time_util.utcnow()
        self.laj_ip_addresses = []

    def laj_host_add(self, lhost):
        """
        Add one host into the job
        """
        self.laj_hosts.append(lhost)

    def laj_has_host(self, lhost):
        """
        Check whether a host is in this job
        """
        return lhost in self.laj_hosts

    def laj_host_remove(self, lhost):
        """
        Remove one host from the job
        """
        self.laj_hosts.remove(lhost)

    def laj_has_ip_address(self, ip_address):
        """
        Check whether a ip address is in this job
        """
        return ip_address in self.laj_ip_addresses

    def laj_ip_address_add(self, ip_address):
        """
        Add one host into the job
        """
        self.laj_ip_addresses.append(ip_address)

    def laj_ip_address_remove(self, ip_address):
        """
        Remove one host from the job
        """
        self.laj_ip_addresses.remove(ip_address)


def rpc2descriptors(log, rpc_descriptors, same_kvm_host_descriptors,
                    other_descriptors):
    """
    Parse the descriptors from RPC to objects
    """
    # pylint: disable=too-many-locals
    for descriptor in rpc_descriptors:
        descriptor_type = descriptor["rd_type"]
        number_min = descriptor["rd_number_min"]
        number_max = descriptor["rd_number_max"]
        resources = descriptor["rd_resources"]
        if descriptor_type == RESOURCE_TYPE_HOST:
            distro = descriptor["rdh_distro"]
            purpose = descriptor["rdh_purpose"]
            same_kvm_server = descriptor["rdh_same_kvm_server"]
            tag = descriptor["rdh_tag"]
            hosts = []
            for res in resources:
                hostname = res["lrh_hostname"]
                kvm_server_hostname = res["lrh_kvm_server_hostname"]
                expected_distro = res["lrh_expected_distro"]
                ipv4_addresses = res["lrh_ipv4_addresses"]
                kvm_template_ipv4_address = res["lrh_kvm_template_ipv4_address"]
                kvm_template = res["lrh_kvm_template"]
                host = RPCHost(hostname, kvm_server_hostname=kvm_server_hostname,
                               expected_distro=expected_distro,
                               ipv4_addresses=ipv4_addresses,
                               kvm_template_ipv4_address=kvm_template_ipv4_address,
                               kvm_template=kvm_template)
                hosts.append(host)
            host_desc = ResourceDescriptorHost(purpose, distro=distro,
                                               same_kvm_server=same_kvm_server,
                                               tag=tag,
                                               number_min=number_min,
                                               number_max=number_max,
                                               hosts=hosts)
            if same_kvm_server:
                same_kvm_host_descriptors.append(host_desc)
            else:
                other_descriptors.append(host_desc)
        elif descriptor_type == RESOURCE_TYPE_IP_ADDRESS:
            rpc_addresses = []
            for res in resources:
                address = res["ripa_address"]
                bindnetaddr = res["ripa_bindnetaddr"]
                rpc_address = RPCIPAddress(address, bindnetaddr)
                rpc_addresses.append(rpc_address)
            ip_desc = ResourceDescriptorIPAddress(number_min=number_min,
                                                  number_max=number_max,
                                                  rpc_addresses=rpc_addresses)
            other_descriptors.append(ip_desc)
        else:
            log.cl_error("wrong descriptor type [%s]", descriptor_type)
            return -1
    return 0


class TestScheduler(object):
    """
    The main object of the scheduler.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, log, scheduler_id, hosts, addresses):
        self.ts_log = log
        self.ts_resources = hosts + addresses
        self.ts_hosts = []
        self.ts_addresses = addresses
        self.ts_job_dict = {}
        self.ts_condition = threading.Condition()
        self.ts_jobid_sequence = 0
        self.ts_id = scheduler_id
        self.ts_id += ("_%d" % os.getpid())
        log.cl_info("ID of scheduler: [%s]", self.ts_id)
        self.ts_kvm_hosts_dict = {}
        for host in hosts:
            self._ts_add_host(host)

    def _ts_add_host(self, host):
        """
        Add host into the list
        """
        log = self.ts_log
        host.th_print_info(log)
        self.ts_hosts.append(host)

        kvm_server_hostname = host.th_kvm_server_hostname

        if kvm_server_hostname is not None:
            if kvm_server_hostname not in self.ts_kvm_hosts_dict:
                self.ts_kvm_hosts_dict[kvm_server_hostname] = []
            kvm_hosts = self.ts_kvm_hosts_dict[kvm_server_hostname]
            kvm_hosts.append(host)

    def ts_find_ip_address(self, ip_address):
        """
        Find the IP address by its hostname. Lock should be acquired in advance.
        """
        for ip_address_obj in self.ts_addresses:
            if ip_address_obj.ipa_address == ip_address:
                return ip_address_obj
        return None

    def ts_find_host(self, hostname):
        """
        Find the host by its hostname. Lock should be acquired in advance.
        """
        for lhost in self.ts_hosts:
            if lhost.th_hostname == hostname:
                return lhost
        return None

    def ts_get_id(self):
        """
        Return the scheduler ID. Scheduler ID is the ID of this scheduler. It
        could prevent clients from operating on a wrong scheduler. Usually
        called remotely by client.
        """
        return self.ts_id

    def ts_host_list(self, error):
        """
        List the hosts that the scheduler is managing. Usually called remotely
        by console.
        """
        log = self.ts_log
        log.cl_debug("listing host")
        format_string = "%-20s%-9s%-8s%-10s%-10s%-9s%-7s%-12s%-11s\n"
        output = format_string % ("Host", "Purpose", "Distro", "KVM host",
                                  "Job slot", "Job seq", "Error",
                                  "Clean time", "Next clean")
        output += '{0:->80}'.format("") + "\n"
        now = time.time()
        for lhost in self.ts_hosts:
            if error:
                if lhost.sr_concurrency > 0:
                    continue
                if lhost.sr_is_clean:
                    continue
                if lhost.sr_max_concurrency > 1:
                    continue
            if lhost.sr_check_time == 0:
                fix_string = "not clean"
                if lhost.sr_cleaning:
                    next_check_string = "cleaning"
                elif lhost.sr_concurrency > 0:
                    next_check_string = "occupied"
                else:
                    next_check_string = "initing"
            else:
                if not lhost.sr_is_clean:
                    fix_string = "not clean"
                    next_check = lhost.sr_check_time + MIN_BAD_RES_CHECK_INTERVAL
                else:
                    fix_time = time.gmtime(lhost.sr_check_time)
                    fix_string = time.strftime("%H:%M:%S", fix_time)
                    next_check = lhost.sr_check_time + MIN_GOOD_RES_CHECK_INTERVAL
                next_check_time = time.gmtime(next_check)
                next_check_string = time.strftime("%H:%M:%S", next_check_time)
                next_check_string += "(%d)" % (int(next_check - now))
            job_slot = ("%d/%d" % (lhost.sr_concurrency,
                                   lhost.sr_max_concurrency))
            output += (format_string %
                       (lhost.th_hostname,
                        lhost.th_purpose,
                        lhost.th_distro,
                        lhost.th_kvm_server_hostname,
                        job_slot,
                        lhost.sr_job_sequence,
                        lhost.sr_error,
                        fix_string,
                        next_check_string))
        return output

    def ts_ip_address_list(self, error):
        """
        List the ip_address that the scheduler is managing. Usually called remotely
        by console.
        """
        log = self.ts_log
        log.cl_debug("listing IP addresses")
        format_string = "%-17s%-17s%-10s%-9s%-7s%-12s%-11s\n"
        output = format_string % ("IP", "Bindnetaddr",
                                  "Job slot", "Job seq", "Error",
                                  "Clean time", "Next clean")
        output += '{0:->80}'.format("") + "\n"
        now = time.time()

        for address in self.ts_addresses:
            if error:
                if address.sr_concurrency > 0:
                    continue
                if address.sr_is_clean:
                    continue
                if address.sr_max_concurrency > 1:
                    continue
            if address.sr_check_time == 0:
                fix_string = "not clean"
                if address.sr_cleaning:
                    next_check_string = "cleaning"
                elif address.sr_concurrency > 0:
                    next_check_string = "occupied"
                else:
                    next_check_string = "initing"
            else:
                if not address.sr_is_clean:
                    fix_string = "not clean"
                    next_check = address.sr_check_time + MIN_BAD_RES_CHECK_INTERVAL
                else:
                    fix_time = time.gmtime(address.sr_check_time)
                    fix_string = time.strftime("%H:%M:%S", fix_time)
                    next_check = address.sr_check_time + MIN_GOOD_RES_CHECK_INTERVAL
                next_check_time = time.gmtime(next_check)
                next_check_string = time.strftime("%H:%M:%S", next_check_time)
                next_check_string += "(%d)" % (int(next_check - now))
            job_slot = ("%d/%d" % (address.sr_concurrency,
                                   address.sr_max_concurrency))
            output += (format_string %
                       (address.ipa_address,
                        address.ipa_bindnetaddr,
                        job_slot,
                        address.sr_job_sequence,
                        address.sr_error,
                        fix_string,
                        next_check_string))
        return output

    def _ts_host_list_allocate(self, host_list, job, distro, number_min,
                               number_max, purpose, tag):
        """
        Allocate hosts from a list, if failed, return []
        """
        # pylint: disable=too-many-arguments,no-self-use
        log = self.ts_log
        log.cl_debug("listing IP addresses")
        rpc_hosts = []
        hosts = []
        # reverse the host from time to time, so that the host in the tail of
        # the list have equal chance to be allocated
        host_list.reverse()

        # Check the potential hosts that can be allocated
        for host in host_list:
            if host.th_distro != distro:
                log.cl_debug("the distro [%s] of host [%s] != [%s]",
                             host.th_distro, host.th_hostname, distro)
                continue
            if host.th_purpose != purpose:
                log.cl_debug("the purpose [%s] of host [%s] != [%s]",
                             host.th_purpose, host.th_hostname, purpose)
                continue
            if tag is not None and host.th_tag != tag:
                log.cl_debug("the tag [%s] of host [%s] != [%s]",
                             host.th_tag, host.th_hostname, tag)
                continue
            if host.sr_max_concurrency <= host.sr_concurrency:
                log.cl_debug("no currency for host [%s]", host.th_hostname)
                continue
            hosts.append(host)

        # Not enough hosts, abort
        if len(hosts) < number_min:
            log.cl_error("not enough hosts to allocate, needs [%d], have [%d]",
                         number_min, len(hosts))
            return rpc_hosts

        hosts.sort(resource_compare)
        # Allocate the hosts
        for host in hosts:
            if len(rpc_hosts) >= number_max:
                break
            host.sr_concurrency += 1
            host.sr_job_sequence = job.laj_sequence
            job.laj_host_add(host)
            rpc_host = RPCHost(host.th_hostname,
                               kvm_server_hostname=host.th_kvm_server_hostname,
                               expected_distro=host.th_distro,
                               ipv4_addresses=host.th_ipv4_addresses,
                               kvm_template=host.th_kvm_template)
            rpc_hosts.append(rpc_host)
            log.cl_debug("preallocated host [%s] for job [%s]",
                         host.th_hostname, job.laj_jobid)
        return rpc_hosts

    def _ts_job_allocate_ip_resource_holding_lock(self, job, desc):
        """
        Allocated one resource for host, if fails, returen -1
        """
        log = self.ts_log
        log.cl_debug("allocating a IP resource for job [%s]", job.laj_jobid)
        rpc_addresses = []
        # reverse the IPs from time to time, so that the host in the tail of
        # the list have equal chance to be allocated
        self.ts_addresses.reverse()

        addresses = []
        # Check the potential addresses that can be allocated
        for address in self.ts_addresses:
            if address.sr_max_concurrency <= address.sr_concurrency:
                continue
            # Can not allocate an IP that might being used
            if not address.sr_is_clean:
                continue
            addresses.append(address)

        # Not enough hosts, abort
        if len(addresses) < desc.rd_number_min:
            log.cl_info("not enough IP to allocate, needs [%d], have [%d]",
                        desc.rd_number_min, len(addresses))
            return -1

        addresses.sort(resource_compare)
        # Allocate the address
        for address in addresses:
            if len(rpc_addresses) >= desc.rd_number_max:
                break
            address.sr_concurrency += 1
            address.sr_job_sequence = job.laj_sequence
            job.laj_ip_address_add(address)
            rpc_address = RPCIPAddress(address.ipa_address,
                                       address.ipa_bindnetaddr)
            rpc_addresses.append(rpc_address)
        desc.rd_resources = rpc_addresses
        return 0

    def _ts_job_allocate_host_resource_holding_lock(self, job, desc):
        """
        Allocated one resource for host, if fails, returen -1
        """
        log = self.ts_log
        if not desc.rdh_same_kvm_server:
            log.cl_debug("allocating a host resource that doesn't need to "
                         "share KVM server for job [%s]", job.laj_jobid)
            rpc_hosts = self._ts_host_list_allocate(self.ts_hosts, job,
                                                    desc.rdh_distro,
                                                    desc.rd_number_min,
                                                    desc.rd_number_max,
                                                    desc.rdh_purpose,
                                                    desc.rdh_tag)
        else:
            log.cl_debug("allocating a hosts resource that shares KVM server "
                         "for job [%s]", job.laj_jobid)
            for host_list in self.ts_kvm_hosts_dict.values():
                rpc_hosts = self._ts_host_list_allocate(host_list, job,
                                                        desc.rdh_distro,
                                                        desc.rd_number_min,
                                                        desc.rd_number_max,
                                                        desc.rdh_purpose,
                                                        desc.rdh_tag)
                if len(rpc_hosts) != 0:
                    break
        if len(rpc_hosts) == 0:
            return -1
        desc.rd_resources = rpc_hosts
        return 0

    def _ts_job_allocate_resource_holding_lock(self, job, desc):
        """
        Allocated one resource, if fails, returen -1
        """
        log = self.ts_log
        log.cl_debug("allocating a resource for job [%s]", job.laj_jobid)
        if desc.rd_type == RESOURCE_TYPE_HOST:
            return self._ts_job_allocate_host_resource_holding_lock(job, desc)
        elif desc.rd_type == RESOURCE_TYPE_IP_ADDRESS:
            return self._ts_job_allocate_ip_resource_holding_lock(job, desc)
        else:
            log.cl_error("wrong resource type [%s]", desc.rd_type)
            return -1

    def _ts_job_allocate_resources_holding_lock(self, job, descs):
        """
        Allocate multiple resources holding lock, if any of them fails, -1
        """
        log = self.ts_log
        log.cl_debug("allocating resources for job [%s]", job.laj_jobid)
        for desc in descs:
            ret = self._ts_job_allocate_resource_holding_lock(job, desc)
            if ret:
                log.cl_debug("failed to allocate resource, releasing "
                             "allocated resource of job [%s]", job.laj_jobid)
                self._ts_job_release_resources_holding_lock(job, descs)
                return ret
        return 0

    def _ts_job_release_one_host_holding_lock(self, job, res):
        """
        Release a host resources
        """
        log = self.ts_log
        log.cl_debug("releasing host [%s] for job [%s]", res.lrh_hostname,
                     job.laj_jobid)
        test_host = self.ts_find_host(res.lrh_hostname)
        if test_host is None:
            log.cl_error("failed to release host [%s], not exists in "
                         "the scheduler", res.lrh_hostname)
            return -1

        if not job.laj_has_host(test_host):
            log.cl_error("failed to release host [%s], not used by the "
                         "job [%s]", res.lrh_hostname, job.laj_jobid)
            return -1

        job.laj_host_remove(test_host)
        test_host.sr_job_sequence = None
        test_host.sr_concurrency -= 1
        return 0

    def _ts_job_release_one_ip_holding_lock(self, job, res):
        """
        Release a host resources
        """
        log = self.ts_log
        log.cl_debug("releasing a IP resource for job [%s]", job.laj_jobid)
        ip_address_obj = self.ts_find_ip_address(res.ripa_address)
        if ip_address_obj is None:
            log.cl_error("failed to release IP address [%s], not exists in "
                         "the scheduler", res.ripa_address)
            return -1

        if not job.laj_has_ip_address(ip_address_obj):
            log.cl_error("failed to release IP address [%s], not used by the "
                         "job [%s]", res.ripa_address, job.laj_jobid)
            return -1

        job.laj_ip_address_remove(ip_address_obj)
        ip_address_obj.sr_job_sequence = None
        ip_address_obj.sr_concurrency -= 1
        return 0

    def _ts_job_release_one_holding_lock(self, job, res):
        """
        Release one resource
        """
        log = self.ts_log
        if res.rr_resource_type == RESOURCE_TYPE_IP_ADDRESS:
            return self._ts_job_release_one_ip_holding_lock(job, res)
        elif res.rr_resource_type == RESOURCE_TYPE_HOST:
            return self._ts_job_release_one_host_holding_lock(job, res)
        else:
            log.cl_error("wrong resource type [%s]", res.rr_resource_type)
            return -1

    def _ts_job_release_resource_holding_lock(self, job, desc):
        """
        Release a resource
        """
        log = self.ts_log
        log.cl_debug("releasing a resource for job [%s]", job.laj_jobid)
        retval = 0
        for res in desc.rd_resources[:]:
            ret = self._ts_job_release_one_holding_lock(job, res)
            if ret:
                log.cl_error("failed to release one resource")
                retval = ret
            else:
                desc.rd_resources.remove(res)

        return retval

    def _ts_job_release_resources_holding_lock(self, job, descs):
        """
        Release a lot resources
        """
        log = self.ts_log
        log.cl_debug("releasing resources for job [%s]", job.laj_jobid)
        retval = 0
        for desc in descs:
            ret = self._ts_job_release_resource_holding_lock(job, desc)
            if ret:
                retval = ret
        return retval

    def _ts_resources_dirty_holding_lock(self, log, jobid, descs):
        """
        Dirty the resources in the descriptors
        Only call this when about to return the resources to client
        """
        # pylint: disable=no-self-use
        host_names = []
        ip_addresses = []
        for desc in descs:
            for rpc_res in desc.rd_resources:
                if desc.rd_type == RESOURCE_TYPE_HOST:
                    res = self.ts_find_host(rpc_res.lrh_hostname)
                    host_names.append(rpc_res.lrh_hostname)
                elif desc.rd_type == RESOURCE_TYPE_IP_ADDRESS:
                    res = self.ts_find_ip_address(rpc_res.ripa_address)
                    ip_addresses.append(rpc_res.ripa_address)
                else:
                    log.cl_error("invalid resource type [%s]", desc.rd_type)
                    return -1
                res.sr_dirty()

        log.cl_error("allocated hosts %s and IPs %s for job [%s]",
                     host_names, ip_addresses, jobid)
        return 0

    def ts_resources_allocate(self, scheduler_id, jobid, descriptors):
        """
        Allocate multiple resources, if any of them fails, return []
        """
        log = self.ts_log
        log.cl_debug("allocating resources for job [%s]", jobid)
        if scheduler_id != self.ts_id:
            log.cl_error("wrong scheduler ID [%s], expected [%s]",
                         scheduler_id, self.ts_id)
            return []

        same_kvm_host_descriptors = []
        other_descriptors = []
        ret = rpc2descriptors(log, descriptors, same_kvm_host_descriptors,
                              other_descriptors)
        if ret:
            log.cl_error("failed to parse resource descriptors from RPC")
            return []
        ret_descriptors = same_kvm_host_descriptors + other_descriptors

        self.ts_condition.acquire()
        if jobid not in self.ts_job_dict:
            log.cl_error("resource allocation from unknown job [%s]", jobid)
            self.ts_condition.release()
            return []
        job = self.ts_job_dict[jobid]
        ret = self._ts_job_allocate_resources_holding_lock(job, same_kvm_host_descriptors)
        if ret == 0:
            ret = self._ts_job_allocate_resources_holding_lock(job, other_descriptors)
            if ret:
                log.cl_debug("failed to allocated resources for job [%s]", jobid)
        else:
            log.cl_error("failed to allocated host resources that share the "
                         "same KVM server for job [%s]", jobid)
            ret = -1

        if ret:
            log.cl_debug("releasing allocated resource of job [%s]", jobid)
            self._ts_job_release_resources_holding_lock(job, ret_descriptors)
            ret_descriptors = []
        else:
            ret = self._ts_resources_dirty_holding_lock(log, jobid,
                                                        ret_descriptors)
            if ret:
                self._ts_job_release_resources_holding_lock(job, ret_descriptors)
                ret_descriptors = []

        job.laj_check_time = time_util.utcnow()
        self.ts_condition.release()
        return ret_descriptors

    def _ts_print_release_message(self, log, jobid, descs):
        """
        Print the release message
        """
        # pylint: disable=no-self-use
        host_names = []
        ip_addresses = []
        for desc in descs:
            for rpc_res in desc.rd_resources:
                if desc.rd_type == RESOURCE_TYPE_HOST:
                    host_names.append(rpc_res.lrh_hostname)
                elif desc.rd_type == RESOURCE_TYPE_IP_ADDRESS:
                    ip_addresses.append(rpc_res.ripa_address)
                else:
                    log.cl_error("invalid resource type [%s]", desc.rd_type)
                    return -1

        log.cl_info("releasing hosts %s and IPs %s for job [%s]",
                    host_names, ip_addresses, jobid)
        return 0

    def ts_resources_release(self, scheduler_id, jobid, descriptors):
        """
        Release multiple resources
        """
        log = self.ts_log
        if scheduler_id != self.ts_id:
            log.cl_error("wrong scheduler ID [%s], expected [%s]",
                         scheduler_id, self.ts_id)
            return -1

        same_kvm_host_descriptors = []
        other_descriptors = []
        ret = rpc2descriptors(log, descriptors, same_kvm_host_descriptors,
                              other_descriptors)
        if ret:
            log.cl_error("failed to parse resource descriptors from RPC")
            return -1

        descs = same_kvm_host_descriptors + other_descriptors
        self._ts_print_release_message(log, jobid, descs)

        self.ts_condition.acquire()
        if jobid not in self.ts_job_dict:
            log.cl_error("resource releasing from unknown job [%s]", jobid)
            self.ts_condition.release()
            return -1
        job = self.ts_job_dict[jobid]
        ret = self._ts_job_release_resources_holding_lock(job, descs)
        job.laj_check_time = time_util.utcnow()
        self.ts_condition.notifyAll()
        self.ts_condition.release()
        return ret

    def ts_ip_cleanup(self, ip_address):
        """
        fix a host
        """
        log = self.ts_log
        log.cl_info("cleaning up IP address [%s]", ip_address)
        res = self.ts_find_ip_address(ip_address)
        if res is None:
            log.cl_error("failed to cleanup IP address [%s], not exists in "
                         "the scheduler", ip_address)
            return -1

        ret = self.ts_resource_cleanup(res)
        if ret:
            log.cl_error("failure during fix process of IP [%s]",
                         ip_address)
            return -1

        log.cl_info("cleaned up IP [%s]", ip_address)
        return 0

    def ts_host_cleanup(self, hostname):
        """
        fix a host
        """
        log = self.ts_log
        log.cl_info("cleaning up host [%s]", hostname)
        res = self.ts_find_host(hostname)
        if res is None:
            log.cl_error("failed to cleanup host [%s], not exists in "
                         "the scheduler", hostname)
            return -1

        ret = self.ts_resource_cleanup(res)
        if ret:
            log.cl_error("failure during fix process of host [%s]",
                         hostname)
            return -1

        log.cl_info("cleaned up host [%s]", hostname)
        return 0

    def ts_job_start(self, scheduler_id):
        """
        Start a jobs in the scheduler. Usually called remotely by client.
        """
        log = self.ts_log
        if scheduler_id != self.ts_id:
            return -1
        self.ts_condition.acquire()
        sequence = self.ts_jobid_sequence
        jobid = time_util.local_strftime(time_util.utcnow(), "%Y-%m-%d-%H_%M_%S")
        jobid += ("-%d" % sequence)
        self.ts_jobid_sequence += 1
        log.cl_info("starting an new job [%s]", jobid)

        job = TestSchedulerJob(self, jobid, sequence)
        self.ts_job_dict[jobid] = job
        self.ts_condition.release()
        return jobid

    def ts_job_list(self):
        """
        List all active jobs in the scheduler. Usually called remotely by
        console.
        """
        log = self.ts_log
        log.cl_info("listing job")
        format_string = "%-25s%-6s%-10s\n"
        job_names = format_string % ("Name", "Hosts", "Heartbeat")
        job_names += "{0:->30}".format("") + "\n"

        now = time_util.utcnow()
        self.ts_condition.acquire()
        for job in self.ts_job_dict.values():
            diff = (now - job.laj_check_time).seconds
            diff_string = str(diff)
            if diff > TEST_HEARTBEAT_TIMEOUT:
                diff_string += "*"
            job_names += (format_string %
                          (job.laj_jobid, str(len(job.laj_hosts)),
                           diff_string))
        self.ts_condition.release()
        return job_names

    def _ts_resource_cleanup_holding_concurrency(self, res):
        """
        Check and fix a res
        This function assumes the concurrency of the res has already been held
        """
        # pylint: disable=bare-except
        log = self.ts_log
        # skip the heathy node
        log.cl_debug("checking resource [%s]", res.sr_name)
        try:
            ret = res.sr_cleanup(log, self)
        except:
            ret = -1
            log.cl_error("exception when cleaning up resource [%s]: [%s]",
                         res.sr_name, traceback.format_exc())
        if ret:
            res.sr_error += 1
            res.sr_is_clean = False
            ret = -1
        else:
            res.sr_is_clean = True
            ret = 0
        res.sr_check_time = time.time()

        self.ts_condition.acquire()
        res.sr_concurrency = 0
        self.ts_condition.release()
        return ret

    def ts_resource_cleanup(self, res):
        """
        Check and fix a res
        """
        # pylint: disable=bare-except
        log = self.ts_log
        self.ts_condition.acquire()
        # If the node is being used by some job, skip it. Job reclaim
        # routine will release the dead nodes..
        if res.sr_concurrency > 0:
            self.ts_condition.release()
            log.cl_info("res [%s] is busy, skipping", res.sr_name)
            return ScheduledResource.RESOURCE_IS_BUSY
        # Set concurrency to max so no other one can use it.
        res.sr_concurrency = res.sr_max_concurrency
        self.ts_condition.release()

        return self._ts_resource_cleanup_holding_concurrency(res)

    def ts_recovery_main(self):
        """
        Checking the health of each nodes, repaire them if necessary.
        """
        # pylint: disable=bare-except
        self.ts_condition.acquire()
        log = self.ts_log
        while True:
            log.cl_debug("recovery thread is checking resources")
            fix_res = None
            res_fix_time = None
            now = time.time()
            wakeup_time = now + MIN_GOOD_RES_CHECK_INTERVAL
            for res in self.ts_resources:
                # Ignore the busy resources
                if res.sr_concurrency > 0:
                    continue

                if res.sr_is_clean:
                    fix_time = res.sr_check_time + MIN_GOOD_RES_CHECK_INTERVAL
                else:
                    fix_time = res.sr_check_time + MIN_BAD_RES_CHECK_INTERVAL

                if fix_time > now:
                    if fix_time < wakeup_time:
                        wakeup_time = fix_time
                    continue

                if (fix_res is None or
                        res_fix_time < fix_time):
                    fix_res = res
                    res_fix_time = fix_time
            if fix_res is not None:
                # Hold the concurrency and create a thread to fix it
                fix_res.sr_concurrency = fix_res.sr_max_concurrency
                self.ts_condition.release()
                utils.thread_start(self._ts_resource_cleanup_holding_concurrency,
                                   (fix_res, ))
                self.ts_condition.acquire()
                continue
            # Sleep unless something happen or time for fixing again
            now = time.time()
            if wakeup_time > now:
                sleep_time = wakeup_time - now
                log.cl_debug("recovery thread is going to sleep for [%s] "
                             "seconds", sleep_time)
                start_time = now
                self.ts_condition.wait(sleep_time)
                now = time.time()
                log.cl_debug("recovery thread slept [%s] seconds",
                             now - start_time)
        self.ts_condition.release()

    def ts_jobs_check(self):
        """
        Checking the timeout of all active jobs. The scheduler checks all the
        jobs from time to time to cleanup timeout jobs.
        """
        log = self.ts_log
        log.cl_debug("scheduler is checking jobs")
        now = time_util.utcnow()
        stopped = False
        self.ts_condition.acquire()
        for job in self.ts_job_dict.values():
            log.cl_info("checking job [%s]", job.laj_jobid)
            diff = (now - job.laj_check_time).seconds
            if diff > TEST_HEARTBEAT_TIMEOUT:
                self._ts_job_stop(job)
                stopped = True
        if stopped:
            self.ts_condition.notifyAll()
        self.ts_condition.release()
        log.cl_debug("scheduler checked jobs")

    def _ts_job_stop(self, job):
        """
        Stop a job.
        """
        log = self.ts_log
        log.cl_info("job [%s] stopping", job.laj_jobid)
        for lhost in job.laj_hosts[:]:
            job.laj_host_remove(lhost)
            lhost.sr_job_sequence = None
            lhost.sr_concurrency -= 1
        for res in job.laj_ip_addresses[:]:
            job.laj_ip_address_remove(res)
            res.sr_job_sequence = None
            res.sr_concurrency -= 1
        del self.ts_job_dict[job.laj_jobid]

    def ts_job_stop(self, scheduler_id, jobid):
        """
        Stop a job. Usually called remotely by client or console.
        """
        log = self.ts_log
        if scheduler_id != self.ts_id:
            return -1
        self.ts_condition.acquire()
        if jobid not in self.ts_job_dict:
            log.cl_error("stopping unknown job [%s]", jobid)
            self.ts_condition.release()
            return -1
        job = self.ts_job_dict[jobid]
        self._ts_job_stop(job)
        self.ts_condition.notifyAll()
        self.ts_condition.release()
        return 0

    def ts_job_heartbeat(self, scheduler_id, jobid):
        """
        Stop a job. This is usually called remotely by client.
        """
        log = self.ts_log
        if scheduler_id != self.ts_id:
            log.cl_info("got a heartbeat from job [%s] with wrong scheduler "
                        "ID, expected [%s], got [%s]", jobid, self.ts_id,
                        scheduler_id)
            return -1
        log.cl_info("recived heatbeat of job [%s]", jobid)
        self.ts_condition.acquire()
        if jobid not in self.ts_job_dict:
            log.cl_error("heartbeat from unknown job [%s]", jobid)
            self.ts_condition.release()
            return -1
        job = self.ts_job_dict[jobid]
        job.laj_check_time = time_util.utcnow()
        self.ts_condition.release()
        return 0


def server_main(scheduler, scheduler_port):
    """
    Main function of scheduler thread.
    """
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(("0.0.0.0",
                                                    scheduler_port),
                                                   allow_none=True)
    server.register_introspection_functions()
    server.register_instance(scheduler)
    while not SHUTTING_DOWN:
        server.handle_request()


def parse_config_test_hosts(log, test_host_configs, kvm_template_dict):
    """
    Parse test hosts from configuration.
    :param test_host_configs:
    :return: host node list, None if failed
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    compute_node = re.compile(r"(?P<comname>[\w\-.]+)"
                              r"(?P<range>\[(?P<start>\d+)\-(?P<stop>\d+)\])?",
                              re.VERBOSE)
    hosts = list()
    for node_conf in test_host_configs:
        node_hostname = node_conf.get(cstr.CSTR_HOSTNAME)
        if node_hostname is None:
            log.cl_error("no [%s] found in items of section [%s]",
                         cstr.CSTR_HOSTNAME, cstr.CSTR_TEST_HOSTS)
            return None

        match = compute_node.match(node_hostname)
        if not match or not match.group("comname"):
            log.cl_error("wrong format of hostname configuration [%s]",
                         node_hostname)
            return None

        distro = node_conf.get(cstr.CSTR_DISTRO)
        if distro is None:
            log.cl_error("no [%s] found of node configuration [%s]",
                         cstr.CSTR_DISTRO, node_conf)
            return None

        purpose = node_conf.get(cstr.CSTR_PURPOSE)
        if purpose is None:
            log.cl_error("no [%s] found of node configuration [%s]",
                         cstr.CSTR_PURPOSE, node_conf)
            return None

        if purpose != PURPOSE_BUILD and purpose != PURPOSE_TEST:
            log.cl_error("unknown purpose [%s] of test host configuration [%s]",
                         purpose, node_conf)
            return None

        if purpose == PURPOSE_BUILD:
            concurrency = node_conf.get(cstr.CSTR_CONCURRENCY)
            if concurrency is None:
                log.cl_error("no [%s] found of node configuration [%s]",
                             cstr.CSTR_CONCURRENCY, node_conf)
                return None
        else:
            concurrency = 1
            kvm = node_conf.get(cstr.CSTR_KVM)
            if kvm is None:
                log.cl_debug("no [%s] found of kvm host configuration [%s]",
                             cstr.CSTR_KVM, node_conf)
                kvm_server_hostname = None
                kvm_template_ipv4_address = None
                template_hostname = None
                kvm_template = None
            else:
                kvm_server_hostname = kvm.get(cstr.CSTR_KVM_SERVER_HOSTNAME)
                if kvm_server_hostname is None:
                    log.cl_error("no [%s] found of kvm host configuration [%s]",
                                 cstr.CSTR_KVM_SERVER_HOSTNAME, kvm)
                    return None

                kvm_template_ipv4_address = kvm.get(cstr.CSTR_KVM_TEMPLATE_IPV4_ADDRESS)
                if kvm_template_ipv4_address is None:
                    log.cl_error("no [%s] found of kvm host configuration [%s]",
                                 cstr.CSTR_KVM_TEMPLATE_IPV4_ADDRESS, kvm)
                    return None

                template_hostname = kvm.get(cstr.CSTR_TEMPLATE_HOSTNAME)
                if template_hostname is None:
                    log.cl_error("no [%s] found of kvm host configuration [%s]",
                                 cstr.CSTR_TEMPLATE_HOSTNAME, kvm)
                    return None

                if template_hostname not in kvm_template_dict:
                    log.cl_error("no VM template with hostname [%s] is configured",
                                 template_hostname)
                    return None
                kvm_template = kvm_template_dict[template_hostname]

        tag = node_conf.get(cstr.CSTR_TAG)

        comname = match.group("comname")
        if not match.group("range"):
            # This assumes the /etc/hosts or LDAP is properly configured so
            # we can get the IP by the hostname
            ipv4_address = socket.gethostbyname(comname)
            ipv4_addresses = [ipv4_address]

            l_host = TestHost(comname, distro, purpose, tag,
                              concurrency, ipv4_addresses=ipv4_addresses,
                              kvm_server_hostname=kvm_server_hostname,
                              kvm_template_ipv4_address=kvm_template_ipv4_address,
                              kvm_template=kvm_template)
            hosts.append(l_host)
            continue

        start = int(match.group("start"))
        stop = int(match.group("stop")) + 1
        if start > stop:
            log.cl_error("range error in host configuration [%s]", node_conf)
            return None
        for i in range(start, stop):
            hostname = ("%s%d" % (comname, i))
            ipv4_address = socket.gethostbyname(hostname)
            ipv4_addresses = [ipv4_address]
            l_host = TestHost(hostname, distro, purpose, tag,
                              concurrency, kvm_server_hostname=kvm_server_hostname,
                              kvm_template_ipv4_address=kvm_template_ipv4_address,
                              ipv4_addresses=ipv4_addresses,
                              kvm_template=kvm_template)
            hosts.append(l_host)
    return hosts


def parse_config_test_hosts_and_templates(log, workspace, config, config_file):
    """
    Parse the scheduler configuration
    """
    test_host_configs = config.get(cstr.CSTR_TEST_HOSTS)
    if test_host_configs is None:
        log.cl_error("no section [%s] found in configuration file [%s]",
                     cstr.CSTR_TEST_HOSTS, config_file)
        return None

    kvm_template_dict = lvirt.parse_templates_config(log, workspace,
                                                     config, config_file,
                                                     hosts=None)
    if kvm_template_dict is None:
        log.cl_error("failed to parse template configs in file [%s]",
                     config_file)
        return None

    test_hosts = parse_config_test_hosts(log, test_host_configs, kvm_template_dict)
    if test_hosts is None:
        log.cl_error("failed to parse [%s] from configuration file [%s]",
                     cstr.CSTR_TEST_HOSTS, config_file)
        return None

    return test_hosts


def parse_config_ip_addresses(log, config, config_fpath):
    """
    Parse the IP adress config
    """
    ip_addresses = []
    address_configs = config.get(cstr.CSTR_IP_ADDRESSES)
    if address_configs is None:
        log.cl_error("no section [%s] found in configuration file [%s]",
                     cstr.CSTR_IP_ADDRESSES, config_fpath)
        return None

    for address_config in address_configs:
        address = address_config.get(cstr.CSTR_IP_ADDRESS)
        if address is None:
            log.cl_error("one of the config in [%s] doesn't have [%s] "
                         "configured, please correct configuration file [%s]",
                         cstr.CSTR_IP_ADDRESSES, cstr.CSTR_IP_ADDRESS,
                         config_fpath)
            return None

        bindnetaddr = address_config.get(cstr.CSTR_BINDNETADDR)
        if bindnetaddr is None:
            log.cl_error("the config of ip address with [%s] in [%s] doesn't "
                         "have [%s] configured, please correct configuration "
                         "file [%s]", address, cstr.CSTR_IP_ADDRESSES,
                         cstr.CSTR_BINDNETADDR, config_fpath)
            return None

        ip_address = IPAddress(address, bindnetaddr)
        ip_addresses.append(ip_address)
    return ip_addresses


def signal_handler(signum, frame):
    """
    Singnal hander. Set the shutting down flag.
    """
    # pylint: disable=unused-argument,global-statement
    log = GLOBAL_LOG
    log.cl_info("signal handler called with signal [%d]", signum)
    global SHUTTING_DOWN
    SHUTTING_DOWN = True


def ltest_scheduler(log, workspace, config_fpath):
    """
    Start to test Clownfish holding the configure lock
    """
    # pylint: disable=bare-except,global-statement
    global GLOBAL_LOG

    GLOBAL_LOG = log

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

    scheduler_id = os.path.basename(workspace)
    log.cl_info("Clownfish test scheduler started, please check [%s] for more log",
                workspace)

    scheduler_port = config.get(cstr.CSTR_PORT)
    if scheduler_port is None:
        scheduler_port = TEST_SCHEDULER_PORT

    addresses = parse_config_ip_addresses(log, config,
                                          config_fpath)
    if addresses is None:
        log.cl_error("failed to parse config of addresses")
        return -1

    test_hosts = parse_config_test_hosts_and_templates(log, workspace, config,
                                                       config_fpath)
    if test_hosts is None:
        log.cl_error("failed to parse config test hosts and templates")
        return -1

    scheduler = TestScheduler(log, scheduler_id, test_hosts, addresses)
    output = scheduler.ts_host_list(False)
    log.cl_info("\n%s", output)

    output = scheduler.ts_ip_address_list(False)
    log.cl_info("\n%s", output)
    # Set signal hander before start to handling reqeust.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    utils.thread_start(server_main, (scheduler, scheduler_port))
    utils.thread_start(scheduler.ts_recovery_main, ())

    while not SHUTTING_DOWN:
        scheduler.ts_jobs_check()
        time.sleep(TEST_HEARTBEAT_TIMEOUT)
    log.cl_info("stopping test scheduler service")
    return 0


def main():
    """
    Start to test Clownfish
    """
    cmd_general.main(TEST_SCHEDULER_CONFIG, TEST_SCHEDULER_LOG_DIR,
                     ltest_scheduler)
