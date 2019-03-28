# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: gzheng@ddn.com
"""
Tool for Clownfish cluster setup
Clownfish is an automatic management system for Lustre
"""
import sys
import os
import re
import time

# Local libs
from pylustre import utils
from pylustre import ssh_host
from pylustre import cstr
from pylustre import cmd_general
from pylustre import install_common
from pylustre import install_common_nodeps
from pylustre import constants
from pyclownfish import clownfish_common

CLOWNFISH_COROSYNC_FNAME = "corosync.conf"
CLOWNFISH_AUTHKEY_FNAME = "authkey"
COROSYNC_CONFIG_DIR = "/etc/corosync/"
CLOWNFISH_COROSYNC_CONFIG = COROSYNC_CONFIG_DIR + CLOWNFISH_COROSYNC_FNAME
CLOWNFISH_COROSYNC_AUTHKEY = COROSYNC_CONFIG_DIR + CLOWNFISH_AUTHKEY_FNAME
CLOWNFISH_ISO_DIR = "/mnt/clownfish_ha/"


def pcs_check_resource(log, host, res_name):
    """
    Check pacemaker resource on target host, return the operator
    """
    command = "pcs resource show " + res_name
    operator = "create"
    retval = host.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_info("creating [%s] resource on cluster",
                    res_name)
    else:
        log.cl_info("updating existing [%s] resource on cluster",
                    res_name)
        operator = "update"

    return operator


class ClownfishCluster(install_common.InstallationCluster):
    # pylint: disable=too-many-instance-attributes,too-many-arguments
    """
    Clownfish HA cluster config.
    """
    def __init__(self, workspace, hosts, virtual_ip, bindnetaddr, mnt_path,
                 clownfish_config_fpath):
        super(ClownfishCluster, self).__init__(workspace,
                                               hosts,
                                               mnt_path)
        self.ccl_virtual_ip = virtual_ip
        self.ccl_bindnetaddr = bindnetaddr
        # Clownfish config file path on installation server
        self.ccl_clownfish_config_fpath = clownfish_config_fpath
        self.ccl_corosync_config = ("""
totem {
    version: 2
    interface {
        ringnumber: 0
        bindnetaddr: %s
        mcastaddr: 226.94.1.1
        mcastport: 5405
        ttl: 1
    }
}
service {
    ver:  0
    name: pacemaker
}
logging {
    to_logfile: yes
    logfile: /var/log/cluster/corosync.log
    to_syslog: yes
    logger_subsys {
        subsys: QUORUM
        debug: off
    }
}
aisexec {
    user: root
    group: root
}
quorum {
    provider: corosync_votequorum
}
""" % (bindnetaddr))
        nodelist_string = "nodelist {"
        for host in self.ic_hosts:
            nodelist_string += ("""
    node {
        ring0_addr: %s
    }""" % (host.sh_hostname))
        nodelist_string += """
}"""
        self.ccl_corosync_config += nodelist_string

    def ccl_check_before_install(self, log, localhost):
        """
        Check the virtual IP before install
        """
        # Stop Corosync so that the virtual IP will be stopped
        for host in self.ic_hosts:
            command = "systemctl status corosync"
            retval = host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                log.cl_info("stopping corosync on host [%s]",
                            host.sh_hostname)
                command = "systemctl stop corosync"
                retval = host.sh_run(log, command)
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to run command [%s] on host "
                                 "[%s], ret = [%d], stdout = [%s], stderr = "
                                 "[%s]",
                                 command,
                                 host.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

        ret = localhost.sh_check_network_connection(log, self.ccl_virtual_ip,
                                                    quiet=True)
        if ret == 0:
            log.cl_error("IP [%s] is under use by some host",
                         self.ccl_virtual_ip)
            return -1
        return 0

    def ccl_config(self, log, workspace):
        """
        Configure corosync and pacemaker, and add target resource
        """
        # pylint: disable=too-many-branches
        # edit corosync.conf and sync to all ha hosts
        corosync_config_fpath = workspace + "/" + CLOWNFISH_COROSYNC_FNAME
        corosync_config_fd = open(corosync_config_fpath, 'w')
        if not corosync_config_fd:
            log.cl_error("failed to open file [%s] on localhost",
                         corosync_config_fpath)
            return -1
        corosync_config_fd.write(self.ccl_corosync_config)
        corosync_config_fd.close()

        # Generate corosync authkey on host 0
        host_first = self.ic_hosts[0]
        command = "/usr/sbin/corosync-keygen --less-secure"
        retval = host_first.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to start run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host_first.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # sync clownfish.conf, corosync.conf and authkey to all ha hosts
        for host in self.ic_hosts:
            ret = host.sh_send_file(log, self.ccl_clownfish_config_fpath,
                                    constants.CLOWNFISH_CONFIG)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "file [%s] on host [%s]",
                             self.ccl_clownfish_config_fpath,
                             constants.CLOWNFISH_CONFIG,
                             host.sh_hostname)
                return ret

            ret = host.sh_send_file(log, corosync_config_fpath,
                                    CLOWNFISH_COROSYNC_CONFIG)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "file [%s] on host [%s]",
                             corosync_config_fpath,
                             CLOWNFISH_COROSYNC_CONFIG,
                             host.sh_hostname)
                return ret

            if host != host_first:
                ret = host_first.sh_send_file(log, CLOWNFISH_COROSYNC_AUTHKEY,
                                              CLOWNFISH_COROSYNC_AUTHKEY,
                                              from_local=False,
                                              remote_host=host)
                if ret:
                    log.cl_error("failed to send file [%s] on host [%s] to "
                                 "file [%s] on host [%s]",
                                 CLOWNFISH_COROSYNC_AUTHKEY,
                                 host_first.sh_hostname,
                                 CLOWNFISH_COROSYNC_AUTHKEY,
                                 host.sh_hostname)
                    return ret

            log.cl_info("configuring autostart of corosync and pacemaker on "
                        "host [%s]", host.sh_hostname)
            command = "systemctl enable corosync pacemaker"
            retval = host.sh_run(log, command)
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

    def ccl_start(self, log):
        """
        Config and create clownfish resouce.
        """
        # start pacemaker and corosync
        command = "systemctl restart corosync pacemaker"
        for host in self.ic_hosts:
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host "
                             "[%s], ret = [%d], stdout = [%s], stderr = "
                             "[%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        host0 = self.ic_hosts[0]
        # config pacemaker property on host0
        command = ("pcs property set stonith-enabled=false "
                   "no-quorum-policy=ignore")
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to set pacemaker property [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # set VIP and add clownfish_server resource on ha host0
        operator = pcs_check_resource(log, host0, "VIP")
        command = ("pcs resource %s VIP ocf:heartbeat:IPaddr2 ip=%s "
                   "cidr_netmask=" %
                   (operator, self.ccl_virtual_ip))
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to create/update VIP on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        operator = pcs_check_resource(log, host0, "clownfish_server")
        command = ("pcs resource %s clownfish_server systemd:clownfish_server "
                   "op monitor interval=10s" % operator)
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to create/update clownfish_server on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # set VIP front of clownfish_server and run on same host
        command = "pcs constraint colocation add clownfish_server VIP INFINITY --force"
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        command = ("pcs constraint order start VIP then start "
                   "clownfish_server --force")
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to set VIP&clownfish_server order [%s] on "
                         "host [%s], ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def ccl_check_virtual_ip(self, log, localhost):
        """
        Make sure local host can connect to virtual IP
        """
        return localhost.sh_check_network_connection(log, self.ccl_virtual_ip)

    def ccl_check_clownfish_service(self, log):
        """
        Make sure at least a clownfish service is running
        """
        # pylint: disable=unused-argument
        command = "systemctl status clownfish_server"
        for host in self.ic_hosts:
            retval = host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                return 0
        return -1

    def ccl_clownfish_host(self, log):
        """
        Return the host running clownfish
        """
        command = "systemctl status clownfish_server"
        running_hosts = []
        for host in self.ic_hosts:
            retval = host.sh_run(log, command)
            if retval.cr_exit_status == 0:
                running_hosts.append(host)
        if len(running_hosts) == 0:
            log.cl_error("no host is running Clownfish server service")
            return None
        elif len(running_hosts) > 1:
            host_names = []
            for host in running_hosts:
                host_names.append(host.sh_hostname)
            log.cl_error("multiple hosts %s are running Clownfish server "
                         "service unexpectly", host_names)
            return None
        return running_hosts[0]

    def ccl_check_high_availability(self, log, localhost):
        """
        Make sure Clownfish is running properly
        """
        ret = utils.wait_condition(log, self.ccl_check_virtual_ip,
                                   (localhost, ))
        if ret:
            log.cl_error("timeout when waiting connection to IP [%s]",
                         self.ccl_virtual_ip)
            return ret

        ret = utils.wait_condition(log, self.ccl_check_clownfish_service, ())
        if ret:
            log.cl_error("timeout when waiting Clownfish service to start "
                         "running")
            return ret

        running_host = self.ccl_clownfish_host(log)
        if running_host is None:
            return ret

        command = "clownfish_console %s pwd" % self.ccl_virtual_ip
        for host in self.ic_hosts:
            retval = host.sh_run(log, command)
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

    def ccl_check(self, log, localhost):
        """
        Check the high availability works well
        """
        # pylint: disable=too-many-branches
        ret = self.ccl_check_high_availability(log, localhost)
        if ret:
            log.cl_error("Clownfish doesn't work well")
            return -1

        if len(self.ic_hosts) < 2:
            return 0

        time_start = time.time()
        # Run HA test for 1 minutes
        timeout = 60
        test_times = 1
        while True:
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed >= timeout:
                break

            running_host = self.ccl_clownfish_host(log)
            if running_host is None:
                return ret

            log.cl_info("testing HA of Clownfish for [%s] time(s) by "
                        "stopping services on [%s]",
                        test_times, running_host.sh_hostname)
            test_times += 1

            command = "systemctl stop clownfish_server"
            retval = running_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             running_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = self.ccl_check_high_availability(log, localhost)
            if ret:
                log.cl_error("high availability doesn't work well after "
                             "stopping Clownfish on host [%s]",
                             running_host.sh_hostname)
                return -1

            command = "systemctl stop pacemaker"
            retval = running_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             running_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = self.ccl_check_high_availability(log, localhost)
            if ret:
                log.cl_error("high availability doesn't work well after "
                             "stopping pacemaker on host [%s]",
                             running_host.sh_hostname)
                return -1

            command = "systemctl stop corosync"
            retval = running_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             running_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = self.ccl_check_high_availability(log, localhost)
            if ret:
                log.cl_error("high availability doesn't work well after "
                             "stopping corosync on host [%s]",
                             running_host.sh_hostname)
                return -1

            # start pacemaker and corosync again
            command = "systemctl start corosync pacemaker"
            retval = running_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             running_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        running_host = self.ccl_clownfish_host(log)
        if running_host is None:
            return -1
        log.cl_info("Clownfish server serivce is running on host [%s] currently",
                    running_host.sh_hostname)
        log.cl_info("Please connect to it using command [clownfish_console %s]",
                    self.ccl_virtual_ip)
        return 0


def clownfish_parse_server_hosts(log, config, config_fpath):
    """
    Return the server hosts of clownfish
    """
    ssh_host_configs = utils.config_value(config, cstr.CSTR_SSH_HOSTS)
    if ssh_host_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_SSH_HOSTS, config_fpath)
        return None

    hosts = {}
    for host_config in ssh_host_configs:
        host_id = host_config[cstr.CSTR_HOST_ID]
        if host_id is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_HOST_ID, config_fpath)
            return None

        hostname = utils.config_value(host_config, cstr.CSTR_HOSTNAME)
        if hostname is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "with ID [%s], please correct file [%s]",
                         cstr.CSTR_HOSTNAME, host_id, config_fpath)
            return None

        ssh_identity_file = utils.config_value(host_config, cstr.CSTR_SSH_IDENTITY_FILE)

        if host_id in hosts:
            log.cl_error("multiple SSH hosts with the same ID [%s], please "
                         "correct file [%s]", host_id, config_fpath)
            return None
        host = ssh_host.SSHHost(hostname,
                                identity_file=ssh_identity_file,
                                host_id=host_id)
        hosts[host_id] = host

    cluster_config = utils.config_value(config, cstr.CSTR_CLUSTER)
    if cluster_config is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_CLUSTER, config_fpath)
        return None

    clownfish_hosts = []
    for host_config in cluster_config:
        host_id = host_config[cstr.CSTR_HOST_ID]
        if host_id is None:
            log.cl_error("can NOT find [%s/%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_CLUSTER, cstr.CSTR_HOST_ID, config_fpath)
            return None

        if host_id not in hosts:
            log.cl_error("host with host id is not configured,"
                         "please correct file [%s]", config_fpath)
            return None

        clownfish_hosts.append(hosts[host_id])
    return clownfish_hosts


def clownfish_install_parse_config(log, workspace, config, config_fpath, mnt_path):
    """
    Parse the config and init Clownfish HA cluster
    """
    # pylint: disable=too-many-locals,too-many-branches
    iso_path = utils.config_value(config, cstr.CSTR_ISO_PATH)
    if iso_path is None:
        log.cl_info("no [%s] in the config file", cstr.CSTR_ISO_PATH)
    elif not os.path.exists(iso_path):
        log.cl_error("ISO file [%s] doesn't exist", iso_path)
        return -1

    virtual_ip = utils.config_value(config, cstr.CSTR_VIRTUAL_IP)
    if not virtual_ip:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VIRTUAL_IP, config_fpath)
        return None

    ip_regular = (r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
                  "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
    pattern = re.match(ip_regular, virtual_ip, re.IGNORECASE)
    if not pattern:
        log.cl_error("wrong format of virtual IP [%s], please correct file "
                     "[%s]", virtual_ip, config_fpath)
        return None

    bindnetaddr = utils.config_value(config, cstr.CSTR_BINDNETADDR)
    if not bindnetaddr:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_BINDNETADDR, config_fpath)
        return None
    pattern = re.match(ip_regular, bindnetaddr, re.IGNORECASE)
    if not pattern:
        log.cl_error("wrong format of bindnetaddr [%s], please correct file "
                     "[%s]", bindnetaddr, config_fpath)
        return None

    clownfish_hosts = clownfish_parse_server_hosts(log, config, config_fpath)
    if clownfish_hosts is None:
        log.cl_error("failed to parse Clownfish server hosts, please correct "
                     "file [%s]", config_fpath)
        return None

    clownfish_config_fpath = utils.config_value(config,
                                                cstr.CSTR_CONFIG_FPATH)
    if clownfish_config_fpath is None:
        log.cl_error("can NOT find [%s] in the installation config, "
                     "please correct file [%s]",
                     cstr.CSTR_CONFIG_FPATH, config_fpath)
        return -1

    return ClownfishCluster(workspace, clownfish_hosts, virtual_ip, bindnetaddr,
                            mnt_path, clownfish_config_fpath)


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s <config_file>" %
                 sys.argv[0])


def clownfish_install(log, workspace, config, config_fpath, mnt_path,
                      localhost):
    # pylint: disable=too-many-arguments
    """
    Install Clownfish cluster
    """
    cluster = clownfish_install_parse_config(log, workspace, config, config_fpath,
                                             mnt_path)
    if cluster is None:
        log.cl_error("failed to parse config of Clownfish cluster")
        return -1

    ret = cluster.ccl_check_before_install(log, localhost)
    if ret:
        log.cl_error("failed to check before installing Clownfish cluster")
        return -1

    ret = cluster.ic_install(log, [], clownfish_common.CLOWNFISH_DEPENDENT_RPMS)
    if ret:
        log.cl_error("failed to install Clownfish cluster")
        return -1

    ret = cluster.ccl_config(log, workspace)
    if ret:
        log.cl_error("failed to configure Clownfish cluster")
        return -1

    ret = cluster.ccl_start(log)
    if ret:
        log.cl_error("failed to start Clownfish cluster")
        return -1

    ret = cluster.ccl_check(log, localhost)
    if ret:
        log.cl_error("failed to check Clownfish cluster")
        return -1
    return 0


def clownfish_mount_and_install(log, workspace, config_fpath):
    """
    Start install Clownfish
    """
    ret = install_common_nodeps.mount_iso_and_install(log, workspace,
                                                      config_fpath,
                                                      clownfish_install)
    if ret:
        log.cl_info("Failed to install Clownfish cluster, please check [%s] "
                    "for more log", workspace)
    else:
        log.cl_info("Clownfish cluster is started successfully, please check [%s] "
                    "for more log", workspace)
    return ret


def main():
    """
    Clownfish installation command
    """
    cmd_general.main(constants.CLOWNFISH_INSTALL_CONFIG,
                     constants.CLOWNFISH_INSTALL_LOG_DIR,
                     clownfish_mount_and_install)
