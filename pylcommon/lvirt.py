# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for installing virtual machines
"""
# pylint: disable=too-many-lines
import sys
import traceback
import random
import yaml

# Local libs
from pylcommon import utils
from pylcommon import ssh_host
from pylcommon import lustre
from pylcommon import cstr
from pylcommon import cmd_general

LVIRT_CONFIG_FNAME = "lvirt.conf"
LVIRT_CONFIG = "/etc/" + LVIRT_CONFIG_FNAME
LVIRT_LOG_DIR = "/var/log/lvirt"
LVIRT_UDEV_RULES = "/etc/udev/rules.d/80-lvirt-name.rules"


class VirtTemplate(object):
    """
    Each virtual machine template has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # pylint: disable=too-many-arguments
    def __init__(self, iso, template_hostname, internet, network_configs,
                 image_dir, distro, ram_size, disk_sizes, dns,
                 bus_type=cstr.CSTR_BUS_SCSI,
                 server_host=None, server_host_id=None, reinstall=None):
        self.vt_server_host = server_host
        self.vt_server_host_id = server_host_id
        self.vt_reinstall = reinstall

        self.vt_dns = dns
        self.vt_iso = iso
        self.vt_template_hostname = template_hostname
        self.vt_internet = internet
        self.vt_network_configs = network_configs
        self.vt_image_dir = image_dir
        self.vt_distro = distro
        self.vt_ram_size = ram_size
        self.vt_disk_sizes = disk_sizes
        self.vt_bus_type = bus_type


class SharedDisk(object):
    """
    Each shared disk has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, disk_id, server_host, server_host_id, image_fpath, size):
        self.sd_disk_id = disk_id
        self.sd_server_host = server_host
        self.sd_image_fpath = image_fpath
        self.sd_size = size
        self.sd_server_host_id = server_host_id
        self.sd_targets = []

    def _sd_create(self, log):
        """
        Create the shared disk
        """
        command = ("qemu-img create -f raw %s %sG" %
                   (self.sd_image_fpath, self.sd_size))

        retval = self.sd_server_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sd_server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def sd_add_target(self, target):
        """
        Add a host which shares this disk
        """
        self.sd_targets.append(target)

    def _sd_share_target(self, log, target):
        """
        Share the disk with the host
        """
        # pylint: disable=too-many-branches,too-many-return-statements
        target_name = target.st_target_name
        host = target.st_host

        ret, devices = host.sh_lsscsi(log)
        if ret:
            log.cl_error("failed to get device on host [%s]",
                         host.sh_hostname)
            return -1

        command = ("virsh attach-disk %s %s %s --subdriver raw --persistent --cache=directsync" %
                   (host.sh_hostname, self.sd_image_fpath, target_name))
        retval = self.sd_server_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.sd_server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # Need to wait until VM shut off, otherwise "virsh change-media" won't
        # change the XML file
        ret = utils.wait_condition(log, host_check_lsscsi,
                                   (host, len(devices) + 1))
        if ret:
            log.cl_error("timeout when waiting the device number of host "
                         "[%s]", host.sh_hostname)
            return ret

        ret, new_devices = host.sh_lsscsi(log)
        if ret:
            log.cl_error("failed to get device on host [%s]",
                         host.sh_hostname)
            return -1

        if len(new_devices) != len(devices) + 1:
            log.cl_error("unexpected new devices number %s on host [%s], old "
                         "devices %s", new_devices, host.sh_hostname, devices)
            return -1

        new_device = None
        for device in new_devices:
            if device not in devices:
                if new_device is not None:
                    log.cl_error("unexpected new devices %s on host [%s], "
                                 "old devices %s", new_devices,
                                 host.sh_hostname, devices)
                    return -1
                new_device = device

        serial = host.sh_device_serial(log, new_device)
        if serial is None:
            log.cl_error("failed to get serial of device [%s] on host [%s]",
                         serial, host.sh_hostname)
            return -1

        log.cl_debug("added device [%s] with serial number [%s] on host [%s]",
                     new_device, serial, host.sh_hostname)
        command = ('echo \'ENV{ID_SERIAL}=="%s", SYMLINK+="mapper/%s"\' >> %s' %
                   (serial, self.sd_disk_id, LVIRT_UDEV_RULES))
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

        command = "udevadm control --reload-rules && udevadm trigger"
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

        device_link = "/dev/mapper/" + self.sd_disk_id
        command = "readlink -f %s" % device_link
        expect_stdout = new_device + "\n"
        ret = host.sh_wait_update(log, command, expect_exit_status=0,
                                  expect_stdout=expect_stdout)
        if ret:
            log.cl_error("created wrong symlink [%s] on host "
                         "[%s], expected [%s]",
                         device_link, host.sh_hostname, new_device)
            return -1
        return 0

    def sd_share(self, log):
        """
        Share the disk on all hosts
        """
        log.cl_info("sharing disk [%s]", self.sd_disk_id)
        if len(self.sd_targets) == 0:
            return 0
        ret = self._sd_create(log)
        if ret:
            log.cl_error("failed to create shared disk [%s] on host with "
                         "ID [%s]", self.sd_image_fpath,
                         self.sd_server_host_id)
            return -1

        for target in self.sd_targets:
            ret = self._sd_share_target(log, target)
            if ret:
                log.cl_error("failed to share disk [%s] on server host with "
                             "ID [%s] to VM [%s]", self.sd_image_fpath,
                             self.sd_server_host_id, target.st_host.sh_hostname)
                return -1
        return 0


class SharedTarget(object):
    """
    Each shared disk on each VM has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, vm_host, target_name):
        self.st_host = vm_host
        self.st_target_name = target_name


def random_mac():
    """
    Generate random MAC address
    """
    mac_parts = [random.randint(0x00, 0x7f),
                 random.randint(0x00, 0xff),
                 random.randint(0x00, 0xff)]
    mac_string = "52:54:00"
    for mac_part in mac_parts:
        mac_string += ":" + ("%02x" % mac_part)
    return mac_string


def vm_is_shut_off(log, server_host, hostname):
    """
    Check whether vm is shut off
    """
    state = server_host.sh_virsh_dominfo_state(log, hostname)
    if state is None:
        return False
    elif state == "shut off":
        return True
    return False


def host_check_lsscsi(log, host, expect_dev_number):
    """
    Check whether scsi number is expected
    """
    ret, new_devices = host.sh_lsscsi(log)
    if ret:
        log.cl_error("failed to get device on host [%s]",
                     host.sh_hostname)
        return -1
    if len(new_devices) == expect_dev_number:
        return 0
    else:
        return -1


def vm_check_shut_off(log, server_host, hostname):
    """
    Check whether vm is shut off
    """
    off = vm_is_shut_off(log, server_host, hostname)
    if off:
        return 0
    return -1


def vm_delete(log, server_host, hostname):
    """
    Delete a virtual machine
    """
    existed = True
    active = True
    state = server_host.sh_virsh_dominfo_state(log, hostname)
    if state is None:
        existed = False
        active = False
    elif state == "shut off":
        active = False

    if active:
        command = ("virsh destroy %s" % hostname)
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

    if existed:
        command = ("virsh undefine %s" % hostname)
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

    return 0


def vm_clone(log, workspace, server_host, hostname, network_configs, ips,
             template_hostname, image_dir, distro, internet, disk_number):
    """
    Create virtual machine
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    log.cl_info("cloning host [%s] from template [%s]", hostname,
                template_hostname)
    host_ip = ips[0]
    ret = vm_delete(log, server_host, hostname)
    if ret:
        return -1

    command = ("ping -c 1 %s" % host_ip)
    retval = server_host.sh_run(log, command)
    if retval.cr_exit_status == 0:
        log.cl_error("IP [%s] already used by a host", host_ip)
        return -1

    command = ("ping -c 1 %s" % hostname)
    retval = server_host.sh_run(log, command)
    if retval.cr_exit_status == 0:
        log.cl_error("host [%s] already up", hostname)
        return -1

    active = True
    state = server_host.sh_virsh_dominfo_state(log, template_hostname)
    if state is None:
        log.cl_error("template [%s] doesn't exist on host [%s]",
                     template_hostname, server_host.sh_hostname)
        return -1
    elif state == "shut off":
        active = False

    if active:
        command = ("virsh destroy %s" % template_hostname)
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

    file_options = ""
    for disk_index in range(disk_number):
        file_options += (" --file %s/%s_%d.img" %
                         (image_dir, hostname, disk_index))

        command = ("rm -f %s/%s_%d.img" %
                   (image_dir, hostname, disk_index))
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

    command = ("virt-clone --original %s --name %s%s" %
               (template_hostname, hostname, file_options))
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

    local_host_dir = workspace + "/" + hostname
    ret = utils.mkdir(local_host_dir)
    if ret:
        log.cl_error("failed to create directory [%s] on local host",
                     local_host_dir)
        return -1

    # net.ifnames=0 biosdevname=0 has been added to grub, so the interface
    # name will always be eth*
    eth_number = 0
    for eth_ip in ips:
        network_config = network_configs[eth_number]
        ifcfg = 'DEVICE="eth%d"\n' % eth_number
        ifcfg += 'IPADDR="%s"\n' % eth_ip
        ifcfg += 'NETMASK="%s"\n' % network_config["netmask"]
        if "gateway" in network_config:
            ifcfg += 'GATEWAY=\"%s"\n' % network_config["gateway"]
        ifcfg += """ONBOOT=yes
BOOTPROTO="static"
TYPE=Ethernet
IPV6INIT=no
NM_CONTROLLED=no
"""

        ifcfg_fname = "ifcfg-eth%d" % eth_number
        ifcfg_fpath = local_host_dir + "/" + ifcfg_fname
        with open(ifcfg_fpath, "wt") as fout:
            fout.write(ifcfg)

        host_ifcfg_fpath = workspace + "/" + ifcfg_fname
        ret = server_host.sh_send_file(log, ifcfg_fpath, workspace)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         ifcfg_fpath, workspace,
                         server_host.sh_hostname)
            return -1

        ret = server_host.sh_run(log, "which virt-copy-in")
        if ret.cr_exit_status != 0:
            command = ("yum install libguestfs-tools-c -y")
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

        command = ("virt-copy-in -d %s %s "
                   "/etc/sysconfig/network-scripts" % (hostname, host_ifcfg_fpath))
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
        eth_number += 1

    host_rules_fpath = workspace + "/70-persistent-net.rules"
    command = ("> %s" % host_rules_fpath)
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

    command = ("virt-copy-in -d %s %s "
               "/etc/udev/rules.d" % (hostname, host_rules_fpath))
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

    if distro == ssh_host.DISTRO_RHEL6:
        network_string = 'NETWORKING=yes\n'
        network_string += 'HOSTNAME=%s\n' % hostname
        network_fname = "network"
        network_fpath = local_host_dir + "/" + network_fname
        with open(network_fpath, "wt") as fout:
            fout.write(network_string)

        host_network_fpath = workspace + "/" + network_fname
        ret = server_host.sh_send_file(log, network_fpath, workspace)
        if ret:
            log.cl_error("failed to send file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         network_fpath, workspace,
                         server_host.sh_hostname)
            return -1

        command = ("virt-copy-in -d %s %s "
                   "/etc/sysconfig" % (hostname, host_network_fpath))
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
    else:
        host_hostname_fpath = workspace + "/hostname"
        command = ("echo %s > %s" % (hostname, host_hostname_fpath))
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

        command = ("virt-copy-in -d %s %s "
                   "/etc" % (hostname, host_hostname_fpath))
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

    command = ("virsh start %s" % hostname)
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

    # Remove the record in known_hosts, otherwise ssh will fail
    command = ('sed -i "/%s /d" /root/.ssh/known_hosts' % (host_ip))
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

    # Remove the record in known_hosts, otherwise ssh will fail
    command = ('sed -i "/%s /d" /root/.ssh/known_hosts' % (hostname))
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

    vm_host = ssh_host.SSHHost(host_ip)
    ret = vm_host.sh_wait_up(log)
    if ret:
        log.cl_error("failed to wait host [%s] up",
                     host_ip)
        return -1

    ret = vm_check(log, hostname, host_ip, distro, internet)
    if ret:
        return -1
    return 0


def vm_check(log, hostname, host_ip, distro, internet):
    """
    Check whether virtual machine is up and fine
    """
    # pylint: disable=too-many-return-statements
    vm_host = ssh_host.SSHHost(host_ip)
    command = "hostname"
    retval = vm_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     host_ip,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    current_hostname = retval.cr_stdout.strip()
    if current_hostname != hostname:
        log.cl_error("wrong host name of the virtual machine [%s], expected "
                     "[%s], got [%s]", host_ip, hostname, current_hostname)
        return -1

    vm_host = ssh_host.SSHHost(hostname)
    command = "hostname"
    retval = vm_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    current_hostname = retval.cr_stdout.strip()
    if current_hostname != hostname:
        log.cl_error("wrong host name of the virtual machine [%s], expected "
                     "[%s], got [%s]", hostname, hostname, current_hostname)
        return -1

    vm_distro = vm_host.sh_distro(log)
    if vm_distro != distro:
        log.cl_error("wrong distro of the virtual machine [%s], expected "
                     "[%s], got [%s]", hostname, distro, vm_distro)
        return -1

    if internet:
        if vm_host.sh_check_internet(log):
            log.cl_error("virtual machine [%s] can not access Internet",
                         hostname)
            return -1
    return 0


def vm_start(log, workspace, server_host, hostname, network_configs, ips,
             template_hostname, image_dir, distro, internet, disk_number):
    """
    Start virtual machine, if vm is bad, clone it
    """
    # pylint: disable=too-many-arguments,too-many-locals
    log.cl_info("starting virtual machine [%s]", hostname)
    host_ip = ips[0]
    ret = vm_check(log, hostname, host_ip, distro, internet)
    if ret == 0:
        return 0

    if vm_is_shut_off(log, server_host, hostname):
        command = ("virsh start %s" % (hostname))
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

    vm_host = ssh_host.SSHHost(hostname)
    ret = vm_host.sh_wait_up(log)
    if ret == 0:
        ret = vm_check(log, hostname, host_ip, distro, internet)
        if ret == 0:
            return 0

    ret = vm_clone(log, workspace, server_host, hostname, network_configs, ips,
                   template_hostname, image_dir, distro, internet, disk_number)
    if ret:
        log.cl_error("failed to create virtual machine [%s] based on "
                     "template [%s]", hostname, template_hostname)
        return -1
    return 0


def mount_iso(log, server_host, iso_path):
    """
    Mount the ISO, return the mnt path
    """
    mnt_path = "/mnt/" + utils.random_word(8)
    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, iso_path, mnt_path))
    retval = server_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     server_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None
    return mnt_path


def _vm_install(log, workspace, bus_type, image_dir, ram_size, disk_sizes,
                distro, hostname, network_configs, server_host, iso_path,
                mnt_path):
    """
    Actually start to install by using virt_install
    """
    # pylint: disable=too-many-arguments,too-many-locals
    # pylint: disable=too-many-return-statements,too-many-statements
    # pylint: disable=too-many-branches
    ks_config = """# Kickstart file automatically generated by Lvirt.
install
reboot
cdrom
lang en_US.UTF-8
keyboard us
"""
    pri_disk = ""
    if bus_type == cstr.CSTR_BUS_VIRTIO:
        pri_disk = cstr.CSTR_DISK_VIRTIO_PRIMARY
    elif bus_type == cstr.CSTR_BUS_IDE:
        pri_disk = cstr.CSTR_DISK_IDE_PRIMARY
    elif bus_type == cstr.CSTR_BUS_SCSI:
        pri_disk = cstr.CSTR_DISK_SCSI_PRIMARY
    else:
        log.cl_error("unsupported bus type [%s], please correct it",
                     bus_type)
        return -1
    ks_config += """rootpw password
firewall --disabled
authconfig --enableshadow --passalgo=sha512
selinux --disabled
timezone --utc Asia/Shanghai
"""
    ks_config += """bootloader --location=mbr --driveorder=%s --append="crashkernel=auto net.ifnames=0 biosdevname=0"\
""" % pri_disk
    ks_config += """
zerombr
clearpart --all --initlabel
"""
    ks_config += "part / --fstype=ext4 --grow --size=500 --ondisk=%s --asprimary" % pri_disk
    ks_config += """
repo --name="Media" --baseurl=file:///mnt/source --cost=100
%packages
@Core
%end
%post --log=/var/log/anaconda/post-install.log
#!/bin/bash
# Configure hostname, somehow virt-install --name doesn't work
"""
    if distro == ssh_host.DISTRO_RHEL6:
        ks_config += 'echo NETWORKING=yes > /etc/sysconfig/network\n'
        ks_config += ('echo HOSTNAME=%s >> /etc/sysconfig/network\n' %
                      (hostname))
    elif distro == ssh_host.DISTRO_RHEL7:
        ks_config += "echo %s > /etc/hostname\n" % (hostname)
    else:
        log.cl_error("wrong distro [%s]", distro)
        return -1
    ks_config += "# Configure network\n"
    eth_number = 0
    ens_number = 3
    for network_config in network_configs:
        # net.ifnames=0 biosdevname=0 will be added to GRUB_CMDLINE_LINUX, so the
        # interface name will always be eth*
        ks_config += "# Network eth%d\n" % eth_number
        ks_config += ("rm -f /etc/sysconfig/network-scripts/ifcfg-ens%d\n" %
                      ens_number)
        ks_config += ("cat << EOF > /etc/sysconfig/network-scripts/ifcfg-eth%d\n" %
                      eth_number)
        ks_config += "DEVICE=eth%d\n" % eth_number
        ks_config += 'IPADDR="%s"\n' % network_config["ip"]
        ks_config += 'NETMASK="%s"\n' % network_config["netmask"]
        if "gateway" in network_config:
            ks_config += 'GATEWAY=\"%s"\n' % network_config["gateway"]
        ks_config += """ONBOOT=yes
BOOTPROTO="static"
TYPE=Ethernet
IPV6INIT=no
NM_CONTROLLED=no
EOF
"""
        eth_number += 1
        ens_number += 1

    ks_config += "%end\n"
    local_host_dir = workspace + "/" + hostname
    ret = utils.mkdir(local_host_dir)
    if ret:
        log.cl_error("failed to create directory [%s] on local host",
                     local_host_dir)
        return -1

    ks_fname = "%s.ks" % hostname
    ks_fpath = local_host_dir + "/" + ks_fname
    with open(ks_fpath, "wt") as fout:
        fout.write(ks_config)

    host_ks_fpath = workspace + "/" + ks_fname
    ret = server_host.sh_send_file(log, ks_fpath, workspace)
    if ret:
        log.cl_error("failed to send file [%s] on local host to "
                     "directory [%s] on host [%s]",
                     ks_fpath, workspace,
                     server_host.sh_hostname)
        return -1

    command = ("virt-install --vcpus=1 --os-type=linux "
               "--hvm --connect=qemu:///system "
               "--accelerate --serial pty -v --nographics --noautoconsole --wait=-1 ")
    command += "--ram=%s " % ram_size
    for network_config in network_configs:
        command += ("--network=%s " % (network_config["virt_install_option"]))
    command += ("--name=%s " % (hostname))
    command += ("--initrd-inject=%s " % (host_ks_fpath))
    disk_index = 0
    for disk_size in disk_sizes:
        disk_path = "%s/%s_%d.img" % (image_dir, hostname, disk_index)
        remove_command = "rm -f %s" % disk_path
        retval = server_host.sh_run(log, remove_command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        command += ("--disk path=%s,bus=%s,size=%s " %
                    (disk_path, bus_type, disk_size))
        disk_index += 1
    command += ("--location %s " % (mnt_path))
    command += ("--disk=%s,device=cdrom,perms=ro " % (iso_path))
    command += ("--extra-args='console=tty0 console=ttyS0,115200n8 "
                "ks=file:/%s'" % (ks_fname))

    if distro == ssh_host.DISTRO_RHEL6:
        install_timeout = 600
    elif distro == ssh_host.DISTRO_RHEL7:
        install_timeout = 1200

    retval = server_host.sh_run(log, command, timeout=install_timeout)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     server_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1


def vm_install(log, workspace, server_host, iso_path, hostname, internet, dns,
               network_configs, image_dir, distro, ram_size, disk_sizes,
               bus_type=cstr.CSTR_BUS_SCSI):
    """
    Install virtual machine from ISO
    """
    # pylint: disable=too-many-arguments,too-many-locals
    # pylint: disable=too-many-return-statements,too-many-statements
    # pylint: disable=too-many-branches
    ret = vm_delete(log, server_host, hostname)
    if ret:
        return -1

    network_config = network_configs[0]
    host_ip = network_config["ip"]
    command = ("ping -c 1 %s" % host_ip)
    retval = server_host.sh_run(log, command)
    if retval.cr_exit_status == 0:
        log.cl_error("IP [%s] is already used by a host", host_ip)
        return -1

    command = ("ping -c 1 %s" % hostname)
    retval = server_host.sh_run(log, command)
    if retval.cr_exit_status == 0:
        log.cl_error("host [%s] is already up", hostname)
        return -1

    mnt_path = mount_iso(log, server_host, iso_path)
    if mnt_path is None:
        log.cl_error("failed to get mnt path of ISO [%s]", iso_path)
        return -1

    ret = _vm_install(log, workspace, bus_type, image_dir, ram_size, disk_sizes,
                      distro, hostname, network_configs, server_host, iso_path,
                      mnt_path)
    if ret:
        log.cl_error("failed install VM")

    command = ("umount %s" % (mnt_path))
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

    command = ("rmdir %s" % (mnt_path))
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

    if ret:
        log.cl_error("quiting because failed to install VM")
        return -1

    ret = server_host.sh_run(log, "which sshpass")
    if ret.cr_exit_status != 0:
        command = ("yum install sshpass -y")
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

    # Remove the record in known_hosts, otherwise ssh will fail
    command = ('sed -i "/%s /d" /root/.ssh/known_hosts' % (host_ip))
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

    # When virt-install finished, the virtual machine starts to reboot
    # so wait a little bit here until the host is up. Need
    # StrictHostKeyChecking=no, otherwise exit code will be 6 (ENOENT)
    expect_stdout = hostname + "\n"
    command = ("sshpass -p password ssh -o StrictHostKeyChecking=no "
               "root@%s hostname" % (host_ip))
    ret = server_host.sh_wait_update(log, command, expect_exit_status=0,
                                     expect_stdout=expect_stdout)
    if ret:
        log.cl_error("failed to wait host [%s] up", hostname)
        return -1

    command = ("sshpass -p password ssh root@%s "
               "\"mkdir /root/.ssh && chmod 600 /root/.ssh\"" % (host_ip))
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

    command = ("sshpass -p password scp /root/.ssh/* root@%s:/root/.ssh" % (host_ip))
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

    vm_host = ssh_host.SSHHost(host_ip)
    command = "> /root/.ssh/known_hosts"
    retval = vm_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     vm_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    command = "hostname"
    retval = vm_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     vm_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    real_hostname = retval.cr_stdout.strip()
    if real_hostname != hostname:
        log.cl_error("wrong hostname, expected [%s], got [%s]",
                     hostname, real_hostname)
        return -1

    if internet:
        ret = vm_host.sh_enable_dns(log, dns)
        if ret:
            log.cl_error("failed to enable dns on host [%s]")
            return -1

        command = "yum install rsync -y"
        retval = vm_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         vm_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

    # Do not check the return status, because the connection could be stopped
    command = "init 0"
    vm_host.sh_run(log, command)

    # Need to wait until VM shut off, otherwise "virsh change-media" won't
    # change the XML file
    ret = utils.wait_condition(log, vm_check_shut_off, (server_host, hostname))
    if ret:
        log.cl_error("failed when waiting host [%s] on [%s] shut off",
                     hostname, server_host.sh_hostname)
        return ret

    # Find the CDROM device
    command = ("virsh domblklist %s --details | grep cdrom | "
               "awk '{print $3}'" % (hostname))
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
    cdroms = retval.cr_stdout.splitlines()
    if len(cdroms) != 1:
        log.cl_error("unexpected cdroms: [%s]",
                     retval.cr_stdout)
        return -1
    cdrom = cdroms[0]

    command = ("virsh change-media %s %s --eject" % (hostname, cdrom))
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

    return 0


def target_index2name(log, target_index, bus_type=cstr.CSTR_BUS_SCSI):
    """
    Return the target name according to index
    0 -> sda
    1 -> sdb
    ...
    """
    ascii_number = ord('a')
    ascii_number += target_index

    prefix = ""
    if bus_type == cstr.CSTR_BUS_VIRTIO:
        prefix = cstr.CSTR_DISK_VIRTIO_PREFIX
    elif bus_type == cstr.CSTR_BUS_IDE:
        prefix = cstr.CSTR_DISK_IDE_PREFIX
    elif bus_type == cstr.CSTR_BUS_SCSI:
        prefix = cstr.CSTR_DISK_SCSI_PREFIX
    else:
        log.cl_error("unsupported bus type [%s], please correct it", bus_type)
        return None

    return prefix + chr(ascii_number)


def lvirt_parse_sharedisks_configs(log, shared_disk_configs, shared_disks,
                                  hosts, config_fpath):
    """
    Parse shared disk configs.
    """
    if shared_disk_configs is None or len(shared_disk_configs) == 0:
        return -1

    for shared_disk_config in shared_disk_configs:
        disk_id = utils.config_value(shared_disk_config, cstr.CSTR_DISK_ID)
        if disk_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_DISK_ID, config_fpath)
            return -1

        size = utils.config_value(shared_disk_config, cstr.CSTR_SIZE)
        if size is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_SIZE, config_fpath)
            return -1

        server_host_id = utils.config_value(shared_disk_config, cstr.CSTR_SERVER_HOST_ID)
        if server_host_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_SERVER_HOST_ID, config_fpath)
            return -1
        if server_host_id not in hosts:
            log.cl_error("SSH host with ID [%s] is NOT configured in "
                         "[%s], please correct file [%s]",
                         cstr.CSTR_SERVER_HOST_ID, cstr.CSTR_SSH_HOSTS,
                         config_fpath)
            return -1

        server_host = hosts[server_host_id]

        image_file = utils.config_value(shared_disk_config, cstr.CSTR_IMAGE_FILE)
        if image_file is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_IMAGE_FILE, config_fpath)
            return -1

        shared_disk = SharedDisk(disk_id, server_host, server_host_id, image_file, size)
        shared_disks[disk_id] = shared_disk

    return 0


def lvirt_vm_reboot(log, host, hostserver):
    """
    Reset the guest vm on hostserver
    """
    # pylint: disable=too-many-return-statements,too-many-locals
    # pylint: disable=too-many-branches,too-many-statements
    ret = host.sh_reboot(log)
    if ret == 0:
        return 0
    # reboot failed? try hard reset
    command = "virsh reset %s" % host.sh_hostname
    retval = hostserver.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command [%s] on host [%s], ",
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command, hostserver.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout, retval.cr_stderr)
        return -1

    # wait for the host up after hard reset
    if host.sh_wait_up(log):
        log.cl_error("host [%s] failed to startup, even after hard reset.",
                     host.sh_hostname)
        return -1
    return 0


def parse_templates_config(log, workspace, config, config_fpath, hosts=None):
    """
    Parse the template configurations
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    template_configs = utils.config_value(config, cstr.CSTR_TEMPLATES)
    if template_configs is None:
        log.cl_error("no section [%s] found in configuration file [%s]",
                     cstr.CSTR_TEMPLATES, config_fpath)
        return None

    templates = {}
    for template_config in template_configs:
        template_hostname = utils.config_value(template_config,
                                               cstr.CSTR_HOSTNAME)
        if template_hostname is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_HOSTNAME, config_fpath)
            return None

        internet = utils.config_value(template_config,
                                      cstr.CSTR_INTERNET)
        if internet is None:
            internet = False
            log.cl_debug("no [%s] is configured, will "
                         "not add internet support", cstr.CSTR_INTERNET)

        if internet:
            dns = utils.config_value(template_config, cstr.CSTR_DNS)
            if dns is None:
                log.cl_error("no [%s] is configured, when internet support "
                             "is enabled, please correct file [%s]",
                             cstr.CSTR_DNS, config_fpath)
                return None

        ram_size = utils.config_value(template_config, cstr.CSTR_RAM_SIZE)
        if ram_size is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_RAM_SIZE, config_fpath)
            return None

        disk_sizes = utils.config_value(template_config,
                                        cstr.CSTR_DISK_SIZES)
        if disk_sizes is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_DISK_SIZES, config_fpath)
            return None

        bus_type = utils.config_value(template_config,
                                      cstr.CSTR_BUS_TYPE)
        if bus_type is None:
            log.cl_info("no [%s] is configured, use scsi as default",
                        cstr.CSTR_BUS_TYPE)
            bus_type = cstr.CSTR_BUS_SCSI

        network_configs = utils.config_value(template_config,
                                             cstr.CSTR_NETWORK_CONFIGS)
        if network_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_NETWORK_CONFIGS, config_fpath)
            return None

        iso = utils.config_value(template_config, cstr.CSTR_ISO)
        if iso is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_ISO, config_fpath)
            return None

        distro = utils.config_value(template_config, cstr.CSTR_DISTRO)
        if distro is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_DISTRO, config_fpath)
            return None

        image_dir = utils.config_value(template_config, cstr.CSTR_IMAGE_DIR)
        if image_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_IMAGE_DIR, config_fpath)
            return None

        if hosts is None:
            server_host_id = None
            server_host = None
            reinstall = None
        else:
            server_host_id = utils.config_value(template_config,
                                                cstr.CSTR_SERVER_HOST_ID)
            if server_host_id is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_SERVER_HOST_ID, config_fpath)
                return None

            if server_host_id not in hosts:
                log.cl_error("SSH host with ID [%s] is NOT configured in "
                             "[%s], please correct file [%s]",
                             cstr.CSTR_SERVER_HOST_ID, cstr.CSTR_SSH_HOSTS,
                             config_fpath)
                return None

            server_host = hosts[server_host_id]
            command = "mkdir -p %s" % workspace
            retval = server_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             server_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return None

            reinstall = utils.config_value(template_config,
                                           cstr.CSTR_REINSTALL)
            if reinstall is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_REINSTALL, config_fpath)
                return None

        template = VirtTemplate(iso, template_hostname, internet,
                                network_configs, image_dir, distro, ram_size,
                                disk_sizes, dns,
                                bus_type=bus_type,
                                server_host=server_host,
                                server_host_id=server_host_id,
                                reinstall=reinstall)
        templates[template_hostname] = template
    return templates


def lvirt_vm_install(log, workspace, config, config_fpath):
    """
    Start to install virtual machine
    """
    # pylint: disable=too-many-return-statements,too-many-locals
    # pylint: disable=too-many-branches,too-many-statements
    ssh_host_configs = utils.config_value(config, cstr.CSTR_SSH_HOSTS)
    if ssh_host_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_SSH_HOSTS, config_fpath)
        return -1

    hosts = {}
    for host_config in ssh_host_configs:
        host_id = host_config[cstr.CSTR_HOST_ID]
        if host_id is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_HOST_ID, config_fpath)
            return -1

        hostname = utils.config_value(host_config, cstr.CSTR_HOSTNAME)
        if hostname is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "with ID [%s], please correct file [%s]",
                         cstr.CSTR_HOSTNAME, host_id, config_fpath)
            return -1

        ssh_identity_file = utils.config_value(host_config, cstr.CSTR_SSH_IDENTITY_FILE)

        if host_id in hosts:
            log.cl_error("multiple SSH hosts with the same ID [%s], please "
                         "correct file [%s]", host_id, config_fpath)
            return -1
        host = ssh_host.SSHHost(hostname, ssh_identity_file)
        hosts[host_id] = host

    kvm_template_dict = parse_templates_config(log, workspace, config, config_fpath, hosts=hosts)
    if kvm_template_dict is None:
        log.cl_error("failed to parse the config of templates")
        return -1

    for template in kvm_template_dict.values():
        iso = template.vt_iso
        template_hostname = template.vt_template_hostname
        internet = template.vt_internet
        network_configs = template.vt_network_configs
        image_dir = template.vt_image_dir
        distro = template.vt_distro
        ram_size = template.vt_ram_size
        disk_sizes = template.vt_disk_sizes
        bus_type = template.vt_bus_type
        server_host = template.vt_server_host
        reinstall = template.vt_reinstall
        dns = template.vt_dns

        state = server_host.sh_virsh_dominfo_state(log, template_hostname)
        if not reinstall and state is not None:
            log.cl_debug("skipping reinstall of template [%s] according to config",
                         template_hostname)
            continue

        ret = vm_install(log, workspace, server_host, iso, template_hostname,
                         internet, dns, network_configs, image_dir, distro,
                         ram_size, disk_sizes, bus_type)
        if ret:
            log.cl_error("failed to create virtual machine template [%s]",
                         template_hostname)
            return -1

    shared_disks = {}
    shared_disk_configs = utils.config_value(config, cstr.CSTR_SHARED_DISKS)
    if shared_disk_configs is None:
        log.cl_info("can NOT find [%s] in the config file [%s], "
                    "ignore it.",
                    cstr.CSTR_SHARED_DISKS, config_fpath)
    else:
        ret = lvirt_parse_sharedisks_configs(log, shared_disk_configs,
                                            shared_disks, hosts,
                                            config_fpath)
        if ret:
            log.cl_error("failed to parse [%s] in the config file [%s], "
                         "please correct it.",
                         cstr.CSTR_SHARED_DISKS, config_fpath)
            return -1

    vm_host_configs = utils.config_value(config, cstr.CSTR_VM_HOSTS)
    if vm_host_configs is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VM_HOSTS, config_fpath)
        return -1

    vm_hosts = []
    hosts_servers_mapping = dict()
    hosts_string = ""
    for vm_host_config in vm_host_configs:
        hostname = utils.config_value(vm_host_config, cstr.CSTR_HOSTNAME)
        if hostname is None:
            log.cl_error("no [hostname] is configured for a vm_host, "
                         "please correct file [%s]", config_fpath)
            return -1

        ips = utils.config_value(vm_host_config, cstr.CSTR_HOST_IPS)
        if ips is None:
            log.cl_error("no [%s] is configured for a vm_host, "
                         "please correct file [%s]", cstr.CSTR_HOST_IPS,
                         config_fpath)
            return -1

        template_hostname = utils.config_value(vm_host_config,
                                               cstr.CSTR_TEMPLATE_HOSTNAME)
        if template_hostname is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_TEMPLATE_HOSTNAME, config_fpath)
            return -1

        if template_hostname not in kvm_template_dict:
            log.cl_error("template with hostname [%s] is NOT configured in "
                         "[%s], please correct file [%s]",
                         template_hostname, cstr.CSTR_TEMPLATES, config_fpath)
            return -1

        template = kvm_template_dict[template_hostname]

        reinstall = utils.config_value(vm_host_config, cstr.CSTR_REINSTALL)
        state = template.vt_server_host.sh_virsh_dominfo_state(log, hostname)
        if reinstall is None:
            reinstall = False
        if state is None:
            reinstall = True

        if not reinstall:
            ret = vm_start(log, workspace,
                           template.vt_server_host,
                           hostname,
                           template.vt_network_configs,
                           ips,
                           template.vt_template_hostname,
                           template.vt_image_dir,
                           template.vt_distro,
                           template.vt_internet,
                           len(template.vt_disk_sizes))
            if ret:
                log.cl_error("virtual machine [%s] can't be started",
                             hostname)
                return -1
        else:
            ret = vm_clone(log, workspace,
                           template.vt_server_host,
                           hostname,
                           template.vt_network_configs,
                           ips,
                           template.vt_template_hostname,
                           template.vt_image_dir,
                           template.vt_distro,
                           template.vt_internet,
                           len(template.vt_disk_sizes))
            if ret:
                log.cl_error("failed to create virtual machine [%s] based on "
                             "template [%s]", hostname,
                             template.vt_template_hostname)
                return -1

        host_ip = ips[0]
        vm_host = lustre.LustreServerHost(hostname)
        hosts_string += ("%s %s\n" % (host_ip, hostname))
        vm_hosts.append(vm_host)
        hosts_servers_mapping[hostname] = template.vt_server_host
        shared_disk_ids = utils.config_value(vm_host_config,
                                             cstr.CSTR_SHARED_DISK_IDS)
        if shared_disk_ids is None or shared_disk_configs is None:
            continue

        command = ("> %s" % LVIRT_UDEV_RULES)
        retval = vm_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         server_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        target_index = 0
        for shared_disk_id in shared_disk_ids:
            if shared_disk_id not in shared_disks:
                log.cl_error("shared disk with ID [%s] is not configured, "
                             "please correct file [%s]",
                             shared_disk_id, config_fpath)
                return -1

            shared_disk = shared_disks[shared_disk_id]

            if template.vt_server_host_id != shared_disk.sd_server_host_id:
                log.cl_error("Shared disk with ID [%s] is not configured "
                             "on host with ID [%s]. It is on host with ID "
                             "[%s] instead, thus can't share it on VM [%s]. "
                             "Please correct file [%s].",
                             shared_disk_id, template.vt_server_host_id,
                             shared_disk.sd_server_host_id, hostname,
                             config_fpath)
                return -1
            target_name = target_index2name(log, target_index)
            shared_target = SharedTarget(vm_host, target_name)
            shared_disk.sd_add_target(shared_target)
            target_index += 1

    host_configs = utils.config_value(config, cstr.CSTR_HOSTS)
    if host_configs is not None:
        for host_config in host_configs:
            hostname = utils.config_value(host_config, cstr.CSTR_HOSTNAME)
            if hostname is None:
                log.cl_debug("can NOT find [%s] in the config file, "
                             "please correct file [%s]",
                             cstr.CSTR_HOSTNAME, config_fpath)
                continue

            host_ip = utils.config_value(host_config, cstr.CSTR_IP)
            if host_ip is None:
                log.cl_debug("can NOT find [%s] in the config file, "
                             "please correct file [%s]",
                             cstr.CSTR_IP, config_fpath)
                continue
            hosts_string += ("%s %s\n" % (host_ip, hostname))
    else:
        log.cl_debug("can NOT find [%s] in the config file [%s], "
                     "ignore it",
                     cstr.CSTR_HOSTS, config_fpath)

    hosts_fpath = workspace + "/hosts"
    with open(hosts_fpath, "wt") as hosts_file:
        with open("/etc/hosts") as local_hosts:
            for line in local_hosts:
                hosts_file.write(line)

        hosts_file.write(hosts_string)
        hosts_file.flush()

    for host in vm_hosts:
        # Cleanup log dirs, as previous clownfish testing may generate
        # lots of logs.
        command = "rm -rf /var/log/lvirt*"
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

        command = "rm -rf /var/log/clownfish*"
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

        log.cl_info("preparing virtual machine [%s] after starting it",
                    host.sh_hostname)
        ret = host.sh_send_file(log, hosts_fpath, "/etc")
        if ret:
            log.cl_error("failed to send hosts file [%s] on local host to "
                         "directory [%s] on host [%s]",
                         hosts_fpath, workspace,
                         host.sh_hostname)
            return -1

        # Clear the known_hosts, otherwise the reinstalled hosts can't be
        # accessed by other hosts
        command = "> /root/.ssh/known_hosts"
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

    # Stop Corosync to kill all possible Clownfish server
    for host in vm_hosts:
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

    # umount all Lustre clients first
    reboot_hosts = []
    for host in vm_hosts:
        ret = host.lsh_lustre_umount_services(log, client_only=True)
        if ret:
            log.cl_info("failed to umount Lustre clients on host [%s], "
                        "reboot is needed", host.sh_hostname)
            reboot_hosts.append(host)

    # umount all Lustre servers
    for host in vm_hosts:
        ret = host.lsh_lustre_umount_services(log)
        if ret:
            log.cl_info("failed to umount Lustre servers on host [%s], "
                        "reboot is needed", host.sh_hostname)
            if host not in reboot_hosts:
                reboot_hosts.append(host)

    for host in reboot_hosts:
        ret = lvirt_vm_reboot(log, host,
                             hosts_servers_mapping[host.sh_hostname])
        if ret:
            log.cl_error("failed to reboot host [%s]",
                         host.sh_hostname)
            return -1

    # Destroy all ZFS pool
    for host in vm_hosts:
        ret = host.sh_destroy_zfs_pools(log)
        if ret:
            log.cl_info("failed to destroy ZFS pools on host [%s], "
                        "reboot is needed", host.sh_hostname)
            ret = lvirt_vm_reboot(log, host,
                                 hosts_servers_mapping[host.sh_hostname])
            if ret:
                log.cl_error("failed to reboot host [%s]",
                             host.sh_hostname)
                return -1

            ret = host.sh_destroy_zfs_pools(log)
            if ret:
                log.cl_info("failed to destroy ZFS pools on host [%s] even "
                            "after reboot", host.sh_hostname)
                return -1

    # Cleanup all shared disk first
    for host in vm_hosts:
        hostname = host.sh_hostname
        server_host = hosts_servers_mapping[hostname]
        ret = server_host.sh_virsh_detach_domblks(log, hostname)
        if ret:
            log.cl_error("failed to deatch disks on VM [%s]",
                         hostname)
            return -1

    for shared_disk in shared_disks.values():
        ret = shared_disk.sd_share(log)
        if ret:
            log.cl_error("failed to share disk [%s] on server host with "
                         "ID [%s]", shared_disk.sd_image_fpath,
                         shared_disk.sd_server_host_id)
            return -1
    return 0


def lvirt(log, workspace, config_fpath):
    """
    Start to test holding the confiure lock
    """
    # pylint: disable=too-many-branches,bare-except,too-many-locals
    # pylint: disable=too-many-statements
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
        ret = lvirt_vm_install(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    if ret:
        log.cl_error("failed to install the VMs, please check [%s] for more "
                     "log", workspace)
    else:
        log.cl_info("installed the VMs successfully, please check [%s] for more "
                    "log", workspace)
    return ret


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s <config_file>" %
                 sys.argv[0])


def main():
    """
    Install virtual machines
    """
    cmd_general.main(LVIRT_CONFIG, LVIRT_LOG_DIR,
                     lvirt)
