# Copyright (c) 2017 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Generate clownfish_test.conf
"""
import sys
import yaml

from pylcommon import utils
from pylcommon import clog
from pylcommon import cstr
from pylcommon import lvirt
from pylcommon import lyaml
from pylcommon import constants
from pyclownfish import clownfish_test


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s config_file" %
                 sys.argv[0])


def main():
    """
    Generate clownfish_test.conf
    """
    # pylint: disable=bare-except
    log = clog.get_log()
    reload(sys)
    sys.setdefaultencoding("utf-8")

    if len(sys.argv) != 2:
        usage()
        sys.exit(-1)

    config_fpath = sys.argv[1]
    config = {}
    config[cstr.CSTR_VIRT_CONFIG] = lvirt.LVIRT_CONFIG
    config[cstr.CSTR_SKIP_VIRT] = False
    config[cstr.CSTR_SKIP_INSTALL] = False
    config[cstr.CSTR_INSTALL_CONFIG] = constants.CLOWNFISH_INSTALL_CONFIG
    install_server = {}
    install_server[cstr.CSTR_HOSTNAME] = "install_host"
    install_server[cstr.CSTR_SSH_IDENTITY_FILE] = "/root/.ssh/id_dsa"
    config[cstr.CSTR_INSTALL_SERVER] = install_server
    tests = []
    for test_funct in clownfish_test.CLOWNFISH_TESTS:
        tests.append(test_funct.__name__)
    config[cstr.CSTR_ONLY_TESTS] = tests
    config_string = ("""#
# Configuration file for testing Clownfish from DDN
#
# Please comment the test names under "%s" if want to skip some tests
#
# Please set "%s" to true if Clownfish is already installed and
# properly running.
#
# Please set "%s" to true if the virtual machines are already
# installed and properly running.
#
""" % (cstr.CSTR_ONLY_TESTS, cstr.CSTR_SKIP_INSTALL, cstr.CSTR_SKIP_VIRT))
    config_string += yaml.dump(config, Dumper=lyaml.YamlDumper,
                               default_flow_style=False)
    try:
        with open(config_fpath, 'w') as yaml_file:
            yaml_file.write(config_string)
    except:
        log.cl_error("""Failed to save the config file. To avoid data lose, please save the
following config manually:""")
        sys.stdout.write(config_string)
        sys.exit(-1)
    log.cl_info("Config file saved to file [%s]", config_fpath)
    sys.exit(0)
