"""
Install python RPMs for clownfish_install to work properly first
"""
# Local libs
import sys
from pylustre import ssh_host
from pylustre import install_common
from pylustre import clog
from pylustre import constants


def main():
    """
    Install Clownfish
    """
    # pylint: disable=unused-variable
    log = clog.get_log()
    missing_dependencies = []

    try:
        import yaml
    except ImportError:
        missing_dependencies.append("PyYAML")

    try:
        import filelock
    except ImportError:
        missing_dependencies.append("python2-filelock")

    try:
        import dateutil
    except ImportError:
        missing_dependencies.append("python-dateutil")

    local_host = ssh_host.SSHHost("localhost", local=True)
    for dependent_rpm in install_common.CLOWNFISH_INSTALL_DEPENDENT_RPMS:
        ret = local_host.sh_rpm_query(log, dependent_rpm)
        if ret != 0:
            missing_dependencies.append(dependent_rpm)

    if len(missing_dependencies):
        log.cl_info("installing dependency RPMs of %s",
                    missing_dependencies)
        ret = install_common.dependency_install(log, local_host,
                                                constants.CLOWNFISH_INSTALL_CONFIG,
                                                missing_dependencies, "Clownfish",
                                                "clownfish-*.iso")
        if ret:
            log.cl_error("not able to install Clownfish because some depdendency "
                         "RPMs are missing and not able to be installed: %s",
                         missing_dependencies)
            sys.exit(-1)
    from pyclownfish import clownfish_install_nodeps
    clownfish_install_nodeps.main()
