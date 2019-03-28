# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Yaml
"""
import yaml


class YamlDumper(yaml.Dumper):
    # pylint: disable=too-many-ancestors
    """
    Provide proper indent
    """
    def increase_indent(self, flow=False, indentless=False):
        return super(YamlDumper, self).increase_indent(flow, False)
