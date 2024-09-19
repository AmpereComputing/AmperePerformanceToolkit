# Copyright (c) 2024, Ampere Computing LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import posixpath
from jinja2 import Environment, FileSystemLoader
from perfkitbenchmarker import data
from perfkitbenchmarker.linux_virtual_machine import BaseLinuxVirtualMachine
from perfkitbenchmarker import vm_util
from typing import Any, List, Dict


def _fill_template(template_name: str, render_args: Dict) -> str:
    """
    Fills a given template with all arguments specified
    Saves rendered template as bash script in /tmp/perfkitbenchmarker/runs/<run_uri>
        
    Returns: local path to bash script rendered by template
    """
    environment = Environment(loader=FileSystemLoader(data.ResourcePath("./ampere/pkb/templates")))
    template = environment.get_template(template_name)
    content = template.render(**render_args)
    outfile = f"{vm_util.GetTempDir()}/{template_name.strip('.j2')}"
    with open(outfile, "w", encoding="utf-8") as f:
        f.write(content)
    logging.debug(f"Script template {template_name} rendered at local path: {outfile}")
    return outfile


def render_and_copy_to_vm(vm: BaseLinuxVirtualMachine, deploy_dir: str, template_name: str, render_args: Dict) -> str:
    """
    Calls helper function _fill_template()
    Pushes rendered template to VM at specified directory
    
    Returns: remote path to bash script rendered by template
    """
    outfile = _fill_template(template_name, render_args)
    vm.PushFile(outfile, deploy_dir)
    deploy_path = posixpath.join(deploy_dir, os.path.basename(outfile))
    vm.RemoteCommand(f"sudo chmod +x {deploy_path}")
    logging.debug(f"Rendered script copied to VM at remote path: {deploy_path}")
    return deploy_path

