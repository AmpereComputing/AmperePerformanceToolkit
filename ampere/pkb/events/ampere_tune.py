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
from typing import Any, List

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_virtual_machine import BaseLinuxVirtualMachine
from perfkitbenchmarker.benchmark_spec import BenchmarkSpec

from ampere.pkb.common import download_utils
from ampere.pkb.utils import bash_template


FLAGS = flags.FLAGS

flags.DEFINE_list(f"ampere_tune_global", None, help="Tunings to apply globally on all systems")
flags.DEFINE_list(f"ampere_tune_servers", None, help="Tunings to apply to server(s)")
flags.DEFINE_list(f"ampere_tune_clients", None, help="Tunings to apply to client(s)")


def register_all(_: Any, parsed_flags: flags.FLAGS):
    events.before_phase.connect(before_phase, weak=False)


def before_phase(sender: Any, benchmark_spec: BenchmarkSpec):
    if sender != stages.PREPARE:
        return

    # Get 3 types of tunings
    global_tunings = FLAGS[f"ampere_tune_global"].value
    server_tunings = FLAGS[f"ampere_tune_servers"].value
    client_tunings = FLAGS[f"ampere_tune_clients"].value

    if not any((global_tunings, server_tunings, client_tunings)):
        return

    # Get named "clients" and "servers" (None if they don't exist)
    client_vms = benchmark_spec.vm_groups.get("clients")
    server_vms = benchmark_spec.vm_groups.get("servers")

    # Apply global tunings to all VMs in vm_groups (regardless of name)
    if global_tunings:
        for group_type, group_vms in benchmark_spec.vm_groups.items():
            _run_tuning_list(global_tunings, group_vms, group_type, "global")
    # Only apply server/client tunings if the vm group exists
    if server_tunings and server_vms:
        _run_tuning_list(server_tunings, server_vms, "servers", "servers")
    if client_tunings and client_vms:
        _run_tuning_list(client_tunings, client_vms, "clients", "clients")


def _run_tuning_list(tuning_list, vm_list, vm_list_type, tuning_type):
    """Helper function writes a list of tunings to an executable script on a given list of VMs"""
    for vm_idx, vm in enumerate(vm_list):
        # Deploy tuning script to 
        #   /opt/pkb/ampere_tune_global_servers0
        #   /opt/pkb/ampere_tune_global_clients0
        #   /opt/pkb/ampere_tune_servers_servers0
        #   /opt/pkb/ampere_tune_clients_clients0
        deploy_dir = f"{download_utils.INSTALL_DIR}/ampere_tune_{tuning_type}_{vm_list_type}{vm_idx}"
        vm.RemoteCommand(f"mkdir -p {deploy_dir}")

        # Detect if "NIC" is present for current vm type, format and update tuning_list accordingly
        #   -   Catch cases where global tunings are applied to named groups 
        #       "servers"/"clients" and there are no NICs specified
        current_nic = None
        if vm_list_type == "servers":
            all_server_nics = FLAGS[f"ampere_server_nics"].value
            if all_server_nics:
                current_nic = all_server_nics[vm_idx]
        if vm_list_type == "clients":
            all_client_nics = FLAGS[f"ampere_client_nics"].value
            if all_client_nics:
                current_nic = all_client_nics[vm_idx]
        tuning_list = [tuning.format(NIC=current_nic) if "NIC" in tuning else tuning for tuning in tuning_list]

        # Fill bash template with each line from tuning list
        render_args = {
            "tuning_list": tuning_list
        }
        deploy_path = bash_template.render_and_copy_to_vm(vm, deploy_dir, f"{tuning_type}_tune.sh.j2", render_args)
        log_path = f"{deploy_dir}/{tuning_type}_tune.log"
        # Execute bash script (ignore failed tunings) and redirect to log
        vm.RemoteCommand(f"sudo {deploy_path} &> {log_path}", ignore_failure=True)
        # Copy deploy directory back to /tmp/perfkitbenchmarker/runs/<run_uri> on the runner
        vm.PullFile(vm_util.GetTempDir(), deploy_dir)

