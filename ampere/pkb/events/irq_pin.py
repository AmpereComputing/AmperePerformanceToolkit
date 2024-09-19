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
import posixpath
from typing import Any, List 

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker.linux_virtual_machine import BaseLinuxVirtualMachine
from perfkitbenchmarker.benchmark_spec import BenchmarkSpec

from ampere.pkb.common import download_utils

FLAGS = flags.FLAGS

flags.DEFINE_bool(f'ampere_irq_pin', False, help='Enable IRQ pinning')
flags.DEFINE_list(f'ampere_server_nics', None, help='List of NIC names for each server (SUT)')
flags.DEFINE_list(f'ampere_client_nics', None, help='List of NIC names for each client')
flags.DEFINE_list(f'ampere_server_irq_cores', None, 
                    f'Used to override default IRQ binding on each server in network tuning step. '
                    f'Can be a single range or multiple comma-separated ranges. '
                    f'e.g. 0-7 or 0-7,80-87')
flags.DEFINE_list(f'ampere_client_irq_cores', None, 
                    f'Used to override default IRQ binding on each client in network tuning step. '
                    f'Can be a single range or multiple comma-separated ranges. '
                    f'e.g. 0-7 or 0-7,80-87')
flags.DEFINE_list('ampere_server_irq_list', None, 'List of space-separated server IRQs to bind.')
flags.DEFINE_list('ampere_client_irq_list', None, 'List of space-separated client IRQs to bind.')


def register_all(_: Any, parsed_flags: flags.FLAGS):
    events.after_phase.connect(after_phase, weak=False)


def after_phase(sender: Any, benchmark_spec: BenchmarkSpec):
    if not FLAGS.ampere_irq_pin:
        return
    if sender == stages.PREPARE:
        client_vms = benchmark_spec.vm_groups['clients']
        server_vms = benchmark_spec.vm_groups['servers']
        return perform_irq_pin(client_vms, server_vms)


def perform_irq_pin(client_vms: List[BaseLinuxVirtualMachine], server_vms: List[BaseLinuxVirtualMachine]):
    # Get flag values
    server_nics = FLAGS[f'ampere_server_nics'].value
    client_nics = FLAGS[f'ampere_client_nics'].value
    server_irq_cores = FLAGS[f'ampere_server_irq_cores'].value 
    client_irq_cores = FLAGS[f'ampere_client_irq_cores'].value 
    server_irq_list = FLAGS['ampere_server_irq_list'].value
    client_irq_list = FLAGS['ampere_client_irq_list'].value

    # VM count must be equal to no. of NICs specified and no. of core ranges specified
    if len(server_vms) != len(server_nics) or len(server_vms) != len(server_irq_cores):
        raise ValueError(f'Number of server VMs must equal number of server NICs and number of server IRQ core ranges!')
    if len(client_vms) != len(client_nics) or len(client_vms) != len(client_irq_cores):
        raise ValueError(f'Number of client VMs must equal number of client NICs and number of client IRQ core ranges!')

    # Iterate over servers and clients
    for server_idx, server_vm in enumerate(server_vms):
        server_nic = server_nics[server_idx]
        server_irq_core_range = server_irq_cores[server_idx]
        server_irqs = server_irq_list[server_idx] if server_irq_list else ""
        # Prepare and run IRQ pinning on all servers
        server_deploy = _prepare_irq_pin(server_vm, server_nic)
        _execute_irq_pin(server_vm, server_nic, server_irq_core_range, server_deploy, server_irqs)
    for client_idx, client_vm in enumerate(client_vms):
        client_nic = client_nics[client_idx]
        client_irq_core_range = client_irq_cores[client_idx]
        client_irqs = client_irq_list[client_idx] if client_irq_list else ""
        # Prepare and run IRQ pinning on all clients
        client_deploy = _prepare_irq_pin(client_vm, client_nic)
        _execute_irq_pin(client_vm, client_nic, client_irq_core_range, client_deploy, client_irqs)


def _prepare_irq_pin(vm, nic):
    """Sends all required scripts for IRQ pinning 
    From local runner:
        ./ampere/pkb/data/
    To remote system:
        /opt/pkb/irq_pin/
    Requires either the server_nic or client_nic to be specified.
    Returns:
        deploy_dir where all scripts live on the remote system
    """
    if not nic:
        return
    local_path = './ampere/pkb/data/'
    all_scripts = [
        'common_irq_affinity.sh',
        'set_irq_affinity_cpulist.sh',
    ]
    dir_name = 'irq_pin'
    deploy_dir = posixpath.join(download_utils.INSTALL_DIR, dir_name)
    vm.RemoteCommand(f'mkdir -p {deploy_dir}')
    [vm.PushFile(data.ResourcePath(f'{local_path}' + script), deploy_dir) for script in all_scripts]
    vm.RemoteCommand(f'sudo chmod +x {deploy_dir}/*')
    return deploy_dir


def _execute_irq_pin(vm, nic, irq_core_range, deploy_dir, irq_list):
    """Executes irq pinning script on system"""
    cmd = f'sudo bash set_irq_affinity_cpulist.sh {irq_core_range} {nic} {irq_list}'
    stdout, stderr = vm.RemoteCommand(f'cd {deploy_dir} && {cmd}')
    if stderr:
        logging.debug(f'Failure: IRQ pinning failed: {cmd}')
    else:
        logging.debug(f'Success: IRQ pinning succeeded: {cmd}')

