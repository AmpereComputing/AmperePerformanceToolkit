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

from perfkitbenchmarker import events
from . import ampere_tune, irq_pin, provision_disk


def register():
    """
    Register any event handling for Ampere Namespace code

    Current Event Pipeline (COMMENT MUST REFLECT CODE ACCURATELY)

    - Provision
        - Before Provision
            - benchmark_spec: Collect
        - Benchmark Start
            - ensure_booted
        - After Provision
            - ampere_kernel: Install
            - nvparam
            - reboot
    - Prepare
        - Before Prepare
            - kernel placement
        - After Prepare
            - ampere_openocd: Set Before Registers
            - amp_sys_dump: Collect
            - lm_sensors: Setup
            - network_tune
    - Run
        - Before Run
            - lm_sensors: Start
            - redfish
            - sel: Start
            - sysstat: Start
            - turbostat: Start
            - uart: Start
        - After Run (Run on Exception)
            - ensure_booted
            - lm_sensors: Stop
            - redfish
            - sel: Stop
            - sysstat: Stop
            - turbostat: Stop
            - uart: Stop
    - Cleanup
        - Before Cleanup
            - ensure_booted
        - After Cleanup
            - nvparam
            - ampere_kernel: Restore
            - ampere_openocd: Set After Registers
    - Teardown
        - Before Teardown
        - After Teardown
    - Benchmark End
        - ensure_booted
    """
    events.initialization_complete.connect(ampere_tune.register_all)
    events.initialization_complete.connect(irq_pin.register_all)
    events.initialization_complete.connect(provision_disk.register_all)

