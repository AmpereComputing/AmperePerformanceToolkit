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

from perfkitbenchmarker.linux_virtual_machine import BaseRhelMixin
from perfkitbenchmarker.static_virtual_machine import StaticVirtualMachine

from ampere.pkb import os_types

_ORACLE_EPEL_URL = 'oracle-epel-release-el9'
_ORACLE_EPEL_RELEASE = 'https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm'
_ORACLE_CRB_PATH = '/usr/bin/crb '

class Oracle9Mixin(BaseRhelMixin):
    """Class holding Oracle Linux 9 specific VM methods and attributes."""
    OS_TYPE = os_types.ORACLE9
    PYTHON_2_PACKAGE = None

    def SetupPackageManager(self):
        """Install EPEL."""
        # https://docs.fedoraproject.org/en-US/epel/#_rhel_9
        self.RemoteCommand(f'sudo dnf install -y {_ORACLE_EPEL_URL}')
        self.RemoteCommand(f'sudo dnf install {_ORACLE_EPEL_RELEASE} -y && sudo {_ORACLE_CRB_PATH} enable')


class Oracle9BasedStaticVirtualMachine(StaticVirtualMachine,
                                        Oracle9Mixin):
    pass


