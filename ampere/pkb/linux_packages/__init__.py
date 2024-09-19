# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


from perfkitbenchmarker import import_util
from perfkitbenchmarker.linux_packages import PACKAGES


def _LoadPackages():
  packages = dict([(module.PACKAGE_NAME, module) for module in
                   import_util.LoadModulesForPath(__path__, __name__)])
  return packages


AMPERE_PKB_PACKAGES = _LoadPackages()
PACKAGES.update(AMPERE_PKB_PACKAGES)
