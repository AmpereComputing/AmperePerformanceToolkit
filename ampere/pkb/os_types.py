# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import os_types


ORACLE8 = 'oracle8'
ORACLE9 = 'oracle9'


LINUX_OS_TYPES = [
    ORACLE8,
    ORACLE9,
]
os_types.ALL.extend(LINUX_OS_TYPES)
