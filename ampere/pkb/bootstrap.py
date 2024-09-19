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

"""
Centralized place for Ampere PerfkitBenchmarker to Bootstrap onto Perfkitbenchmarker
"""
from shutil import which
from typing import Any

from absl import flags
from dotenv import load_dotenv, find_dotenv
from perfkitbenchmarker import events

from .events import register as register_events

# Load all Ampere Specific
from . import linux_benchmarks, linux_packages, os_types, systems, providers


FLAGS = flags.FLAGS


def bootstrap():
    """
    Bootstrap Perfkitbenchmarker with Ampere
    """
    load_dotenv(find_dotenv())
    register_events()
    events.initialization_complete.connect(register_flags)


def register_flags(_: Any, parsed_flags: flags.FLAGS):
    return
