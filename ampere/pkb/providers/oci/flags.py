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

"""Module containing flags applicable across benchmark run on OCI."""

from absl import flags

VALID_TIERS = ['VM.Standard', 'VM.Optimized']

VALID_SHAPES = ['.A1.Flex', '.A2.Flex', '3.Flex', '.E4.Flex', '.E5.Flex']

flags.DEFINE_string('oci_availability_domain', None, 'The availability domain')

flags.DEFINE_string('oci_fault_domain', None, 'The fault domain')

flags.DEFINE_string('oci_shape', 'VM.Standard.A1.Flex', 'Performance tier to use for the machine type. Defaults to '
                                                        'Standard.')

flags.DEFINE_integer('oci_compute_units', 1, 'Number of compute units to allocate for the machine type')

flags.DEFINE_integer('oci_compute_memory', None, 'Number of memory in gbs to allocate for the machine type')

flags.DEFINE_integer('oci_boot_disk_size', 50, 'Size of Boot disk in GBs')

flags.DEFINE_boolean('oci_use_vcn', True, 'Use in built networking')

flags.DEFINE_integer('oci_num_local_ssds', 0, 'No. of disks')

flags.DEFINE_string(
    'oci_network_name', None, 'The name of an already created '
    'network to use instead of creating a new one.')

flags.DEFINE_string(
    'oci_network_type', None, 'Type of network to be used '
    'example VFIO or paravirtualized')

flags.DEFINE_string(
    'oci_profile', None, 'Default profile to be used')
