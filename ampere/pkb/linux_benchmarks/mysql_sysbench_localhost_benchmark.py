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

"""Sysbench Benchmark.

This is a set of benchmarks that measures performance of Sysbench Databases on
  managed MySQL or Postgres.

As other cloud providers deliver a managed MySQL service, we will add it here.
"""
import logging
import functools

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import linux_virtual_machine
from ampere.pkb.linux_benchmarks import mysql_sysbench_benchmark

FLAGS = flags.FLAGS
_LinuxVM = linux_virtual_machine.BaseLinuxVirtualMachine

BENCHMARK_NAME = "ampere_mysql_sysbench_localhost"

# The default values for flags and BENCHMARK_CONFIG are not a recommended
# configuration for comparing sysbench performance.  Rather these values
# are set to provide a quick way to verify functionality is working.
# A broader set covering different permuations on much larger data sets
# is prefereable for comparison.

BENCHMARK_CONFIG = """
ampere_mysql_sysbench_localhost:
  description: Benchmark Mysql using mysql-sysbench
  vm_groups:
    servers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""
    

# Ensure localhost flag is set for any phase of the benchmark
FLAGS["ampere_mysql_sysbench_localhost"].value = True


def GetConfig(user_config):
    """Get User Config"""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """Prepare the MySQL DB Instances and sysbench on same machine, configures it.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    mysql_sysbench_benchmark.Prepare(benchmark_spec)


def Run(benchmark_spec):
    """Run the sysbench benchmark and publish results.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.

    Returns:
      Results.
    """
    results = mysql_sysbench_benchmark.Run(benchmark_spec)
    return results


def Cleanup(benchmark_spec) -> None:
    """Cleanup Nginx and load generators.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    mysql_sysbench_benchmark.Cleanup(benchmark_spec)
