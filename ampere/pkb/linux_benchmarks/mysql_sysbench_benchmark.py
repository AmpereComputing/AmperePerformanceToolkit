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
import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import linux_virtual_machine
from ampere.pkb.linux_packages import mysql80
from ampere.pkb.linux_packages import sysbench

FLAGS = flags.FLAGS
_LinuxVM = linux_virtual_machine.BaseLinuxVirtualMachine

BENCHMARK_NAME = "ampere_mysql_sysbench"
PGO_OPTION = flags.DEFINE_bool(
    f"{BENCHMARK_NAME}_pgo", False, "If true, run point select with PGO"
)

localhost_option = flags.DEFINE_bool(
    f"{BENCHMARK_NAME}_localhost",
    False,
    "If true, run mysql and sysbench on same machine",
)

mysql_latency_capped_throughput = flags.DEFINE_bool(
    f"{BENCHMARK_NAME}_latency_capped_throughput",
    False,
    "Measure latency capped throughput. Use in conjunction with "
    "mysql_latency_cap. Defaults to False. ",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_thread_lower_bound",
    0,
    "Use with max throughput mode, defaults to 0.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_thread_upper_bound",
    25,
    "Use with max throughput mode, defaults to 20.",
)

mysql_latency_cap = flags.DEFINE_float(
    f"{BENCHMARK_NAME}_latency_cap",
    6.0,
    "Latency cap in ms. Use in conjunction with "
    "latency_capped_throughput. Defaults to 1ms.",
)


flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_threads_incr",
    1,
    "increment threads by this number for throughput mode",
)

# The default values for flags and BENCHMARK_CONFIG are not a recommended
# configuration for comparing sysbench performance.  Rather these values
# are set to provide a quick way to verify functionality is working.
# A broader set covering different permuations on much larger data sets
# is prefereable for comparison.

BENCHMARK_CONFIG = """
ampere_mysql_sysbench:
  description: Benchmark Mysql using mysql-sysbench
  vm_groups:
    servers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    clients:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
    """Get User Config"""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """Prepare the MySQL DB Instances, configures it.

       Prepare the client test VM, installs SysBench, configures it.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    # We would like to always cleanup server side states.
    # If we don't set this, our cleanup function will only be called when the VM
    # is static VM, but we have server side states to cleanup regardless of the
    # VM type.
    benchmark_spec.always_call_cleanup = True
    mysql_vms = benchmark_spec.vm_groups["servers"][0]
    if localhost_option.value:
        server_partials = [
            functools.partial(_PrepareServer, mysql_vm) for mysql_vm in [mysql_vms]
        ]
        background_tasks.RunThreaded((lambda f: f()), server_partials)
        mysql_vms.Install(sysbench.PACKAGE_NAME)
        sysbench.Configure(mysql_vms, mysql_vms)
    else:
        clients = benchmark_spec.vm_groups["clients"]
        server_partials = [
            functools.partial(_PrepareServer, mysql_vm) for mysql_vm in [mysql_vms]
        ]
        client_partials = [
            functools.partial(_PrepareClient, client) for client in clients
        ]
        background_tasks.RunThreaded((lambda f: f()), server_partials + client_partials)
        num_clients = len(clients)
        for cl in range(num_clients):
            client_vms = clients[cl]
            client_vms.Install(sysbench.PACKAGE_NAME)
            client_vms.AllowPort(mysql80.MYSQL_PORT.value)
        if PGO_OPTION.value:
            mysql_vms.Install(sysbench.PACKAGE_NAME)
            sysbench.Configure(client_vms, mysql_vms)
            sysbench.RunSysbenchOverAllPorts(mysql_vms, mysql_vms, 0, 1)
            mysql80.RemoveBuild(mysql_vms)
            mysql80.MysqlBuild(mysql_vms)
            mysql80.Configure(mysql_vms)
        for cl in range(num_clients):
            client_vms = clients[cl]
            sysbench.Configure(client_vms, mysql_vms)


def _PrepareServer(vm: _LinuxVM) -> None:
    vm.Install(mysql80.PACKAGE_NAME)
    vm.AllowPort(mysql80.MYSQL_PORT.value)
    mysql80.MysqlBuild(vm)
    mysql80.Configure(vm)


def _PrepareClient(vm: _LinuxVM) -> None:
    vm.Install(mysql80.PACKAGE_NAME)
    vm.AllowPort(mysql80.MYSQL_PORT.value)
    mysql80.MysqlBuild(vm)


def Run(benchmark_spec):
    """Run the sysbench benchmark and publish results.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.

    Returns:
      Results.
    """
    logging.info("Start benchmarking, Cloud Provider is %s.", FLAGS.cloud)
    mysql_vms = benchmark_spec.vm_groups["servers"][0]
    num_clients = 0

    def DistributeClientsToPorts(client, client_number, total_clients):
        return sysbench.RunSysbenchOverAllPorts(
            mysql_vms, client, client_number, total_clients
        )

    def RunTestOnMysqlSysbenchClient():
        args = [((client, i, num_clients), {}) for i, client in enumerate(clients)]
        raw_results = background_tasks.RunThreaded(DistributeClientsToPorts, args)
        return raw_results

    def RunTestOnMysqlSysbenchLocalhost():
        args = [((mysql_vms, 0, 1), {})]
        raw_results = background_tasks.RunThreaded(DistributeClientsToPorts, args)
        return raw_results

    def RunLatencyCappedThroughput():
        raw_result = []
        max_tps = 0
        thread_value = None
        best_qps_sample = None
        best_tps_sample = None
        worst_p95_sample = None
        best_results = None
        best_qps_sample = []
        workload_name = FLAGS[f"{sysbench.PACKAGE_NAME}_workloads"].value[0]
        thread_lower = FLAGS[f"{BENCHMARK_NAME}_thread_lower_bound"].value
        thread_upper = FLAGS[f"{BENCHMARK_NAME}_thread_upper_bound"].value
        thread_incr = FLAGS[f"{BENCHMARK_NAME}_threads_incr"].value
        while thread_lower <= thread_upper:
            # get thread  midpoint
            thread_mid_array = []
            thread_mid = thread_lower + (thread_upper - thread_lower) // 2
            thread_mid_array.append(thread_mid)
            FLAGS[f"{sysbench.PACKAGE_NAME}_threads"].value = thread_mid_array
            # giving sleep between 2 runs to bring machine back to normal state
            time.sleep(5)
            if localhost_option.value:
                raw_result = RunTestOnMysqlSysbenchLocalhost()
            else:
                raw_result = RunTestOnMysqlSysbenchClient()
            time.sleep(5)
            results = _ParseDefaultResults(raw_result)
            num_thread = results[0].value
            p95_latency_sample = results[1].value
            qps_sample = results[3].value
            tps_sample = results[2].value
            current_tps, current_p95 = tps_sample, p95_latency_sample
            # SLA violated: lower pipelines, continue
            if current_p95 > mysql_latency_cap.value:
                thread_upper = thread_mid - thread_incr
                continue
            # SLA in bounds: store best
            if current_tps > max_tps:
                max_tps = current_tps
                best_qps_sample = qps_sample
                best_tps_sample = tps_sample
                worst_p95_sample = p95_latency_sample
                thread_value = num_thread
                best_results = results
            thread_lower = thread_mid + thread_incr
        metadata = sysbench.GenerateMetadataFromFlags(num_clients, thread_value)
        best_qps_sample = _ParseMaxTptResults(
            workload_name,
            best_qps_sample,
            best_tps_sample,
            worst_p95_sample,
            thread_value,
            metadata,
            best_results,
	)
        return best_qps_sample
    
    sysbench_workloads = FLAGS[f"{sysbench.PACKAGE_NAME}_workloads"].value
    if len(sysbench_workloads) != 1 and mysql_latency_capped_throughput.value:
        raise ValueError(
            f"MySQL max throughput mode is only compatible with one sysbench workload at a time. "
            f"Received {sysbench.PACKAGE_NAME}_workloads={sysbench_workloads}"
        )

    if mysql_latency_capped_throughput.value:
        best_qps_sample = RunLatencyCappedThroughput()
        return best_qps_sample

    raw_result = []
    if localhost_option.value:
        raw_result = RunTestOnMysqlSysbenchLocalhost()
    else:
        clients = benchmark_spec.vm_groups["clients"]
        num_clients = len(clients)
        raw_result = RunTestOnMysqlSysbenchClient()
    results = _ParseDefaultResults(raw_result)
    return results



def _ParseDefaultResults(raw_results):
    """Parse raw results and metadata from a cassandra run across all cassandra stress processes
    Calculate aggregate results, worst latency, and all individual process results
    Returns:
      tuple containing sample objects and results list
    """
    results = []
    for port_result in raw_results:
        for result_sample in port_result:
            for data in result_sample:
                results.append(data)
    return results


def _ParseMaxTptResults(
    workload_name,
    best_qps_sample,
    best_tps_sample,
    worst_p95_sample,
    thread_value,
    metadata,
    best_results,
):
    """Create a custom sample for max throughput mode results"""
    print("best_results", best_results)
    if best_results is None:
        workload_sample = sample.Sample(
            metric=workload_name,
            value=None,
            unit="",
        )
        threads_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=thread_value,
            unit="",
        )
        p99_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=None,
            unit="",
        )
        max_qps_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=None,
            unit="",
        )
        max_tps_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=None,
            unit="",
        )
    else:
        workload_sample = sample.Sample(
            metric=workload_name,
            value=None,
            unit="",
        )
        threads_sample = sample.Sample(
            metric="Best Mysql Sysbench Thread",
            value=thread_value,
            unit="",
        )
        p99_sample = sample.Sample(
            metric="Worst p95 Latency",
            value=worst_p95_sample,
            unit="ms",
            metadata=metadata,
        )
        max_qps_sample = sample.Sample(
            metric="Best QPS",
            value=best_qps_sample,
            unit="best q/s",
            metadata=metadata,
        )
        max_tps_sample = sample.Sample(
            metric="Best TPS",
            value=best_tps_sample,
            unit="best t/s",
            metadata=metadata,
        )
    return (
        [workload_sample]
        + [threads_sample]
        + [p99_sample]
        + [max_qps_sample]
        + [max_tps_sample]
    )


def Cleanup(benchmark_spec) -> None:
    """Cleanup Nginx and load generators.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    logging.info("End benchmarking")
    # Kill mysql server and clean up
    server_vm = benchmark_spec.vm_groups["servers"][0]
    mysql80.CleanNode(server_vm)
    # clean sysbench
    if not localhost_option.value:
        clients = benchmark_spec.vm_groups["clients"]
        # clean sysbench
        for client in clients:
            sysbench.CleanNode(client)
