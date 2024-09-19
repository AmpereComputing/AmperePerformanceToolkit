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

"""Run memtier_benchmark against Redis.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import background_tasks
from ampere.pkb.common import download_utils
from ampere.pkb.linux_packages import memtier
from ampere.pkb.linux_packages import redis_server


BENCHMARK_NAME = "ampere_redis_memtier"
BENCHMARK_CONFIG = """
ampere_redis_memtier:
  description: >
      Run memtier_benchmark against Redis.
      Specify the number of client VMs with --redis_clients.
  vm_groups:
    servers:
      vm_spec: *default_dual_core
      vm_count: 1
      disk_spec: *default_50_gb
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""

FLAGS = flags.FLAGS
flags.DEFINE_string(
    f"{BENCHMARK_NAME}_client_machine_type",
    None,
    "If provided, overrides the memtier client machine type.",
)
flags.DEFINE_string(
    f"{BENCHMARK_NAME}_server_machine_type",
    None,
    "If provided, overrides the redis server machine type.",
)
flags.DEFINE_bool(
    f"{BENCHMARK_NAME}_max_throughput_mode",
    False,
    "Get the maximum throughput under SLA, "
    "use in conjunction with ampere_redis_memtier_p99_latency_cap.",
)
flags.DEFINE_float(
    f"{BENCHMARK_NAME}_p99_latency_cap",
    1.0,
    "Latency cap in ms. Use in conjunction with latency_capped_throughput. Defaults to 1ms.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_pipelines_lower_bound",
    20,
    "Use with max throughput mode, defaults to 20.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_pipelines_upper_bound",
    1400,
    "Use with max throughput mode, defaults to 700.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_clients_lower_bound",
    1,
    "Use with max throughput mode, defaults to 1.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_clients_upper_bound",
    4,
    "Use with max throughput mode, defaults to 4.",
)
_BenchmarkSpec = benchmark_spec.BenchmarkSpec

def _get_core_range_list(core_range_string):
    """Helper function takes a numactl core range string and
    converts to a python list containing each core
    e.g. "0-3,4-7" -> [0, 1, 2, 3, 4, 5, 6, 7]
    """
    all_cores_list = []
    for core_range in core_range_string.split(","):
        core_start, core_end = core_range.split("-")
        core_start, core_end = int(core_start), int(core_end)
        all_cores_list += [core for core in range(core_start, core_end + 1)]
    return all_cores_list


def get_memtier_core_ranges() -> list:
    """Helper function takes a list of numactl core range strings and
    converts to a python list of lists, where each nested list represents a client and
    contains an integer for each core
    e.g. ["0-3,4-7", "0-3,4-7"] -> [[0, 1, 2, 3, 4, 5, 6, 7], [0, 1, 2, 3, 4, 5, 6, 7]]
    """
    core_ranges_across_clients = []
    for client_core_range_string in memtier._NUMA_CORES.value:
        core_ranges_across_clients.append(_get_core_range_list(client_core_range_string))
    return core_ranges_across_clients


def _VerifyBenchmarkSetup(client_vms, redis_ports):
    """Verifies that benchmark setup is correct."""

    if len(redis_ports) > 1 and (
        len(FLAGS.ampere_memtier_pipeline) > 1
        or len(FLAGS.ampere_memtier_threads) > 1
        or len(FLAGS.ampere_memtier_clients) > 1
    ):
        raise errors.Setup.InvalidFlagConfigurationError(
            "There can only be 1 setting for pipeline, threads and clients if "
            "there are multiple redis endpoints. Consider splitting up the "
            "benchmarking."
        )
    # Check client configuration if using numactl pinning
    if memtier._NUMA_CORES.value:
        # Number of core ranges must equal number of client systems
        if len(client_vms) != len(memtier._NUMA_CORES.value):
            raise errors.Setup.InvalidFlagConfigurationError(
                f"Set ampere_memtier_numa_cores_list flag for {len(client_vms)}" " clients"
                )
        # Number of cores used across clients must equal number of redis ports
        all_client_numa_cores =get_memtier_core_ranges()
        numa_core_count = 0
        for client_cores in all_client_numa_cores:
            numa_core_count += len(client_cores)
        if len(redis_ports) != numa_core_count:
            raise errors.Setup.InvalidFlagConfigurationError(
                "Cores in ampere_memtier_numa_cores_list "
                " should match number of redis ports")


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
    """Load and return benchmark config spec."""
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
    if FLAGS.ampere_redis_memtier_client_machine_type:
        vm_spec = config["vm_groups"]["clients"]["vm_spec"]
        for cloud in vm_spec:
            vm_spec[cloud][
                "machine_type"
            ] = FLAGS.ampere_redis_memtier_client_machine_type
    if FLAGS.ampere_redis_memtier_server_machine_type:
        vm_spec = config["vm_groups"]["servers"]["vm_spec"]
        for cloud in vm_spec:
            vm_spec[cloud][
                "machine_type"
            ] = FLAGS.ampere_redis_memtier_server_machine_type
    if redis_server.REDIS_SIMULATE_AOF.value:
        config["vm_groups"]["servers"]["disk_spec"]["GCP"]["disk_type"] = "local"
        config["vm_groups"]["servers"]["vm_spec"]["GCP"]["num_local_ssds"] = 8
        FLAGS.ampere_num_striped_disks = 7
        FLAGS.ampere_gce_ssd_interface = "NVME"
    else:
        config["vm_groups"]["servers"].pop("disk_spec")
    return config

def create_memtier_args(client_vms, server_ip, ports) -> list:
    """Generates a list of arguments in the form (tuple, dict), which is
    required by background_tasks.RunThreaded()

    Can be used with memtier.Load() or memtier.RunOverAllThreadsPipelinesAndClients()

    Creates correct args based on
        - Any number of clients
        - Numactl of memtier process (if desired)

    args -> [
        (
            (client_vm_1, server_internal_ip, port, password, core), {}
        ),
        ...
        (
            (client_vm_2, server_internal_ip, port, password, core), {}
        )
    ]

    e.g.
    args -> [
        (
            (client_vm_1, "10.x.x.x", 6379, None, 10), {}
        )
        ...
        (
            (client_vm_2, "10.x.x.x", 6449, None, 10), {}
        )
    ]
    """
    args = []
    if memtier._NUMA_CORES.value:
        port_idx = 0
        core_ranges_across_clients = get_memtier_core_ranges()
        for client_idx, client_cores_list in enumerate(core_ranges_across_clients):
            client_vm = client_vms[client_idx]
            for core in client_cores_list:
                args += [
                    (
                        (
                            client_vm,
                            server_ip,
                            ports[port_idx],
                            None,
                            core,
                            ),
                        {},
                    )
                ]
                port_idx += 1
    else:
        # evenly distribute ports to clients
        ports_per_client = int(len(ports) / len(client_vms))
        mod_port_client = len(ports) % len(client_vms)
        client_idx = 0
        # Step through slices of ports per client
        for idx in range(0, len(ports), ports_per_client):
            # Get current client_vm
            # Get current slice of ports for the current client
            ports_for_current_client = ports[idx:idx + ports_per_client]
            if client_idx > len(client_vms) -1:
                client_idx = len(client_vms) -1
                ports_for_current_client = ports[idx:idx + mod_port_client]
            client_vm = client_vms[client_idx]
            for port in ports_for_current_client:
                args += [
                    (
                        (
                            client_vm,
                            server_ip,
                            port,
                            None,
                            ),
                        {},
                    )
                ]
            client_idx += 1
    return args


def Prepare(bm_spec: _BenchmarkSpec) -> None:
    """Install Redis on one VM and memtier_benchmark on another."""
    server_count = len(bm_spec.vm_groups["servers"])
    if server_count != 1:
        raise errors.Benchmarks.PrepareException(
            f"Expected servers vm count to be 1, got {server_count}"
        )
    client_vms = bm_spec.vm_groups["clients"]
    server_vm = bm_spec.vm_groups["servers"][0]

    ports = redis_server.GetRedisPorts(server_vm)

    # Verify benchmark setup
    _VerifyBenchmarkSetup(client_vms, ports)

    print("checking _VerifyBenchmarkClientsSetup")

    # Install memtier
    background_tasks.RunThreaded(
        lambda client: client.Install(memtier.PACKAGE_NAME), client_vms
    )

    # Increase number of SSH connections on both client and server to arbitrarily high value
    max_startups_sessions = 500
    all_vms = client_vms + [server_vm]
    args_increase_sessions = [((vm, max_startups_sessions), {}) for i,vm in enumerate(all_vms)]
    background_tasks.RunThreaded(_increase_ssh_max_sessions_startups, args_increase_sessions)

    # Install redis on the 1st machine.
    server_vm.Install(redis_server.PACKAGE_NAME)
    redis_server.Start(server_vm)
    bm_spec.redis_endpoint_ip = bm_spec.vm_groups["servers"][0].internal_ip
    args = create_memtier_args(client_vms, bm_spec.redis_endpoint_ip, ports)
    background_tasks.RunThreaded(memtier.Load, args)


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
    """Run memtier_benchmark against Redis."""
    client_vms = bm_spec.vm_groups["clients"]
    # Don't reference vm_groups['server'] directly, because this is reused by
    # kubernetes_redis_memtier_benchmark, which doesn't have one.

    server_vm = bm_spec.vm_groups["servers"][0]
    ports = [str(port) for port in redis_server.GetRedisPorts(server_vm)]

    # If redis processes are numa bound, update ports accordingly
    if redis_server._NUMA_CORES.value:
        all_cores_ports = redis_server._numa_cores_to_ports()
        ports = list(map(lambda x: x[1], all_cores_ports))

    # If numa is specified for Memtier, get cores and assign to ports
    # memtier_cores_ports = None
    # if memtier._NUMA_CORES.value:
    # memtier_cores = memtier._get_numa_cores()
    # zip together memtier cores and ports
    # NOTE: the resulting list of zipped tuples stops when the shortest list is exhausted
    # memtier_cores_ports = list(zip(memtier_cores, ports))
    # The format (tuple, dict) is required by background_tasks.RunThreaded()
    # for i in range(len(memtier_cores_ports)):
    #  memtier_cores_ports[i] = (memtier_cores_ports[i], {})

    # Allow traffic on all ports on both client and server
    for vm in client_vms + [server_vm]:
        vm.AllowPort(int(ports[0]), int(ports[-1]))
        # Check if firewalld is installed on system by default
        stdout, stderr = vm.RemoteCommand(
            "sudo firewall-cmd --version", ignore_failure=True
        )
        if not stderr:
            vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={ports[0]}-{ports[-1]}/tcp --permanent"
            )
            vm.RemoteCommand(f"sudo firewall-cmd --reload")

    benchmark_metadata = {}
    redis_metadata = redis_server.GetMetadata()

    def RunMemtierMaxTptMode(redis_metadata, benchmark_metadata):
        """Run Memtier against Redis with binary search to find a configuration that
        achieves the best aggregate ops throughput under 1ms p99 latency
        Returns:
          - Best throughput sample and config (see _ParseMaxTptResults)
        Notes:
          - Pipelines/clients:  default upper/lower bounds are set by global flags
          - Threads:            hard coded to 1 since redis is single threaded
          - All client values from lower bound to upper bound are tested to find the ideal pipelines value
            - The midpoint of the pipelines upper/lower bound is tested at each iteration
            - If the current pipelines value violates 1ms p99, pipelines are lowered and search continues
            - If the current pipelines value doesn't violate 1ms p99, clients are increased until SLA is broken
          - The best value/configuration is saved at each stage of the search
        """
        pipelines_lower = FLAGS[f"{BENCHMARK_NAME}_pipelines_lower_bound"].value
        pipelines_upper = FLAGS[f"{BENCHMARK_NAME}_pipelines_upper_bound"].value
        clients_lower = FLAGS[f"{BENCHMARK_NAME}_clients_lower_bound"].value
        clients_upper = FLAGS[f"{BENCHMARK_NAME}_clients_upper_bound"].value
        THREADS = 1  # hard coded since redis is single threaded

        max_agg = 0
        best_ops_sample = None
        worst_p99_sample = None
        best_cache_hit_rate_sample = None
        best_results = None

        # Perform binary search on pipelines for each client value
        #   - stop when the lower bound is within 5 steps of the upper bound
        for client in range(clients_lower, clients_upper):
            p_low, p_up = pipelines_lower, pipelines_upper
            while p_low <= (p_up - 5):
                # get pipelines midpoint
                p_mid = p_low + (p_up - p_low) // 2
                # try current pipeline with current client
                params = {"pipelines": p_mid, "clients": client, "threads": THREADS}
                raw_results = RunMemtierCustomParams(params)
                agg_sample, p99_sample, cache_hit_rate_sample, results = (
                    _ParseDefaultResults(
                        raw_results, redis_metadata, benchmark_metadata
                    )
                )
                current_agg, current_p99 = agg_sample.value, p99_sample.value
                # SLA violated: lower pipelines, continue
                if current_p99 > FLAGS.ampere_redis_memtier_p99_latency_cap:
                    p_up = p_mid - 1
                    continue
                # SLA in bounds: store best
                if current_agg > max_agg:
                    max_agg = current_agg
                    best_ops_sample = agg_sample
                    worst_p99_sample = p99_sample
                    best_cache_hit_rate_sample = cache_hit_rate_sample
                    best_results = results
                # Raise pipelines and continue
                p_low = p_mid + 1
        # Create sample and return
        best_tpt_sample = _ParseMaxTptResults(
            best_ops_sample, worst_p99_sample, best_cache_hit_rate_sample, best_results
        )
        return best_tpt_sample

    def RunMemtierCustomParams(params: Dict):
        """Helper function to run memtier with specific pipelines, clients, and threads"""
        # Set memtier flag values for max tpt mode (must be lists)
        FLAGS["ampere_memtier_pipeline"].value = [params["pipelines"]]
        FLAGS["ampere_memtier_clients"].value = [params["clients"]]
        FLAGS["ampere_memtier_threads"].value = [params["threads"]]
        FLAGS["ampere_memtier_run_duration"].value = 30
        args = create_memtier_args(client_vms, bm_spec.redis_endpoint_ip, ports)
        raw_results = background_tasks.RunThreaded(
            memtier.RunOverAllThreadsPipelinesAndClients, args)
        return raw_results

    def RunMemtierDefaultMode(redis_metadata, benchmark_metadata):
        """Runs Memtier against Redis in default mode
        Returns:
          - aggregate ops throughput
          - p99 latency from all processes
          - all processes
        """

        # If numactl, load with specific core/port values
        # Otherwise load with just port values
        args = create_memtier_args(client_vms, bm_spec.redis_endpoint_ip, ports)
        raw_results = background_tasks.RunThreaded(
            memtier.RunOverAllThreadsPipelinesAndClients, args
            )
        agg_sample, p99_sample, cache_hit_rate_sample, results = (
            _ParseDefaultResults(raw_results, redis_metadata, benchmark_metadata)
            )
        return [agg_sample] + [p99_sample] + [cache_hit_rate_sample] + results

    max_tpt_mode = FLAGS[f"{BENCHMARK_NAME}_max_throughput_mode"].value
    if max_tpt_mode:
        return RunMemtierMaxTptMode(redis_metadata, benchmark_metadata)
    else:
        return RunMemtierDefaultMode(redis_metadata, benchmark_metadata)


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
    # Kill redis-server and clean up
    client_vms = bm_spec.vm_groups["clients"]
    server_vm = bm_spec.vm_groups["servers"][0]
    server_vm.RemoteCommand("sudo pkill -f redis-server")
    server_vm.RemoteCommand(f"sudo rm -rf {download_utils.INSTALL_DIR}")
    for client in client_vms:
        client.RemoteCommand(f"sudo rm -rf {download_utils.INSTALL_DIR}")
    del bm_spec


def _ParseDefaultResults(raw_results, redis_metadata, benchmark_metadata):
    """Parse raw results and metadata from a memtier run across all redis processes
    Calculate aggregate results, worst latency, cache hitrate, and all individual process results
    Returns:
      tuple containing sample objects and results list
    """
    results = []
    aggregate_ops_tpt = 0
    p99_latency = 0
    aggregate_hits_per_sec = 0
    aggregate_get_ops_per_sec = 0

    for port_result in raw_results:
        for result_sample in port_result:
            result_sample.metadata.update(redis_metadata)
            result_sample.metadata.update(benchmark_metadata)
            if result_sample.metric == "Ops Throughput":
                current_tpt = result_sample.value
                current_p99 = result_sample.metadata["p99_latency"]
                aggregate_ops_tpt += current_tpt
                p99_latency = max(p99_latency, current_p99)
            if result_sample.metric == "Hits/s":
                aggregate_hits_per_sec += result_sample.value
            if result_sample.metric == "Get Ops/s":
                aggregate_get_ops_per_sec += result_sample.value
            results.append(result_sample)

    agg_ops_sample = sample.Sample(
        metric="Aggregate Ops Throughput",
        value=aggregate_ops_tpt,
        unit="aggregate ops/s",
    )
    p99_sample = sample.Sample(metric="p99_latency", value=p99_latency, unit="ms")

    cache_hit_rate = (aggregate_hits_per_sec / aggregate_get_ops_per_sec) * 100
    cache_hit_rate_sample = sample.Sample(
        metric="cache_hit_rate", value=cache_hit_rate, unit="hitrate"
    )

    agg_ops_sample.metadata.update(redis_metadata)
    agg_ops_sample.metadata.update(benchmark_metadata)
    p99_sample.metadata.update(redis_metadata)
    p99_sample.metadata.update(benchmark_metadata)
    cache_hit_rate_sample.metadata.update(redis_metadata)
    cache_hit_rate_sample.metadata.update(benchmark_metadata)

    return agg_ops_sample, p99_sample, cache_hit_rate_sample, results


def _ParseMaxTptResults(
    best_ops_sample, worst_p99_sample, best_cache_hit_rate_sample, best_results
):
    """Create a custom sample for max throughput mode results"""
    max_tpt_sample = sample.Sample(
        metric="Best Aggregate Ops Throughput",
        value=best_ops_sample.value,
        unit="best aggregate ops/s",
        metadata=best_ops_sample.metadata,
    )

    pipelines_sample = sample.Sample(
        metric="Best Memtier Pipelines",
        value=best_results[0].metadata["memtier_pipeline"],
        unit="memtier_pipelines",
    )

    clients_sample = sample.Sample(
        metric="Best Memtier Clients",
        value=best_results[0].metadata["memtier_clients"],
        unit="memtier_clients",
    )

    threads_sample = sample.Sample(
        metric="Best Memtier Threads",
        value=best_results[0].metadata["memtier_threads"],
        unit="memtier_threads",
    )

    p99_sample = sample.Sample(
        metric="Worst p99 Latency",
        value=worst_p99_sample.value,
        unit="ms",
        metadata=worst_p99_sample.metadata,
    )

    cache_hit_rate_sample = sample.Sample(
        metric="cache_hit_rate",
        value=best_cache_hit_rate_sample.value,
        unit="hitrate",
        metadata=best_cache_hit_rate_sample.metadata,
    )

    return (
        [max_tpt_sample]
        + [pipelines_sample]
        + [clients_sample]
        + [threads_sample]
        + [p99_sample]
        + [cache_hit_rate_sample]
        + best_results
    )

def _increase_ssh_max_sessions_startups(vm, session_count):
    vm.RemoteCommand(fr'sudo sed -i -e "s/.*MaxStartups.*/MaxStartups {session_count}/" /etc/ssh/sshd_config')
    vm.RemoteCommand(fr'sudo sed -i -e "s/.*MaxSessions.*/MaxSessions {session_count}/" /etc/ssh/sshd_config')
    # Restart sshd either for RHEL-based or Debian-based
    stdout, stderr = vm.RemoteCommand('sudo systemctl --version', ignore_failure=True)
    if not stderr:
        vm.RemoteCommand('sudo systemctl restart sshd')
    else:
        vm.RemoteCommand('sudo service ssh restart')


