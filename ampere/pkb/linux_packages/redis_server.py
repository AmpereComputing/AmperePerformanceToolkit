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


"""Module containing redis installation and cleanup functions."""

import logging
import posixpath

from perfkitbenchmarker import data
from typing import Any, Dict, List
from absl import flags
from ampere.pkb.common import download_utils

PACKAGE_NAME = "ampere_redis_server"


class RedisEvictionPolicy:
    """Enum of options for --redis_eviction_policy."""

    NOEVICTION = "noeviction"
    ALLKEYS_LRU = "allkeys-lru"
    VOLATILE_LRU = "volatile-lru"
    ALLKEYS_RANDOM = "allkeys-random"
    VOLATILE_RANDOM = "volatile-random"
    VOLATILE_TTL = "volatile-ttl"


_VERSION = flags.DEFINE_string(
    f"{PACKAGE_NAME}_version", "7.2.0", "Version of redis server to use."
)
_IO_THREADS = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_io_threads",
    None,
    "Only supported for redis version >= 6, the "
    "number of redis server IO threads to use.",
)
_IO_THREADS_DO_READS = flags.DEFINE_bool(
    f"{PACKAGE_NAME}_io_threads_do_reads",
    False,
    "If true, makes both reads and writes use IO threads instead of just " "writes.",
)
_IO_THREAD_AFFINITY = flags.DEFINE_bool(
    f"{PACKAGE_NAME}_io_threads_cpu_affinity",
    False,
    "If true, attempts to pin IO threads to CPUs.",
)
_ENABLE_SNAPSHOTS = flags.DEFINE_bool(
    f"{PACKAGE_NAME}_enable_snapshots",
    False,
    "If true, uses the default redis snapshot policy.",
)
_NUM_PROCESSES = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_total_num_processes",
    None,
    "Total number of redis server processes. Useful when running with a redis "
    "version lower than 6. Defaults to lscpu count.",
)
_EVICTION_POLICY = flags.DEFINE_enum(
    f"{PACKAGE_NAME}_eviction_policy",
    RedisEvictionPolicy.VOLATILE_TTL,
    [
        RedisEvictionPolicy.NOEVICTION,
        RedisEvictionPolicy.ALLKEYS_LRU,
        RedisEvictionPolicy.VOLATILE_LRU,
        RedisEvictionPolicy.ALLKEYS_RANDOM,
        RedisEvictionPolicy.VOLATILE_RANDOM,
        RedisEvictionPolicy.VOLATILE_TTL,
    ],
    "Redis eviction policy when maxmemory limit is reached. This requires "
    "running clients with larger amounts of data than Redis can hold.",
)
REDIS_SIMULATE_AOF = flags.DEFINE_bool(
    f"{PACKAGE_NAME}_simulate_aof",
    False,
    "If true, simulate usage of " "disks on the server for aof backups. ",
)
REDIS_CONFIG = flags.DEFINE_string(
    f"{PACKAGE_NAME}_config",
    None,
    "An alternate config to use with redis server. "
    "Must be located in ./ampere/pkb/data/ ",
)
_MAX_MEMORY = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_max_memory", 1536, "Default max memory."
)
_NUMA_CORES = flags.DEFINE_string(
    f'{PACKAGE_NAME}_numa_cores', None,
    f'Used to pin each redis server process to a core with numactl. '
    f'Can be a single range or multiple comma-separated ranges. Currently only supports 1P systems.'
    f'e.g. 0-80 or 0-39,80-119')
_ENABLE_THP = flags.DEFINE_bool(
    f'{PACKAGE_NAME}_enable_thp', False, 'If true, will pass `--disable-thp no` to all '
    f'Redis server start commands.')


# Default port for Redis
_DEFAULT_PORT = 6379
REDIS_PID_FILE = "redis.pid"
FLAGS = flags.FLAGS
REDIS_GIT = "https://github.com/antirez/redis.git"
REDIS_BACKUP = "redis_backup"


def _GetRedisTarName() -> str:
    return f"redis-{_VERSION.value}.tar.gz"


def GetRedisDir() -> str:
    return f"{download_utils.INSTALL_DIR}/redis"


def _Install(vm) -> None:
    """Installs the redis package on the VM."""
    vm.Install("build_tools")
    vm.Install("wget")
    vm.InstallPackages("numactl")
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR}; git clone {REDIS_GIT}")
    vm.RemoteCommand(
        f'cd {GetRedisDir()} && git checkout {_VERSION.value} && make -j CFLAGS="-O3 -march=native"'
    )


def YumInstall(vm) -> None:
    """Installs the redis package on the VM."""
    # Memtier recommends these packages for CentOS systems, see the Prerequisites section at https://github.com/RedisLabs/memtier_benchmark
    yum_packages = [
        "pcre-devel",
        "zlib-devel",
        "libmemcached-devel",
        "libevent-devel",
        "openssl-devel",
    ]
    # Catch cases where packages aren't available with --skip-broken (RHEL9 on Arm doesn't have libmemcached-devel)
    vm.InstallPackages(" ".join(yum_packages) + " " + "--skip-broken")
    _Install(vm)


def AptInstall(vm) -> None:
    """Installs the redis package on the VM."""
    vm.InstallPackages("tcl-dev")
    _Install(vm)


def _BuildStartCommand(vm, port: int, config_path, numa_prefix="") -> str:
    """Returns the run command used to start the redis server.

    See https://raw.githubusercontent.com/redis/redis/6.0/redis.conf
    for the default redis configuration.

    Args:
      vm: The redis server VM.
      port: The port to start redis on.

    Returns:
      A command that can be used to start redis in the background.
[
    """
    redis_dir = GetRedisDir()
    cmd = "nohup sudo {numa} {redis_dir}/src/redis-server {args} &> {server_log_path} &"

    cmd_args = [
        f"--port {port}",
        "--protected-mode no",
        "--tcp-backlog 262144",
        '--dbfilename ""',
        "--repl-disable-tcp-nodelay no",
        "--hz 100",
        '--bind "*"',
    ]
    # Support alternate redis config for baremetal as first argument
    if REDIS_CONFIG.value:
        cmd_args = [config_path] + cmd_args

    if REDIS_SIMULATE_AOF.value:
        cmd_args += [
            "--appendonly yes",
            "--appendfilename backup",
            f"--dir /{REDIS_BACKUP}",
        ]
    # Add check for the MADV_FREE/fork arm64 Linux kernel bug
    if _VERSION.value >= '6.2.1':
        cmd_args.append('--ignore-warnings ARM64-COW-BUG')
    # Snapshotting
    if not _ENABLE_SNAPSHOTS.value:
        cmd_args.append('--save ""')
    # IO threads
    if _IO_THREADS.value:
        cmd_args.append(f'--io-threads {_IO_THREADS.value}')
    # IO thread reads
    if _IO_THREADS_DO_READS.value:
        do_reads = 'yes' if _IO_THREADS_DO_READS.value else 'no'
        cmd_args.append(f'--io-threads-do-reads {do_reads}')
    # IO thread affinity
    if _IO_THREAD_AFFINITY.value:
        cpu_affinity = f'0-{vm.num_cpus-1}'
        cmd_args.append(f'--server_cpulist {cpu_affinity}')
    if _EVICTION_POLICY.value:
        cmd_args.append(f'--maxmemory-policy {_EVICTION_POLICY.value}')
    if _MAX_MEMORY.value:
        cmd_args.append(f'--maxmemory {_MAX_MEMORY.value}mb')
    # Enable THP 
    if _ENABLE_THP.value:
        cmd_args.append(f'--disable-thp no')
    new_cmd = cmd.format(numa=numa_prefix, redis_dir=redis_dir, args=' '.join(cmd_args), server_log_path=f'/tmp/redis{port}.log')
    logging.debug(f'REDIS SERVER START: {cmd}')    
    return new_cmd



def Start(vm) -> None:
    """Start redis server process."""
    # Redis tuning parameters, see
    # https://www.techandme.se/performance-tips-for-redis-cache-server/.
    # This command works on 2nd generation of VMs only.
    update_sysvtl = vm.TryRemoteCommand(
        'echo "'
        "vm.overcommit_memory = 1\n"
        "net.core.somaxconn = 65535\n"
        '" | sudo tee -a /etc/sysctl.conf'
    )
    commit_sysvtl = vm.TryRemoteCommand("sudo /usr/sbin/sysctl -p")
    if not (update_sysvtl and commit_sysvtl):
        logging.info("Fail to optimize overcommit_memory and socket connections.")

    # Support alternate redis config for baremetal
    #   - ./ampere/pkb/data/redis_baremetal.conf
    deploy_config = None
    if REDIS_CONFIG.value:
        redis_dir = GetRedisDir()
        local_config = data.ResourcePath(REDIS_CONFIG.value)
        vm.PushFile(local_config, redis_dir)
        deploy_config = posixpath.join(redis_dir, posixpath.basename(local_config))

    # Bind each redis process with numactl if desired
    if _NUMA_CORES.value:
        for core, port in _numa_cores_to_ports():
            prefix = f"numactl -C {core}"
            vm.RemoteCommand(
                _BuildStartCommand(vm, port, deploy_config, numa_prefix=prefix)
            )
    else:
        for port in GetRedisPorts(vm):  # Run default w/o numactl
            vm.RemoteCommand(_BuildStartCommand(vm, port, deploy_config))


def GetMetadata() -> Dict[str, Any]:
    return {
        "redis_server_version": _VERSION.value,
        "redis_server_io_threads": _IO_THREADS.value,
        "redis_server_io_threads_do_reads": _IO_THREADS_DO_READS.value,
        "redis_server_io_threads_cpu_affinity": _IO_THREAD_AFFINITY.value,
        "redis_server_enable_snapshots": _ENABLE_SNAPSHOTS.value,
        "redis_server_num_processes": _NUM_PROCESSES.value,
    }


def GetRedisPorts(vm) -> List[int]:
    """Returns a list of redis port(s)."""
    # Set ampere specified defaults (dependent on target system)
    #   - num_processes ->  lscpu
    #   - io_threads    ->  3/4 of lscpu
    lscpu = vm.CheckLsCpu()
    cpu_count = int(lscpu.data["CPU(s)"])
    if not _NUM_PROCESSES.value:
        flags.FLAGS.set_default(f"{PACKAGE_NAME}_total_num_processes", cpu_count)
    if not _IO_THREADS.value:
        flags.FLAGS.set_default(f"{PACKAGE_NAME}_io_threads", int(cpu_count * 0.75))

    return [_DEFAULT_PORT + i for i in range(_NUM_PROCESSES.value)]


def _numa_cores_to_ports():
    """Helper function that returns a list of tuples
    containing core/port pairs for numactl binding
    e.g. [(0, 6379), (1, 6380), ...]
    """
    all_cores_ports = []
    if _NUMA_CORES.value:
        current_port = _DEFAULT_PORT
        core_ranges_string = _NUMA_CORES.value
        for core_range in core_ranges_string.split(","):
            core_start, core_end = core_range.split("-")
            core_start, core_end = int(core_start), int(core_end)
            for core in range(core_start, core_end + 1):
                all_cores_ports.append((core, current_port))
                current_port += 1
    return all_cores_ports
