# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing wrk installation and cleanup functions.

WRK is an extremely scalable HTTP benchmarking tool.
https://github.com/wg/wrk
"""

import csv
import posixpath
import dataclasses
import logging
import os
from typing import Any, Dict, List, Text
from absl import flags
import six
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import data
from ampere.pkb.common import download_utils


PACKAGE_NAME = "ampere_wrk"
WRK_URL = "https://github.com/wg/wrk/archive/4.2.0.tar.gz"
WRK_DIR = posixpath.join(download_utils.INSTALL_DIR, "wrk")
WRK_PATH = posixpath.join(WRK_DIR, "wrk")
LUA_PATH = posixpath.join(WRK_DIR, "obj")

# Rather than parse WRK's free text output, this script is used to generate a
# CSV report
_LUA_SCRIPT_NAME = "wrk_latency.lua"
_LUA_SCRIPT_PATH = posixpath.join(WRK_DIR, _LUA_SCRIPT_NAME)

# WRK always outputs a free text report. _LUA_SCRIPT_NAME (above)
# writes this prefix before the CSV output begins.
_CSV_PREFIX = "==CSV==\n"

FLAGS = flags.FLAGS

YUM_PACKAGES = "zlib-devel pcre-devel libevent-devel openssl openssl-devel"

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_duration",
    30,
    "Mutually exclusive with Wrk_Connections."
    "Duration for each client count in seconds. ",
)
flags.DEFINE_integer(f"{PACKAGE_NAME}_connections", 120, "Number of total connections")
flags.DEFINE_integer(
    f"{PACKAGE_NAME}_threads",
    16,
    "Comma separated list of number of threads. "
    "Specify more than 1 value to vary the number of threads. ",
)
flags.DEFINE_bool(
    f"{PACKAGE_NAME}_latency_capped_throughput",
    False,
    "Measure latency capped throughput. Use in conjunction with "
    "wrk_latency_cap. Defaults to False. ",
)
flags.DEFINE_float(
    f"{PACKAGE_NAME}_latency_cap",
    5.0,
    "Latency cap in ms. Use in conjunction with "
    "latency_capped_throughput. Defaults to 5ms.",
)
WRK_DATA_FILES = flags.DEFINE_string(
    f"{PACKAGE_NAME}_data", "./perfkitbenchmarker/data/", "Must be in ./perfkitbenchmarker/data/"
)


def _Install(vm):

    vm.Install("curl")
    vm.Install("unzip")
    vm.RemoteCommand(f"sudo rm -rf {WRK_DIR}")
    vm.RemoteCommand(f"sudo mkdir -p {WRK_DIR} && "
                     f"curl -L {WRK_URL} | sudo tar --strip-components=1 -C {WRK_DIR} -xzf -")
    vm.RemoteCommand(f"cd {WRK_DIR} && sudo make -j")
    vm.PushDataFile(
        data.ResourcePath(posixpath.join(WRK_DATA_FILES.value, _LUA_SCRIPT_NAME)),
        "/tmp/wrk_latency.lua"
    )
    vm.RemoteCommand(f'sudo cp "/tmp/wrk_latency.lua" {_LUA_SCRIPT_PATH}')


def YumInstall(vm):
    """Installs wrk on the VM."""
    vm.InstallPackageGroup("Development Tools")
    vm.InstallPackages(YUM_PACKAGES)
    vm.InstallPackages("lua readline readline-devel")
    vm.PushDataFile(
        posixpath.join(vm_util.GetTempDir(), "NGINX_TEST_SSL.crt"),
        "/tmp/NGINX_TEST_SSL.crt",
    )
    vm.InstallPackages("ca-certificates")
    vm.RemoteCommand("sudo update-ca-trust force-enable")
    vm.RemoteCommand(
        "sudo cp /tmp/NGINX_TEST_SSL.crt /etc/pki/ca-trust/source/anchors/"
    )
    vm.RemoteCommand("sudo update-ca-trust extract")
    vm.InstallPackages("perl")
    _Install(vm)


def AptInstall(vm):
    """Installs wrk on the VM."""
    vm.Install("build_tools")
    vm.InstallPackages("lua5.1 liblua5.1-dev")
    vm.InstallPackages("ca-certificates")
    vm.PushDataFile(
        posixpath.join(vm_util.GetTempDir(), "NGINX_TEST_SSL.crt"),
        "/tmp/NGINX_TEST_SSL.crt",
    )
    vm.RemoteCommand(
        "sudo cp /tmp/NGINX_TEST_SSL.crt /usr/local/share/ca-certificates/NGINX_TEST_SSL.crt"
    )
    vm.RemoteCommand("sudo update-ca-certificates --verbose")
    _Install(vm)


def Uninstall(vm):
    """Cleans up Ampere wrk from the target vm.

    Args:
      vm: The vm on which Ampere wrk is uninstalled.
    """
    vm.RemoteCommand(f"sudo rm -rf {WRK_DIR}")


def RunWithConnectionsAndThreads(
    client_vm, client_number: int, target: str
) -> List[sample.Sample]:
    """Runs wrk over all connections and thread combinations."""
    samples = []
    client_connection = FLAGS[f"{PACKAGE_NAME}_connections"].value
    client_thread = FLAGS[f"{PACKAGE_NAME}_threads"].value
    logging.info(
        "Start benchmarking nginx using wrk:\n"
        "\twrk threads: %s"
        "\twrk connections, %s",
        client_thread,
        client_connection,
    )
    results = _Run(
        vm=client_vm,
        client_number=client_number,
        target=target,
        threads=client_thread,
        connections=client_connection,
    )
    metadata = GetMetadata(
        client_num=client_number, threads=client_thread, connections=client_connection
    )
    samples.extend(results.GetSamples(metadata))
    return samples


def _Run(vm, client_number, target, threads, connections) -> "WrkResult":
    """Runs wrk against a given target.

    Args:
      vm: Virtual machine.
      target: URL to fetch.
      connections: Number of concurrent connections.
      duration: Duration of the test, in seconds.
      script_path: If specified, a lua script to execute.
      threads: Number of threads. Defaults to min(connections, num_cores).
    Yields:
      sample.Sample objects with results.
    """
    output_path = os.path.join(
        vm_util.GetTempDir(), f"wrk_results{client_number}_{threads}_{connections}"
    )
    results_file = posixpath.join(
        "/tmp", f"wrk_results{client_number}_{threads}_{connections}"
    )

    wrk=WRK_PATH
    script=_LUA_SCRIPT_PATH
    duration=FLAGS[f"{PACKAGE_NAME}_duration"].value

    cmd = (
        f"{wrk} --connections={connections} --threads={threads} "
        f"--duration={duration} "
        f"--script={script} {target}"
    )
    cmd += " > " + results_file
    vm.RemoteCommand(cmd)
    vm.PullFile(vm_util.GetTempDir(), results_file)
    vm.RemoteCommand(f"sudo rm -f {results_file}")
    with open(output_path, "r",encoding='utf-8') as output:
        summary_data = output.read()
    return WrkResult.Parse(summary_data)


def GetMetadata(client_num: int, threads: int, connections: int) -> Dict[str, Any]:
    """Metadata for Wrk test."""
    meta = {"client_number": client_num, "connections": connections, "threads": threads}
    return meta


@dataclasses.dataclass
class WrkResult:
    """Class that represents Wrk results."""

    requests: float
    throughput: float
    p90_latency: float
    p95_latency: float
    p99_latency: float

    @classmethod
    def Parse(cls, wrk_results: Text) -> "WrkResult":
        """Parse wrk result textfile and return results.

        Args:
          wrk_results: Text output of running wrk.
        Returns:
        """
        aggregated_result = _ParseTotalThroughputAndLatency(wrk_results)
        return cls(
            requests=aggregated_result.requests,
            throughput=aggregated_result.throughput,
            p90_latency=aggregated_result.p90_latency,
            p95_latency=aggregated_result.p95_latency,
            p99_latency=aggregated_result.p99_latency,
        )

    def GetSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
        """Return this result as a list of samples."""
        metadata["p90_latency"] = self.p90_latency
        metadata["p95_latency"] = self.p95_latency
        metadata["p99_latency"] = self.p99_latency
        samples = [
            sample.Sample("requests", self.requests, "", metadata),
            sample.Sample("throughput", self.throughput, "requests/s", metadata),
        ]
        return samples


@dataclasses.dataclass(frozen=True)
class WrkAggregateResult:
    """Parsed aggregated wrk results."""

    requests: float
    throughput: float
    p90_latency: float
    p95_latency: float
    p99_latency: float


def _ParseTotalThroughputAndLatency(wrk_results: Text) -> "WrkAggregateResult":
    """Parses the output of _LUA_SCRIPT_NAME.

    Yields:
      (variable_name, value, unit) tuples.
    """
    if _CSV_PREFIX not in wrk_results:
        raise ValueError(f"{_CSV_PREFIX} not found in\n{wrk_results}")
    csv_fp = six.StringIO(str(wrk_results).rsplit(_CSV_PREFIX, 1)[-1])
    reader = csv.DictReader(csv_fp)
    if frozenset(reader.fieldnames) != frozenset(["variable", "value", "unit"]):
        raise ValueError(f"Unexpected fields: {reader.fieldnames}")
    for row in reader:
        if row["variable"].startswith("p9"):
            if row["unit"] == "ms":
                value_in_ms = float(row["value"])
            elif row["unit"] == "us":
                value_in_ms = float(row["value"]) / 1000.0
            elif row["unit"] == "s":
                value_in_ms = float(row["value"]) * 1000.0
            elif row["unit"] == "m":
                value_in_ms = float(row["value"]) * 60.0 * 1000.0
            if row["variable"].startswith("p90 latency"):
                p90_latency_f = value_in_ms
            elif row["variable"].startswith("p95 latency"):
                p95_latency_f = value_in_ms
            elif row["variable"].startswith("p99 latency"):
                p99_latency_f = value_in_ms
        if row["variable"].startswith("requests"):
            requests_f = float(row["value"])
        elif row["variable"].startswith("throughput"):
            throughput_f = float(row["value"])

    return WrkAggregateResult(
        requests=requests_f,
        throughput=throughput_f,
        p90_latency=p90_latency_f,
        p95_latency=p95_latency_f,
        p99_latency=p99_latency_f,
    )
