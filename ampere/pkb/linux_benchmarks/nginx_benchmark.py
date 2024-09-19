# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs HTTP load generators against an Nginx server."""

import ipaddress
import posixpath
from typing import Dict
from absl import flags
from perfkitbenchmarker import data


from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import flag_util

from ampere.pkb.linux_packages import wrk
from ampere.pkb.linux_packages import nginx

FLAGS = flags.FLAGS
BENCHMARK_NAME = "ampere_nginx_wrk"

flags.DEFINE_string(
    f"{BENCHMARK_NAME}_client_machine_type",
    None,
    "Machine type to use for the wrk client if different "
    "from nginx server machine type.",
)
flags.DEFINE_string(
    f"{BENCHMARK_NAME}_server_machine_type",
    None,
    "Machine type to use for the nginx server if different "
    "from wrk client machine type.",
)
flags.DEFINE_boolean(
    f"{BENCHMARK_NAME}_use_ssl", True, "Use HTTPs when connecting to nginx."
)

flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_connections_lower_bound",
    1,
    "Use with max throughput mode, defaults to 1.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_connections_upper_bound",
    500,
    "Use with max throughput mode, defaults to 500.",
)

flag_util.DEFINE_integerlist(
    f"{BENCHMARK_NAME}_threads_list",
    [1,2,4,8,12,16,20,24,28,32],
    "Use with max throughput mode, list of threads to iterate through.",
)

flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_server_port", 80, "The port that nginx server will listen to. "
)


BENCHMARK_CONFIG = """
ampere_nginx_wrk:
  description: Benchmarks Nginx server performance.
  vm_groups:
    clients:
#      static_vms:
      vm_spec: *default_dual_core
      vm_count: 1
    servers:
#      static_vms:    
      vm_spec: *default_dual_core
      vm_count: 1
  flags:
"""


def GetConfig(user_config):
    """Load and return benchmark config.

    Args:
      user_config: user supplied configuration (flags and config file)

    Returns:
      loaded benchmark configuration
    """
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
    return config


def _ConfigureNginxForSsl(server):
    """Configures an nginx server for SSL/TLS."""
    server.RemoteCommand("sudo mkdir -p /etc/pki/tls/private")
    server.RemoteCommand("sudo mkdir -p /etc/pki/tls/certs")
    server.RemoteCommand(
        "sudo openssl req -x509 -nodes -days 365 -newkey ec "
        '-subj "/CN=localhost" '
        "-pkeyopt ec_paramgen_curve:secp384r1 "
        "-keyout /etc/pki/tls/private/NGINX_TEST_SSL.key "
        "-out /etc/pki/tls/certs/NGINX_TEST_SSL.crt"
    )

    server.PullFile(vm_util.GetTempDir(), "/etc/pki/tls/certs/NGINX_TEST_SSL.crt")


def _ConfigureNginx(server):
    """Configures nginx server."""
    server.RemoteCommand("sudo mkdir -p /var/www/html")
    server.RemoteCommand("sudo mkdir -p /var/log/nginx")
    nginx_path = nginx.GetnginxDirPath()
    server.RemoteCommand("sudo mkdir -p /etc/nginx")
    server.RemoteCommand(f"sudo cp -rf {nginx_path}/conf/mime.types* /etc/nginx/")
    content_path = data.ResourcePath(
        posixpath.join(
            FLAGS[f"{nginx.PACKAGE_NAME}_data"].value,
            FLAGS[f"{nginx.PACKAGE_NAME}_local_html"].value,
        )
    )
    server.PushDataFile(content_path, "/tmp/test.html")
    content_path = "/var/www/html/test.html"
    server.RemoteCommand(f"sudo cp /tmp/test.html {content_path}")
    server.RemoteCommand("sudo mkdir -p /var/www/html/content")
    server.RemoteCommand("sudo cp /tmp/test.html /var/www/html/1.html")
    nginx_wrk_conf = data.ResourcePath(
        posixpath.join(
            FLAGS[f"{nginx.PACKAGE_NAME}_data"].value,
            FLAGS[f"{nginx.PACKAGE_NAME}_conf"].value,
        )
    )

    server.PushDataFile(nginx_wrk_conf, "/tmp/nginx.conf")
    server.RemoteCommand("sudo cp /tmp/nginx.conf /etc/nginx/nginx.conf")

    if FLAGS[f"{BENCHMARK_NAME}_use_ssl"].value:
        _ConfigureNginxForSsl(server)
        FLAGS[f"{BENCHMARK_NAME}_server_port"].value = 443

    server.RemoteCommand(f"sudo {nginx_path}/sbin/nginx -c /etc/nginx/nginx.conf")


def Prepare(benchmark_spec):
    """Install Nginx on the server and a load generator on the clients.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    clients = benchmark_spec.vm_groups["clients"]
    server = benchmark_spec.vm_groups["servers"][0]
    server.Install(nginx.PACKAGE_NAME)
    _ConfigureNginx(server)
    background_tasks.RunThreaded(lambda vm: vm.Install(wrk.PACKAGE_NAME), clients)
    # Allow traffic on all ports on both client and server
    if FLAGS[f"{BENCHMARK_NAME}_use_ssl"].value:
        port = 443
    else:
        port = 80

    for vm in clients + [server]:
        vm.AllowPort(port)
        # Check if firewalld is installed on system by default
        stdout, stderr = vm.RemoteCommand(
            "sudo firewall-cmd --version", ignore_failure=True
        )
        if not stderr:
            vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={port}/tcp --permanent"
            )
            vm.RemoteCommand(f"sudo firewall-cmd --reload")

    benchmark_spec.nginx_endpoint_ip = benchmark_spec.vm_groups["servers"][0].internal_ip


def _Run_nginx_wrk(clients, target):
    """Run single or multiple instances of wrk against a single target."""
    benchmark_metadata = {}
    max_tpt_mode = FLAGS[f"{wrk.PACKAGE_NAME}_latency_capped_throughput"].value
    if max_tpt_mode:
        return _RunMaxTptModeForWrk(benchmark_metadata, clients, target)
    return _RunDefaultModeForWrk(benchmark_metadata, clients, target)

def _RunSingleClient(client, client_number, target):
    """Run wrk from a single client."""
    return wrk.RunWithConnectionsAndThreads(client, client_number, target)

def _RunCustomParamsForWrk(params: Dict, clients, target):
    """Helper function to run wrk with specific connections and threads"""
    # Set wrk flag values for max tpt mode (must be lists)
    FLAGS[f"{wrk.PACKAGE_NAME}_connections"].value = params["connections"]
    FLAGS[f"{wrk.PACKAGE_NAME}_threads"].value = params["threads"]
    FLAGS[f"{wrk.PACKAGE_NAME}_duration"].value = 30
    args = [((client, i, target), {}) for i, client  in enumerate(clients)]
    raw_results = background_tasks.RunThreaded(_RunSingleClient, args)
    return raw_results


def _RunMaxTptModeForWrk(benchmark_metadata, clients, server):
    """Run wrk against nginx with binary search to find a configuration that
        achieves the best aggregate ops throughput under 5ms p95 latency
        Returns:
          - Best throughput sample and config (see _ParseMaxTptResults)
        Notes:
          - connections/threads:  default upper/lower bounds are set by global flags
          - The midpoint of the connections upper/lower bound is tested at each iteration
          - If the current connection value violates 5ms p95,
          connections are lowered and search continues
          - If the current connection value doesn't violate 5ms p95,
          threads are increased until SLA is broken
          - Condition for connections > threads is checked after every change in value
          - The best value/configuration is saved at each stage of the search
        """
    results = []

    lat_cap = FLAGS[f"{wrk.PACKAGE_NAME}_latency_cap"].value

    max_agg = 0
    best_ops_sample = None
    worst_p95_sample = None
    best_results = None
    threads = FLAGS[f"{BENCHMARK_NAME}_threads_list"].value

    for thread in threads:

        conns_lower = FLAGS[f"{BENCHMARK_NAME}_connections_lower_bound"].value
        conns_upper = FLAGS[f"{BENCHMARK_NAME}_connections_upper_bound"].value
        conns_mid = (conns_upper + conns_lower) // 2

        while conns_lower < (conns_upper - 3):

            if conns_mid < thread:
                break
            
            params = {"connections": conns_mid, "threads": thread}
            raw_results = _RunCustomParamsForWrk(params, clients, server)
            agg_sample, p95_sample, results = _ParseDefaultResults(raw_results, benchmark_metadata)
            current_agg, current_p95 = agg_sample.value, p95_sample.value

            if current_p95 > lat_cap:
                conns_upper = conns_mid
                conns_mid = (conns_lower + conns_mid) // 2 
                continue

            elif current_agg > max_agg and current_p95 <= lat_cap:
                max_agg = current_agg
                best_ops_sample = agg_sample
                worst_p95_sample = p95_sample
                best_results = results  

            conns_lower = conns_mid 
            conns_mid = (conns_mid + conns_upper) // 2 

    if best_results is None:
        best_results = results
    best_tpt_sample = _ParseMaxTptResults(
        best_ops_sample, worst_p95_sample, best_results
        )
    return best_tpt_sample

def _RunDefaultModeForWrk(benchmark_metadata, clients, target):
    """Runs Wrk against nginx in default mode
        Returns:
          - aggregate ops throughput
          - p95 latency from all processes
          - all processes
        """
    args = [((client, i, target), {}) for i, client in enumerate(clients)]
    raw_results = background_tasks.RunThreaded(_RunSingleClient, args)
    agg_sample, p95_sample, results = _ParseDefaultResults(
        raw_results, benchmark_metadata
        )
    return [agg_sample] + [p95_sample] + results

def Run(benchmark_spec):
    """Run a benchmark against the Nginx server.
    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    Returns:
      A list of sample.Sample objects.
    """
    clients = benchmark_spec.vm_groups["clients"]
    results = []
    if FLAGS[f"{BENCHMARK_NAME}_use_ssl"].value:
        scheme = "https"
        FLAGS[f"{BENCHMARK_NAME}_server_port"].value = 443
    else:
        scheme = "http"
        FLAGS[f"{BENCHMARK_NAME}_server_port"].value = 80

    hostip = benchmark_spec.nginx_endpoint_ip
    hoststr = (
        f"[{hostip}]"
        if isinstance(ipaddress.ip_address(hostip), ipaddress.IPv6Address)
        else f"{hostip}"
    )
    portstr = (
        ":" + str(FLAGS[f"{BENCHMARK_NAME}_server_port"].value)
        if FLAGS[f"{BENCHMARK_NAME}_server_port"].value
        else ""
    )
    target = f'-H "Accept-Encoding:br" {scheme}://{hoststr}{portstr}/content/1'
    results += _Run_nginx_wrk(clients, target)
    return results


def _ParseDefaultResults(raw_results, benchmark_metadata):
    """Parse raw results and metadata from a wrk run across nginx
    Calculate aggregate results, worst latency results
    Returns:
      tuple containing sample objects and results list
    """
    results = []
    aggregate_ops_tpt = 0
    p95_latency = 0
    for result_sample in raw_results:
        for each_sample in result_sample:
            each_sample.metadata.update(benchmark_metadata)
            if each_sample.metric == "throughput":
                current_tpt = each_sample.value
                current_p95 = each_sample.metadata["p95_latency"]
                aggregate_ops_tpt += current_tpt
                p95_latency = max(p95_latency, current_p95)
            results.append(each_sample)

    agg_ops_sample = sample.Sample(
        metric="Aggregate Ops Throughput",
        value=aggregate_ops_tpt,
        unit="aggregate ops/s",
    )
    p95_sample = sample.Sample(metric="p95_latency", value=p95_latency, unit="ms")

    agg_ops_sample.metadata.update(benchmark_metadata)
    p95_sample.metadata.update(benchmark_metadata)

    return agg_ops_sample, p95_sample, results


def _ParseMaxTptResults(best_ops_sample, worst_p95_sample, best_results):
    """Create a custom sample for max throughput mode results"""
    if best_ops_sample is None and worst_p95_sample is None:
        max_tpt_sample = sample.Sample(
            metric="Cannot converge for given SLA.", value=None, unit="", metadata=None
        )
        connections_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=best_results[0].metadata["connections"],
            unit="connections",
        )
        threads_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=best_results[0].metadata["threads"],
            unit="threads",
        )
        p95_sample = sample.Sample(
            metric="Cannot converge for given SLA.",
            value=None,
            unit="ms",
            metadata=None,
        )
    else:
        max_tpt_sample = sample.Sample(
            metric="Best Aggregate Ops Throughput",
            value=best_ops_sample.value,
            unit="best aggregate ops/s",
            metadata=best_ops_sample.metadata,
        )
        connections_sample = sample.Sample(
            metric="Best Wrk Connections",
            value=best_results[0].metadata["connections"],
            unit="connections",
        )
        threads_sample = sample.Sample(
            metric="Best Wrk Threads",
            value=best_results[0].metadata["threads"],
            unit="threads",
        )
        p95_sample = sample.Sample(
            metric="Worst p95 Latency",
            value=worst_p95_sample.value,
            unit="ms",
            metadata=worst_p95_sample.metadata,
        )

    return (
        [max_tpt_sample]
        + [connections_sample]
        + [threads_sample]
        + [p95_sample]
        + best_results
    )


def Cleanup(benchmark_spec) -> None:
    """Cleanup Nginx and load generators.
    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    client_vms = benchmark_spec.vm_groups["clients"]
    server = benchmark_spec.vm_groups["servers"][0]
    server.Uninstall(nginx.PACKAGE_NAME)
    background_tasks.RunThreaded(lambda vm: vm.Uninstall(wrk.PACKAGE_NAME), client_vms)
