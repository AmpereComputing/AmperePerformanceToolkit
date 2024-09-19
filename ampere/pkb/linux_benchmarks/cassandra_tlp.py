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

"""Runs cassandra.

Cassandra homepage: http://cassandra.apache.org
cassandra-stress tool page:
http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html
"""
import logging
import time

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import background_tasks
from ampere.pkb.linux_packages import cassandra
from ampere.pkb.linux_packages import cassandra_tlp_client


FLAGS = flags.FLAGS

BENCHMARK_NAME = "ampere_cassandra_tlp"


cassandra_latency_capped_throughput = flags.DEFINE_bool(
    f"{BENCHMARK_NAME}_latency_capped_throughput",
    False,
    "Measure latency capped throughput. Use in conjunction with "
    "memtier_latency_cap. Defaults to False. ",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_thread_lower_bound",
    5,
    "Use with max throughput mode, defaults to 0.",
)
flags.DEFINE_integer(
    f"{BENCHMARK_NAME}_thread_upper_bound",
    25,
    "Use with max throughput mode, defaults to 20.",
)

cassandra_latency_cap = flags.DEFINE_float(
    f"{BENCHMARK_NAME}_latency_cap",
    6.0,
    "Latency cap in ms. Use in conjunction with "
    "latency_capped_throughput. Defaults to 1ms.",
)

flags.DEFINE_string(
    f"{BENCHMARK_NAME}_latency_operation",
    "write",
    "check latency capped throughput either on read or write. Defaults to 1ms",
)

BENCHMARK_CONFIG = """
ampere_cassandra_tlp:
  description: Benchmark Cassandra using cassandra-tlp
  vm_groups:
    servers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    clients:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
    """Get the User config file"""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """Install Cassandra and Java on target vms.

    Args:
      benchmark_spec: The benchmark specification. Contains all data that is
          required to run the benchmark.
    """
    logging.info("Preparing data files and Java on all vms.")
    benchmark_spec.always_call_cleanup = True
    cassandra_server_vms = benchmark_spec.vm_groups["servers"][0]
    num_clients = len(benchmark_spec.vm_groups["clients"])
    client_instances = FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_instances"].value
    total_client_instances = num_clients * client_instances
    server_instances = FLAGS[f"{cassandra.PACKAGE_NAME}_instances"].value
    total_heap_size = FLAGS[f"{cassandra.PACKAGE_NAME}_heap_size"].value
    if server_instances == total_client_instances:
        # check heap size based on total server instances
        heap_size, err, _ = cassandra_server_vms.RemoteCommandWithReturnCode(
            "sudo free -t -g | head -n 2 | tail -n 1 |  sed -r 's/[[:blank:]]+/ /g' "
            "| cut -d' ' -f2",
            ignore_failure=True,
        )
        temp_heap_size = (int(heap_size) / server_instances) / 2
        temp1_heap_size = 0
        if total_heap_size != "":
            if "T" in total_heap_size:
                temp1_heap_size = int(total_heap_size[:-1]) * 1024
            elif "M" in total_heap_size:
                temp1_heap_size = int(total_heap_size[:-1]) / 1024
            elif "G" in total_heap_size:
                temp1_heap_size = int(total_heap_size[:-1])
            if temp_heap_size > temp1_heap_size:
                temp_heap_size = temp1_heap_size
        FLAGS[f"{cassandra.PACKAGE_NAME}_heap_size"].value = (
            str(int(temp_heap_size)) + "G"
        )

        # check for numactl on sever and client
        if FLAGS[f"{cassandra.PACKAGE_NAME}_use_numactl"].value:
            if len(FLAGS[f"{cassandra.PACKAGE_NAME}_use_cores"].value) == 0:
                raise ValueError(
                    "ampere_cassandra_server_use_cores flag is empty. "
                    "Add cores for numactl in comma seperated list"
                )
            if server_instances != len(
                FLAGS[f"{cassandra.PACKAGE_NAME}_use_cores"].value
            ):
                raise ValueError(
                    "Number of server instances should be equal to numactl core list"
                    "eg: if number of server instances are 2 then add 2 comma separated"
                    " values in ampere_cassandra_server_use_cores flag"
                )
        if FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_use_numactl"].value:
            if len(FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_use_cores"].value) == 0:
                raise ValueError(
                    "ampere_cassandra_tlp_client_use_cores flag is empty."
                    "Add cores for numactl in comma separated list"
                )
            if total_client_instances != len(
                FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_use_cores"].value
            ):
                raise ValueError(
                    "total clients(Number of clients * client instances) "
                    "should be equal to numactl core list: eg: if number of"
                    " clients are 2 and client instances are 2 then add 4 "
                    "comma separated values in "
                    "ampere_cassandra_tlp_client_use_cores flag"
                )
        cassandra_server_vms.Install(cassandra.PACKAGE_NAME)
        cassandra.CreateInstances(cassandra_server_vms)
        cassandra.Configure(cassandra_server_vms)
        # start all Cassandra instances
        instances = [
            ((cassandra_server_vms, instance), {})
            for instance in range(0, server_instances)
        ]
        server_start_command = background_tasks.RunThreaded(cassandra.Start, instances)
        server_command = "".join(str(x) for x in server_start_command)
        out, err = cassandra_server_vms.RemoteCommand(server_command)
        if "ERROR" in err:
            raise ValueError(f"Cassandra Server is not started {err}")
        if "ERROR" in out:
            raise ValueError(f"Cassandra Server is not started {out}")

        for cl in range(num_clients):
            benchmark_spec.vm_groups["clients"][cl].Install(
                cassandra_tlp_client.PACKAGE_NAME
            )
            cassandra_tlp_client.CreateInstances(
                benchmark_spec.vm_groups["clients"][cl], cl
            )
    else:
        raise ValueError("Number of server instances should be equal to client VM's.")


def Run(benchmark_spec):
    """Run Cassandra on target vms.

    Args:
      benchmark_spec: The benchmark specification. Contains all data
          that is required to run the benchmark.

    Returns:
      A list of sample.Sample objects.
    """

    def DistributeClientsToPorts(client, client_number, thread_num):
        return cassandra_tlp_client.RunCassandraTlpStressOverAllPorts(
            benchmark_spec.vm_groups["servers"], client, client_number, thread_num
        )

    def RunTestOnCassandraClient():
        thread_data_local = FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_threads"].value
        for thread_num_local in thread_data_local:
            args = [
                ((client, i, thread_num_local), {})
                for i, client in enumerate(benchmark_spec.vm_groups["clients"])
            ]
            background_tasks.RunThreaded(DistributeClientsToPorts, args)

    metadata = cassandra_tlp_client.GenerateMetadataFromFlags(
        benchmark_spec.vm_groups["clients"],
        FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_threads"].value,
    )
    # SLA works with single cassandra server instance
    if cassandra_latency_capped_throughput.value:
        thread_lower = FLAGS[f"{BENCHMARK_NAME}_thread_lower_bound"].value
        thread_upper = FLAGS[f"{BENCHMARK_NAME}_thread_upper_bound"].value
        max_agg = 0
        best_qps_sample = None
        worst_p99_sample = None
        other_qps_sample_text = None
        other_p99_sample_text = None
        other_qps_sample = None
        other_p99_sample = None
        while thread_lower <= thread_upper:
            samples = []
            # get thread  midpoint
            thread_mid_array = []
            thread_mid = thread_lower + (thread_upper - thread_lower) // 2
            thread_mid_array.append(thread_mid)
            FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_threads"].value = (
                thread_mid_array
            )
            # giving sleep between 2 runs to bring machine back to normal state
            time.sleep(5)
            RunTestOnCassandraClient()
            time.sleep(5)
            thread_num = FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_threads"].value[0]
            raw_results = cassandra_tlp_client.CollectResults(
                benchmark_spec.vm_groups["clients"],
                thread_num,
            )
            result_data1 = []
            write_agg_qps_sample = None
            write_p99_sample = None
            read_agg_qps_sample = None
            read_p99_sample = None
            num_thread = None
            aggregate_result = []
            aggregate = []
            thread_metadata = cassandra_tlp_client.GenerateMetadataFromFlags(
                benchmark_spec.vm_groups["clients"], thread_num
            )
            for raw_data in raw_results:
                result_data1 = _ParseDefaultResults(
                    raw_data,
                    metadata,
                    thread_num,
                )
                for data in result_data1:
                    aggregate.append(data) 
            aggregate_result = _CalculateAggregateResults(aggregate, thread_metadata)
            if FLAGS[f"{BENCHMARK_NAME}_latency_operation"].value == "write":
                current_agg, current_p99 = aggregate_result[0].value, aggregate_result[2].value
            else:
                current_agg, current_p99 = aggregate_result[1].value,aggregate_result[3].value
            # SLA violated: lower pipelines, continue
            if current_p99 > cassandra_latency_cap.value:
                thread_upper = thread_mid - 1
                continue
            # SLA in bounds: store best
            if current_agg > max_agg:
                max_agg = current_agg
                if FLAGS[f"{BENCHMARK_NAME}_latency_operation"].value == "write":
                    best_qps_sample = aggregate_result[0].value
                    worst_p99_sample = aggregate_result[2].value
                    other_qps_sample_text = "Read Queries per second"
                    other_qps_sample = aggregate_result[1].value
                    other_p99_sample_text = "Read p99 Latency"
                    other_p99_sample = aggregate_result[3].value
                else:
                    best_qps_sample = aggregate_result[1].value
                    worst_p99_sample = aggregate_result[3].value
                    other_qps_sample_text = "Write Queries per second"
                    other_qps_sample = aggregate_result[0].value
                    other_p99_sample_text = "Write p99 Latency"
                    other_p99_sample = aggregate_result[2].value
            thread_lower = thread_mid + 1
        best_qps_sample = _ParseMaxTptResults(
            best_qps_sample,
            worst_p99_sample,
            FLAGS[f"{BENCHMARK_NAME}_latency_operation"].value,
            metadata,
            other_qps_sample_text,
            other_qps_sample,
            other_p99_sample_text,
            other_p99_sample,
            thread_num,
        )
        return best_qps_sample
    else:
        RunTestOnCassandraClient()
        results = []
        aggregate_result = []
        thread_data = FLAGS[f"{cassandra_tlp_client.PACKAGE_NAME}_threads"].value
        for thread_num in thread_data:
            aggregate = []
            thread_metadata = cassandra_tlp_client.GenerateMetadataFromFlags(
                benchmark_spec.vm_groups["clients"], thread_num
            )
            raw_results = cassandra_tlp_client.CollectResults(
                benchmark_spec.vm_groups["clients"],
                thread_num,
            )
            for raw_data in raw_results:
                result_data = _ParseDefaultResults(
                    raw_data, thread_metadata, thread_num
                )
                for data in result_data:
                    results.append(data)
                    aggregate.append(data)
            aggregate_result = _CalculateAggregateResults(aggregate, thread_metadata)
            for agg_data in aggregate_result:
                results.append(agg_data)

        return results


def _CalculateAggregateResults(aggregate_data, metadata):
    """Calculate aggregate results and worst latency
    Return:
      tuple containing sample objects
    """
    results = []
    write_aggregate = 0
    read_aggregate = 0
    write_worst_latency = 0
    read_worst_latency = 0
    for sample1 in aggregate_data:
        if "Write Queries per second" in sample1.metric:
            if sample1.value == 0.0:
                write_aggregate = 0.0
                read_aggregate = 0.0
                write_worst_latency = 0.0
                read_worst_latency = 0.0
                break
            else:
                current_aggregate = sample1.value
                write_aggregate += current_aggregate
        if "Read Queries per second" in sample1.metric:
            current_aggregate = sample1.value
            read_aggregate += current_aggregate
        if "Write p99_latency" in sample1.metric:
            current_latency = sample1.value
            write_worst_latency = max(write_worst_latency, current_latency)
        if "Read p99_latency" in sample1.metric:
            current_latency = sample1.value
            read_worst_latency = max(read_worst_latency, current_latency)
     
    agg_write_sample = sample.Sample(
        metric="Aggregate Write Queries per second",
        value=write_aggregate,
        unit="write aggregate q/s",
        metadata=metadata,
    )
    results.append(agg_write_sample)
    agg_read_sample = sample.Sample(
        metric="Aggregate Read Queries per second",
        value=read_aggregate,
        unit="read aggregate q/s",
        metadata=metadata,
    )
    results.append(agg_read_sample)
    write_worst_latency_sample = sample.Sample(
        metric="Worst Write p99 Latency",
        value=write_worst_latency,
        unit="ms",
        metadata=metadata,
    )
    results.append(write_worst_latency_sample)
    read_worst_latency_sample = sample.Sample(
        metric="Worst Read p99 Latency",
        value=read_worst_latency,
        unit="ms",
        metadata=metadata,
    )
    results.append(read_worst_latency_sample)
    return results


def _ParseDefaultResults(raw_results, metadata, thread_num):
    """Parse raw results and metadata from a cassandra run across all cassandra stress processes
    all individual process results
    Returns:
      tuple containing sample objects and results list
    """
    write_agg_qps = 0
    write_p99_latency = 0
    read_agg_qps = 0
    read_p99_latency = 0
    instance_value = 0
    for result_sample in raw_results:
        if result_sample.metric == str(thread_num) + "_Number of Instances":
            instance_value = result_sample.value
        if result_sample.metric == str(thread_num) + "_Write Ops Throughput":
            write_agg_qps = result_sample.value
        if result_sample.metric == str(thread_num) + "_write_p99_latency":
            write_p99_latency = result_sample.value
        if result_sample.metric == str(thread_num) + "_Read Ops Throughput":
            read_agg_qps = result_sample.value
        if result_sample.metric == str(thread_num) + "_read_p99_latency":
            read_p99_latency = result_sample.value

    return [
        sample.Sample("Instance Number", instance_value, "", metadata),
        sample.Sample("Number of Thread", thread_num, "", metadata),
        sample.Sample("Write Queries per second", write_agg_qps, "q/s", metadata),
        sample.Sample("Write p99_latency", write_p99_latency, "ms", metadata),
        sample.Sample("Read Queries per second", read_agg_qps, "q/s", metadata),
        sample.Sample("Read p99_latency", read_p99_latency, "ms", metadata),
    ]


def _ParseMaxTptResults(
    best_qps_sample,
    worst_p99_sample,
    latency_operation,
    metadata,
    other_qps_sample_text,
    other_qps_sample,
    other_p99_sample_text,
    other_p99_sample,
    thread_value,
):
    """Create a custom sample for max throughput mode results"""
    max_qps_sample = sample.Sample(
        metric="Best " + latency_operation + " Queries per second",
        value=best_qps_sample,
        unit="best q/s",
        metadata=metadata,
    )

    threads_sample = sample.Sample(
        metric="Best Cassandra Tlp Thread",
        value=thread_value,
        unit="cassandra_tlp_threads",
    )

    p99_sample = sample.Sample(
        metric="Worst " + latency_operation + "p99 Latency",
        value=worst_p99_sample,
        unit="ms",
        metadata=metadata,
    )
    other_qps_sample = sample.Sample(
        metric=other_qps_sample_text, value=other_qps_sample, unit="", metadata=metadata
    )

    other_p99_sample = sample.Sample(
        metric=other_p99_sample_text,
        value=other_p99_sample,
        unit="ms",
        metadata=metadata,
    )

    return (
        [max_qps_sample]
        + [threads_sample]
        + [p99_sample]
        + [other_qps_sample]
        + [other_p99_sample]
    )


def Cleanup(benchmark_spec):
    """Cleanup function.

    Args:
      benchmark_spec: The benchmark specification. Contains all data
          that is required to run the benchmark.
    """
    # vm_dict = benchmark_spec.vm_groups
    # cassandra_vms = vm_dict[cassandra_group]
    cassandra_server_vms = benchmark_spec.vm_groups["servers"][0]
    no_of_instances = FLAGS[f"{cassandra.PACKAGE_NAME}_instances"].value
    cassandra.Stop(cassandra_server_vms)
    cassandra.CleanNode(cassandra_server_vms, no_of_instances)
    num_clients = len(benchmark_spec.vm_groups["clients"])
    for cl in range(num_clients):
        cassandra_tlp_client.Stop(benchmark_spec.vm_groups["clients"][cl])
