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
"""Install, execute, and parse results from YCSB.

YCSB (the Yahoo! Cloud Serving Benchmark) is a common method of comparing NoSQL
database performance.
https://github.com/brianfrankcooper/YCSB

For PerfKitBenchmarker, we wrap YCSB to:

  * Pre-load a database with a fixed number of records.
  * Execute a collection of workloads under a staircase load.
  * Parse the results into PerfKitBenchmarker samples.

The 'YCSBExecutor' class handles executing YCSB on a collection of client VMs.
Generally, clients just need this class. For example, to run against
HBase 1.0:

  >>> executor = ycsb.YCSBExecutor('hbase-10')
  >>> samples = executor.LoadAndRun(loader_vms)

By default, this runs YCSB workloads A and B against the database, 32 threads
per client VM, with an initial database size of 1GB (1k records).
Each workload runs for at most 30 minutes.
"""

import bisect
import collections
from collections.abc import Iterable, Mapping, Sequence
import copy
import csv
import dataclasses
import io
import itertools
import json
import logging
import math
import operator
import os
import posixpath
import re
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import maven

FLAGS = flags.FLAGS

YCSB_URL_TEMPLATE = (
    'https://github.com/brianfrankcooper/YCSB/releases/'
    'download/{0}/ycsb-{0}.tar.gz'
)
YCSB_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'ycsb')
YCSB_EXE = posixpath.join(YCSB_DIR, 'bin', 'ycsb')
HDRHISTOGRAM_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'hdrhistogram')
HDRHISTOGRAM_TAR_URL = (
    'https://github.com/HdrHistogram/HdrHistogram/archive/'
    'HdrHistogram-2.1.10.tar.gz'
)
HDRHISTOGRAM_GROUPS = ['READ', 'UPDATE']

_DEFAULT_PERCENTILES = 50, 75, 90, 95, 99, 99.9

HISTOGRAM = 'histogram'
HDRHISTOGRAM = 'hdrhistogram'
TIMESERIES = 'timeseries'
YCSB_MEASUREMENT_TYPES = [HISTOGRAM, HDRHISTOGRAM, TIMESERIES]

# Binary operators to aggregate reported statistics.
# Statistics with operator 'None' will be dropped.
AGGREGATE_OPERATORS = {
    'Operations': operator.add,
    'RunTime(ms)': max,
    'Return=0': operator.add,
    'Return=-1': operator.add,
    'Return=-2': operator.add,
    'Return=-3': operator.add,
    'Return=OK': operator.add,
    'Return=ERROR': operator.add,
    'Return=NOT_FOUND': operator.add,
    'LatencyVariance(ms)': None,
    'AverageLatency(ms)': None,  # Requires both average and # of ops.
    'Throughput(ops/sec)': operator.add,
    '95thPercentileLatency(ms)': None,  # Calculated across clients.
    '99thPercentileLatency(ms)': None,  # Calculated across clients.
    'MinLatency(ms)': min,
    'MaxLatency(ms)': max,
}

flags.DEFINE_string(
    'ycsb_version', '0.17.0', 'YCSB version to use. Defaults to version 0.17.0.'
)
flags.DEFINE_string(
    'ycsb_tar_url',
    None,
    'URL to a YCSB tarball to use instead of the releases located on github.',
)
flags.DEFINE_enum(
    'ycsb_measurement_type',
    HISTOGRAM,
    YCSB_MEASUREMENT_TYPES,
    'Measurement type to use for ycsb. Defaults to histogram.',
)
flags.DEFINE_enum(
    'ycsb_measurement_interval',
    'op',
    ['op', 'intended', 'both'],
    'Measurement interval to use for ycsb. Defaults to op.',
)
flags.DEFINE_boolean(
    'ycsb_histogram',
    False,
    'Include individual '
    'histogram results from YCSB (will increase sample '
    'count).',
)
flags.DEFINE_boolean(
    'ycsb_load_samples', True, 'Include samples from pre-populating database.'
)
flags.DEFINE_boolean(
    'ycsb_skip_load_stage',
    False,
    'If True, skip the data '
    'loading stage. It can be used when the database target '
    'already exists with pre-populated data.',
)
flags.DEFINE_boolean(
    'ycsb_skip_run_stage',
    False,
    'If True, skip the workload '
    'running stage. It can be used when you want to '
    'pre-populate a database target.',
)
flags.DEFINE_boolean(
    'ycsb_include_individual_results',
    False,
    'Include results from each client VM, rather than just combined results.',
)
flags.DEFINE_boolean(
    'ycsb_reload_database',
    True,
    'Reload database, otherwise skip load stage. '
    'Note, this flag is only used if the database '
    'is already loaded.',
)
flags.DEFINE_integer('ycsb_client_vms', 1, 'Number of YCSB client VMs.')
flags.DEFINE_list(
    'ycsb_workload_files',
    ['workloada', 'workloadb'],
    'Path to YCSB workload file to use during *run* '
    'stage only. Comma-separated list',
)
flags.DEFINE_list(
    'ycsb_load_parameters',
    [],
    'Passed to YCSB during the load stage. Comma-separated list '
    'of "key=value" pairs.',
)
flags.DEFINE_list(
    'ycsb_run_parameters',
    [],
    'Passed to YCSB during the run stage. Comma-separated list '
    'of "key=value" pairs.',
)
_THROUGHPUT_TIME_SERIES = flags.DEFINE_bool(
    'ycsb_throughput_time_series',
    False,
    'If true, run prints status which includes a throughput time series (1s '
    'granularity), and includes the results in the samples.',
)
flags.DEFINE_list(
    'ycsb_threads_per_client',
    ['32'],
    'Number of threads per '
    'loader during the benchmark run. Specify a list to vary the '
    'number of clients. For each thread count, optionally supply '
    'target qps per client, which cause ycsb to self-throttle.',
)
flags.DEFINE_integer(
    'ycsb_preload_threads',
    None,
    'Number of threads per '
    'loader during the initial data population stage. '
    'Default value depends on the target DB.',
)
flags.DEFINE_integer(
    'ycsb_record_count',
    None,
    'Pre-load with a total '
    'dataset of records total. Overrides recordcount value in '
    'all workloads of this run. Defaults to None, where '
    'recordcount value in each workload is used. If neither '
    'is not set, ycsb default of 0 is used.',
)
flags.DEFINE_integer(
    'ycsb_operation_count', None, 'Number of operations *per client VM*.'
)
flags.DEFINE_integer(
    'ycsb_timelimit',
    1800,
    'Maximum amount of time to run '
    'each workload / client count combination in seconds. '
    'Set to 0 for unlimited time.',
)
flags.DEFINE_integer(
    'ycsb_field_count',
    10,
    'Number of fields in a record. '
    'Defaults to 10, which is the default in ycsb v0.17.0.',
)
flags.DEFINE_integer(
    'ycsb_field_length',
    None,
    'Size of each field. Defaults to None which uses the ycsb default of 100.',
)
flags.DEFINE_enum(
    'ycsb_requestdistribution',
    None,
    ['uniform', 'zipfian', 'latest'],
    'Type of request distribution.  '
    'This will overwrite workload file parameter',
)
flags.DEFINE_float(
    'ycsb_readproportion',
    None,
    'The read proportion, Default is 0.5 in workloada and 0.95 in YCSB.',
)
flags.DEFINE_float(
    'ycsb_updateproportion',
    None,
    'The update proportion, Default is 0.5 in workloada and 0.05 in YCSB.',
)
flags.DEFINE_float(
    'ycsb_scanproportion',
    None,
    'The scan proportion, Default is 0 in workloada and 0 in YCSB.',
)
flags.DEFINE_boolean(
    'ycsb_dynamic_load',
    False,
    'Apply dynamic load to system under test and find out '
    'maximum sustained throughput (test length controlled by '
    'ycsb_operation_count and ycsb_timelimit) the '
    'system capable of handling. ',
)
flags.DEFINE_integer(
    'ycsb_dynamic_load_throughput_lower_bound',
    None,
    'Apply dynamic load to system under test. '
    'If not supplied, test will halt once reaching '
    'sustained load, otherwise, will keep running until '
    'reaching lower bound.',
)
flags.DEFINE_float(
    'ycsb_dynamic_load_sustain_throughput_ratio',
    0.95,
    'To consider throughput sustainable when applying '
    'dynamic load, the actual overall throughput measured '
    'divided by target throughput applied should exceed '
    'this ratio. If not, we will lower target throughput and '
    'retry.',
)
flags.DEFINE_integer(
    'ycsb_dynamic_load_sustain_timelimit',
    300,
    'Run duration in seconds for each throughput target '
    'if we have already reached sustained throughput.',
)
flags.DEFINE_integer(
    'ycsb_sleep_after_load_in_sec',
    0,
    'Sleep duration in seconds between load and run stage.',
)
_BURST_LOAD_MULTIPLIER = flags.DEFINE_integer(
    'ycsb_burst_load',
    None,
    'If set, applies burst load to the system, by running YCSB once, and then '
    'immediately running again with --ycsb_burst_load times the '
    'amount of load specified by the `target` parameter. Set to -1 for '
    'the max throughput from the client.',
)
_INCREMENTAL_TARGET_QPS = flags.DEFINE_integer(
    'ycsb_incremental_load',
    None,
    'If set, applies an incrementally increasing load until the target QPS is '
    'reached. This should be the aggregate load for all VMs. Running with '
    'this flag requires that there is not a QPS target passed in through '
    '--ycsb_run_parameters.',
)
_SHOULD_RECORD_COMMAND_LINE = flags.DEFINE_boolean(
    'ycsb_record_command_line',
    True,
    'Whether to record the command line used for kicking off the runs as part '
    'of metadata. When there are many VMs, this can get long and clutter the '
    'PKB log.',
)
_SHOULD_FAIL_ON_INCOMPLETE_LOADING = flags.DEFINE_boolean(
    'ycsb_fail_on_incomplete_loading',
    False,
    'Whether to fail the benchmarking if loading is not complete, '
    'e.g., there are insert failures.',
)
_INCOMPLETE_LOADING_METRIC = flags.DEFINE_string(
    'ycsb_insert_error_metric',
    'insert Return=ERROR',
    'Used with --ycsb_fail_on_incomplete_loading. Will fail the benchmark if '
    "this metric's value is non-zero. This metric should be an indicator of "
    'incomplete table loading. If insertion retries are enabled via '
    'core_workload_insertion_retry_limit, then the default metric may be '
    'non-zero even though the retried insertion eventually succeeded.',
)
_ERROR_RATE_THRESHOLD = flags.DEFINE_float(
    'ycsb_max_error_rate',
    1.00,
    'The maximum error rate allowed for the run. '
    'By default, this allows any number of errors.',
)

# Status line pattern
_STATUS_PATTERN = r'(\d+) sec: \d+ operations; (\d+.\d+) current ops\/sec'
# Status interval default is 10 sec, change to 1 sec.
_STATUS_INTERVAL_SEC = 1

# Default loading thread count for non-batching backends.
DEFAULT_PRELOAD_THREADS = 32

# Customer YCSB tar url. If not set, the official YCSB release will be used.
_ycsb_tar_url = None

# Parameters for incremental workload. Can be made into flags in the future.
_INCREMENTAL_STARTING_QPS = 500
_INCREMENTAL_TIMELIMIT_SEC = 60 * 5

_ThroughputTimeSeries = dict[int, float]
# Tuple of (percentile, latency, count)
_HdrHistogramTuple = tuple[float, float, int]


def SetYcsbTarUrl(url):
  global _ycsb_tar_url
  _ycsb_tar_url = url


def _GetVersion(version_str):
  """Returns the version from ycsb version string.

  Args:
    version_str: ycsb version string with format '0.<version>.0'.

  Returns:
    (int) version.
  """
  return int(version_str.split('.')[1])


def _GetVersionFromUrl(url):
  """Returns the version from ycsb url string.

  Args:
    url: ycsb url string with format
      'https://github.com/brianfrankcooper/YCSB/releases/'
      'download/0.<version>.0/ycsb-0.<version>.0.tar.gz' OR
      'https://storage.googleapis.com/<ycsb_client_jar>/ycsb-0.<version>.0.tar.gz'
      OR
      'https://storage.googleapis.com/externally_shared_files/ycsb-0.<version>.0-SNAPSHOT.tar.gz'

  Returns:
    (int) version.
  """
  # matches ycsb-0.<version>.0
  match = re.search(r'ycsb-0\.\d{2}\.0', url)
  return _GetVersion(match.group(0).strip('ycsb-'))


def _GetThreadsQpsPerLoaderList():
  """Returns the list of [client, qps] per VM to use in staircase load."""

  def _FormatThreadQps(thread_qps):
    thread_qps_pair = thread_qps.split(':')
    if len(thread_qps_pair) == 1:
      thread_qps_pair.append(0)
    return [int(val) for val in thread_qps_pair]

  return [
      _FormatThreadQps(thread_qps)
      for thread_qps in FLAGS.ycsb_threads_per_client
  ]


def GetWorkloadFileList() -> list[str]:
  """Returns the list of workload files to run.

  Returns:
    In order of preference:
      * The argument to --ycsb_workload_files.
      * Bundled YCSB workloads A and B.
  """
  return [data.ResourcePath(workload) for workload in FLAGS.ycsb_workload_files]


def _GetRunParameters() -> dict[str, str]:
  """Returns a dict of params from the --ycsb_run_parameters flag."""
  result = {}
  for kv in FLAGS.ycsb_run_parameters:
    param, value = kv.split('=', 1)
    result[param] = value
  return result


def CheckPrerequisites():
  """Verifies that the workload files are present and parameters are valid.

  Raises:
    IOError: On missing workload file.
    errors.Config.InvalidValue on unsupported YCSB version or configs.
  """
  for workload_file in GetWorkloadFileList():
    if not os.path.exists(workload_file):
      raise IOError('Missing workload file: {0}'.format(workload_file))

  if _ycsb_tar_url:
    ycsb_version = _GetVersionFromUrl(_ycsb_tar_url)
  elif FLAGS.ycsb_tar_url:
    ycsb_version = _GetVersionFromUrl(FLAGS.ycsb_tar_url)
  else:
    ycsb_version = _GetVersion(FLAGS.ycsb_version)

  if ycsb_version < 17:
    raise errors.Config.InvalidValue('must use YCSB version 0.17.0 or higher.')

  run_params = _GetRunParameters()

  # Following flags are mutully exclusive.
  run_target = 'target' in run_params
  per_thread_target = any(
      [':' in thread_qps for thread_qps in FLAGS.ycsb_threads_per_client]
  )
  dynamic_load = FLAGS.ycsb_dynamic_load

  if run_target + per_thread_target + dynamic_load > 1:
    raise errors.Config.InvalidValue(
        'Setting YCSB target in ycsb_threads_per_client '
        'or ycsb_run_parameters or applying ycsb_dynamic_load_* flags'
        ' are mutally exclusive.'
    )

  if FLAGS.ycsb_dynamic_load_throughput_lower_bound and not dynamic_load:
    raise errors.Config.InvalidValue(
        'To apply dynamic load, set --ycsb_dynamic_load.'
    )

  if _BURST_LOAD_MULTIPLIER.value and not run_target:
    raise errors.Config.InvalidValue(
        'Running in burst mode requires setting a target QPS using '
        '--ycsb_run_parameters=target=qps. Got None.'
    )

  if _INCREMENTAL_TARGET_QPS.value and run_target:
    raise errors.Config.InvalidValue(
        'Running in incremental mode requires setting a target QPS using '
        '--ycsb_incremental_load=target and not --ycsb_run_parameters.'
    )


@vm_util.Retry(poll_interval=1)
def Install(vm):
  """Installs the YCSB and, if needed, hdrhistogram package on the VM."""
  vm.Install('openjdk')
  # TODO(user): replace with Python 3 when supported.
  # https://github.com/brianfrankcooper/YCSB/issues/1459
  vm.Install('python')
  vm.InstallPackages('curl')
  ycsb_url = (
      _ycsb_tar_url
      or FLAGS.ycsb_tar_url
      or YCSB_URL_TEMPLATE.format(FLAGS.ycsb_version)
  )
  install_cmd = (
      'mkdir -p {0} && curl -L {1} | '
      'tar -C {0} --strip-components=1 -xzf - '
      # Log4j 2 < 2.16 is vulnerable to
      # https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228.
      # YCSB currently ships with a number of vulnerable jars. None are used by
      # PKB, so simply exclude them.
      # After https://github.com/brianfrankcooper/YCSB/pull/1583 is merged and
      # released, this will not be necessary.
      # TODO(user): Update minimum YCSB version and remove.
      "--exclude='**/log4j-core-2*.jar' "
  )
  vm.RemoteCommand(install_cmd.format(YCSB_DIR, ycsb_url))
  if _GetVersion(FLAGS.ycsb_version) >= 11:
    vm.Install('maven')
    vm.RemoteCommand(install_cmd.format(HDRHISTOGRAM_DIR, HDRHISTOGRAM_TAR_URL))
    # _JAVA_OPTIONS needed to work around this issue:
    # https://stackoverflow.com/questions/53010200/maven-surefire-could-not-find-forkedbooter-class
    # https://stackoverflow.com/questions/34170811/maven-connection-reset-error
    vm.RemoteCommand(
        'cd {hist_dir} && _JAVA_OPTIONS=-Djdk.net.URLClassPath.'
        'disableClassPathURLCheck=true,https.protocols=TLSv1.2 '
        '{mvn_cmd}'.format(
            hist_dir=HDRHISTOGRAM_DIR, mvn_cmd=maven.GetRunCommand('install')
        )
    )


@dataclasses.dataclass
class _OpResult:
  """Individual results for a single operation.

  Attributes:
    group: group name (e.g., update, insert, overall)
    statistics: dict mapping from statistic name to value
    data_type: Corresponds to --ycsb_measurement_type.
    data: For HISTOGRAM/HDRHISTOGRAM: list of (ms_lower_bound, count) tuples,
      e.g. [(0, 530), (19, 1)] indicates that 530 ops took between 0ms and 1ms,
      and 1 took between 19ms and 20ms. Empty bins are not reported. For
      TIMESERIES: list of (time, latency us) tuples.
  """

  group: str = ''
  data_type: str = ''
  data: list[tuple[int, float]] = dataclasses.field(default_factory=list)
  statistics: dict[str, float] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class YcsbResult:
  """Aggregate results for the YCSB run.

  Attributes:
    client: Contains YCSB version information.
    command_line: Command line executed.
    throughput_time_series: Time series of throughputs (interval, QPS).
    groups: dict of operation group name to results for that operation.
  """

  client: str = ''
  command_line: str = ''
  throughput_time_series: _ThroughputTimeSeries = dataclasses.field(
      default_factory=dict
  )
  groups: dict[str, _OpResult] = dataclasses.field(default_factory=dict)


def ParseResults(
    ycsb_result_string: str, data_type: str = 'histogram'
) -> 'YcsbResult':
  """Parse YCSB results.

  Example input for histogram datatype:

    YCSB Client 0.1
    Command line: -db com.yahoo.ycsb.db.HBaseClient -P /tmp/pkb/workloada
    [OVERALL], RunTime(ms), 1800413.0
    [OVERALL], Throughput(ops/sec), 2740.503428935472
    [UPDATE], Operations, 2468054
    [UPDATE], AverageLatency(us), 2218.8513395574005
    [UPDATE], MinLatency(us), 554
    [UPDATE], MaxLatency(us), 352634
    [UPDATE], 95thPercentileLatency(ms), 4
    [UPDATE], 99thPercentileLatency(ms), 7
    [UPDATE], Return=0, 2468054
    [UPDATE], 0, 398998
    [UPDATE], 1, 1015682
    [UPDATE], 2, 532078
    ...

  Example input for hdrhistogram datatype:

    YCSB Client 0.17.0
    Command line: -db com.yahoo.ycsb.db.RedisClient -P /opt/pkb/workloadb
    [OVERALL], RunTime(ms), 29770.0
    [OVERALL], Throughput(ops/sec), 33590.86328518643
    [UPDATE], Operations, 49856.0
    [UPDATE], AverageLatency(us), 1478.0115532734276
    [UPDATE], MinLatency(us), 312.0
    [UPDATE], MaxLatency(us), 24623.0
    [UPDATE], 95thPercentileLatency(us), 3501.0
    [UPDATE], 99thPercentileLatency(us), 6747.0
    [UPDATE], Return=OK, 49856
    ...

  Example input for ycsb version 0.17.0+:

    ...
    Command line: -db com.yahoo.ycsb.db.HBaseClient10 ... -load
    YCSB Client 0.17.0

    Loading workload...
    Starting test.
    ...
    [OVERALL], RunTime(ms), 11411
    [OVERALL], Throughput(ops/sec), 8763.473841030585
    [INSERT], Operations, 100000
    [INSERT], AverageLatency(us), 74.92
    [INSERT], MinLatency(us), 5
    [INSERT], MaxLatency(us), 98495
    [INSERT], 95thPercentileLatency(us), 42
    [INSERT], 99thPercentileLatency(us), 1411
    [INSERT], Return=OK, 100000
    ...

  Example input for timeseries datatype:

    ...
    [OVERALL], RunTime(ms), 240007.0
    [OVERALL], Throughput(ops/sec), 10664.605615669543
    ...
    [READ], Operations, 1279253
    [READ], AverageLatency(us), 3002.7057071587874
    [READ], MinLatency(us), 63
    [READ], MaxLatency(us), 93584
    [READ], Return=OK, 1279281
    [READ], 0, 528.6142757498257
    [READ], 500, 360.95347448674966
    [READ], 1000, 667.7379547689283
    [READ], 1500, 731.5389357265888
    [READ], 2000, 778.7992281717318
    ...

  Args:
    ycsb_result_string: str. Text output from YCSB.
    data_type: Either 'histogram' or 'timeseries' or 'hdrhistogram'. 'histogram'
      and 'hdrhistogram' datasets are in the same format, with the difference
      being lacking the (millisec, count) histogram component. Hence are parsed
      similarly.

  Returns:
    A YcsbResult object that contains the results from parsing YCSB output.
  Raises:
    IOError: If the results contained unexpected lines.
  """
  if (
      'redis.clients.jedis.exceptions.JedisConnectionException'
      in ycsb_result_string
  ):
    # This error is cause by ycsb using an old version of redis client 2.9.0
    # https://github.com/xetorthio/jedis/issues/1977
    raise errors.Benchmarks.KnownIntermittentError(
        'errors.Benchmarks.KnownIntermittentError'
    )

  lines = []
  client_string = 'YCSB'
  command_line = 'unknown'
  throughput_time_series = {}
  fp = io.StringIO(ycsb_result_string)
  result_string = next(fp).strip()

  def IsHeadOfResults(line):
    return line.startswith('[OVERALL]')

  while not IsHeadOfResults(result_string):
    if result_string.startswith('YCSB Client 0.'):
      client_string = result_string
    if result_string.startswith('Command line:'):
      command_line = result_string
    # Look for status lines which include throughput on a 1-sec basis.
    match = re.search(_STATUS_PATTERN, result_string)
    if match is not None:
      timestamp, qps = int(match.group(1)), float(match.group(2))
      # Repeats in the printed status are erroneous, ignore.
      if timestamp not in throughput_time_series:
        throughput_time_series[timestamp] = qps
    try:
      result_string = next(fp).strip()
    except StopIteration:
      raise IOError(
          f'Could not parse YCSB output: {ycsb_result_string}'
      ) from None

  if result_string.startswith('[OVERALL]'):  # YCSB > 0.7.0.
    lines.append(result_string)
  else:
    # Received unexpected header
    raise IOError(f'Unexpected header: {client_string}')

  # Some databases print additional output to stdout.
  # YCSB results start with [<OPERATION_NAME>];
  # filter to just those lines.
  def LineFilter(line):
    return re.search(r'^\[[A-Z]+\]', line) is not None

  lines = itertools.chain(lines, filter(LineFilter, fp))

  r = csv.reader(lines)

  by_operation = itertools.groupby(r, operator.itemgetter(0))

  result = YcsbResult(
      client=client_string,
      command_line=command_line,
      throughput_time_series=throughput_time_series,
  )

  for operation, lines in by_operation:
    operation = operation[1:-1].lower()

    if operation == 'cleanup':
      continue

    op_result = _OpResult(group=operation, data_type=data_type)
    latency_unit = 'ms'
    for _, name, val in lines:
      name = name.strip()
      val = val.strip()
      # Drop ">" from ">1000"
      if name.startswith('>'):
        name = name[1:]
      val = float(val) if '.' in val or 'nan' in val.lower() else int(val)
      if name.isdigit():
        if val:
          if data_type == TIMESERIES and latency_unit == 'us':
            val /= 1000.0
          op_result.data.append((int(name), val))
      else:
        if '(us)' in name:
          name = name.replace('(us)', '(ms)')
          val /= 1000.0
          latency_unit = 'us'
        op_result.statistics[name] = val

    result.groups[operation] = op_result
  _ValidateErrorRate(result)
  return result


def _ValidateErrorRate(result: YcsbResult) -> None:
  """Raises an error if results contains entries with too high error rate.

  Computes the error rate for each operation, example output looks like:

    [INSERT], Operations, 100
    [INSERT], AverageLatency(us), 74.92
    [INSERT], MinLatency(us), 5
    [INSERT], MaxLatency(us), 98495
    [INSERT], 95thPercentileLatency(us), 42
    [INSERT], 99thPercentileLatency(us), 1411
    [INSERT], Return=OK, 90
    [INSERT], Return=ERROR, 10

  This function will then compute 10/100 = 0.1 error rate.

  Args:
    result: The result of running ParseResults()

  Raises:
    errors.Benchmarks.RunError: If the computed error rate is higher than the
      threshold.
  """
  for operation in result.groups.values():
    name, stats = operation.group, operation.statistics
    # The operation count can be 0
    count = stats.get('Operations', 0)
    if count == 0:
      continue
    # These keys may be missing from the output.
    error_rate = stats.get('Return=ERROR', 0) / count
    if error_rate > _ERROR_RATE_THRESHOLD.value:
      raise errors.Benchmarks.RunError(
          f'YCSB had a {error_rate} error rate for {name}, higher than '
          f'threshold {_ERROR_RATE_THRESHOLD.value}'
      )


def ParseHdrLogFile(logfile: str) -> list[_HdrHistogramTuple]:
  """Parse a hdrhistogram log file into a list of (percentile, latency, count).

  Example decrypted hdrhistogram logfile (value measures latency in microsec):

  #[StartTime: 1523565997 (seconds since epoch), Thu Apr 12 20:46:37 UTC 2018]
       Value     Percentile TotalCount 1/(1-Percentile)

     314.000 0.000000000000          2           1.00
     853.000 0.100000000000      49955           1.11
     949.000 0.200000000000     100351           1.25
     1033.000 0.300000000000     150110           1.43
     ...
     134271.000 0.999998664856    1000008      748982.86
     134271.000 0.999998855591    1000008      873813.33
     201983.000 0.999999046326    1000009     1048576.00
  #[Mean    =     1287.159, StdDeviation   =      667.560]
  #[Max     =   201983.000, Total count    =      1000009]
  #[Buckets =            8, SubBuckets     =         2048]

  Example of output:
     [(0, 0.314, 2), (10, 0.853, 49953), (20, 0.949, 50396), ...]

  Args:
    logfile: Hdrhistogram log file.

  Returns:
    List of (percentile, value, count) tuples
  """
  result = []
  last_percent_value = -1
  prev_total_count = 0
  for row in logfile.split('\n'):
    if re.match(r'( *)(\d|\.)( *)', row):
      row_vals = row.split()
      # convert percentile to 100 based and round up to 3 decimal places
      percentile = math.floor(float(row_vals[1]) * 100000) / 1000.0
      current_total_count = int(row_vals[2])
      if (
          percentile > last_percent_value
          and current_total_count > prev_total_count
      ):
        # convert latency to millisec based and percentile to 100 based.
        latency = float(row_vals[0]) / 1000
        count = current_total_count - prev_total_count
        result.append((percentile, latency, count))
        last_percent_value = percentile
        prev_total_count = current_total_count
  return result


def ParseHdrLogs(
    hdrlogs: Mapping[str, str]
) -> dict[str, list[_HdrHistogramTuple]]:
  """Parse a dict of group to hdr logs into a dict of group to histogram tuples.

  Args:
    hdrlogs: Dict of group (read or update) to hdr logs for that group.

  Returns:
    Dict of group to histogram tuples of reportable percentile values.
  """
  parsed_hdr_histograms = {}
  for group, logfile in hdrlogs.items():
    values = ParseHdrLogFile(logfile)
    parsed_hdr_histograms[group] = values
  return parsed_hdr_histograms


def _CumulativeSum(xs):
  total = 0
  for x in xs:
    total += x
    yield total


def _WeightedQuantile(x, weights, p):
  """Weighted quantile measurement for an ordered list.

  This method interpolates to the higher value when the quantile is not a direct
  member of the list. This works well for YCSB, since latencies are floored.

  Args:
    x: List of values.
    weights: List of numeric weights.
    p: float. Desired quantile in the interval [0, 1].

  Returns:
    float.

  Raises:
    ValueError: When 'x' and 'weights' are not the same length, or 'p' is not in
      the interval [0, 1].
  """
  if len(x) != len(weights):
    raise ValueError(
        'Lengths do not match: {0} != {1}'.format(len(x), len(weights))
    )
  if p < 0 or p > 1:
    raise ValueError('Invalid quantile: {0}'.format(p))
  n = sum(weights)
  target = n * float(p)
  cumulative = list(_CumulativeSum(weights))

  # Find the first cumulative weight >= target
  i = bisect.bisect_left(cumulative, target)
  if i == len(x):
    return x[-1]
  else:
    return x[i]


def _PercentilesFromHistogram(ycsb_histogram, percentiles=_DEFAULT_PERCENTILES):
  """Calculate percentiles for from a YCSB histogram.

  Args:
    ycsb_histogram: List of (time_ms, frequency) tuples.
    percentiles: iterable of floats, in the interval [0, 100].

  Returns:
    dict, mapping from percentile to value.
  Raises:
    ValueError: If one or more percentiles are outside [0, 100].
  """
  result = collections.OrderedDict()
  histogram = sorted(ycsb_histogram)
  for percentile in percentiles:
    if percentile < 0 or percentile > 100:
      raise ValueError('Invalid percentile: {0}'.format(percentile))
    if math.modf(percentile)[0] < 1e-7:
      percentile = int(percentile)
    label = 'p{0}'.format(percentile)
    latencies, freqs = list(zip(*histogram))
    time_ms = _WeightedQuantile(latencies, freqs, percentile * 0.01)
    result[label] = time_ms
  return result


def _CombineResults(
    result_list: Iterable[YcsbResult],
    measurement_type: str,
    combined_hdr: Mapping[str, list[_HdrHistogramTuple]],
):
  """Combine results from multiple YCSB clients.

  Reduces a list of YCSB results (the output of ParseResults)
  into a single result. Histogram bin counts, operation counts, and throughput
  are summed; RunTime is replaced by the maximum runtime of any result.

  Args:
    result_list: Iterable of ParseResults outputs.
    measurement_type: Measurement type used. If measurement type is histogram,
      histogram bins are summed across results. If measurement type is
      hdrhistogram, an aggregated hdrhistogram (combined_hdr) is expected.
    combined_hdr: Dict of already aggregated histogram.

  Returns:
    A dictionary, as returned by ParseResults.
  """

  def DropUnaggregated(result: YcsbResult) -> None:
    """Remove statistics which 'operators' specify should not be combined."""
    drop_keys = {k for k, v in AGGREGATE_OPERATORS.items() if v is None}
    for group in result.groups.values():
      for k in drop_keys:
        group.statistics.pop(k, None)

  def CombineHistograms(hist1, hist2):
    h1 = dict(hist1)
    h2 = dict(hist2)
    keys = sorted(frozenset(h1) | frozenset(h2))
    result = []
    for k in keys:
      result.append((k, h1.get(k, 0) + h2.get(k, 0)))
    return result

  combined_weights = {}

  def _CombineLatencyTimeSeries(
      combined_series: list[tuple[int, float]],
      individual_series: list[tuple[int, float]],
  ) -> list[tuple[int, float]]:
    """Combines two timeseries of average latencies.

    Args:
      combined_series: A list representing the timeseries with which the
        individual series is being merged.
      individual_series: A list representing the timeseries being merged with
        the combined series.

    Returns:
      A list representing the new combined series.

    Note that this assumes that each individual timeseries spent an equal
    amount of time executing requests for each timeslice. This should hold for
    runs without -target where each client has an equal number of threads, but
    may not hold otherwise.
    """
    combined_series = dict(combined_series)
    individual_series = dict(individual_series)
    timestamps = set(combined_series) | set(individual_series)

    result = []
    for timestamp in sorted(timestamps):
      if timestamp not in individual_series:
        continue
      if timestamp not in combined_weights:
        combined_weights[timestamp] = 1.0
      if timestamp not in combined_series:
        result.append((timestamp, individual_series[timestamp]))
        continue

      # This computes a new combined average latency by dividing the sum of
      # request latencies by the sum of request counts for the time period.
      # The sum of latencies for an individual series is assumed to be "1",
      # so the sum of latencies for the combined series is the total number of
      # series i.e. "combined_weight".
      # The request count for an individual series is 1 / average latency.
      # This means the request count for the combined series is
      # combined_weight * 1 / average latency.
      combined_weight = combined_weights[timestamp]
      average_latency = (combined_weight + 1.0) / (
          (combined_weight / combined_series[timestamp])
          + (1.0 / individual_series[timestamp])
      )
      result.append((timestamp, average_latency))
      combined_weights[timestamp] += 1.0
    return result

  def _CombineThroughputTimeSeries(
      series1: _ThroughputTimeSeries, series2: _ThroughputTimeSeries
  ) -> _ThroughputTimeSeries:
    """Returns a combined dict of [timestamp, total QPS] from the two series."""
    timestamps1 = set(series1)
    timestamps2 = set(series2)
    all_timestamps = timestamps1 | timestamps2
    diff_timestamps = timestamps1 ^ timestamps2
    if diff_timestamps:
      # This case is rare but does happen occassionally, so log a warning
      # instead of raising an exception.
      logging.warning(
          'Expected combined timestamps to be the same, got different '
          'timestamps: %s',
          diff_timestamps,
      )
    result = {}
    for timestamp in all_timestamps:
      result[timestamp] = series1.get(timestamp, 0) + series2.get(timestamp, 0)
    return result

  result_list = list(result_list)
  result = copy.deepcopy(result_list[0])
  DropUnaggregated(result)

  for indiv in result_list[1:]:
    for group_name, group in indiv.groups.items():
      if group_name not in result.groups:
        logging.warning(
            'Found result group "%s" in individual YCSB result, '
            'but not in accumulator.',
            group_name,
        )
        result.groups[group_name] = copy.deepcopy(group)
        continue

      # Combine reported statistics.
      # If no combining operator is defined, the statistic is skipped.
      # Otherwise, the aggregated value is either:
      # * The value in 'indiv', if the statistic is not present in 'result' or
      # * AGGREGATE_OPERATORS[statistic](result_value, indiv_value)
      for k, v in group.statistics.items():
        if k not in AGGREGATE_OPERATORS:
          logging.warning('No operator for "%s". Skipping aggregation.', k)
          continue
        elif AGGREGATE_OPERATORS[k] is None:  # Drop
          result.groups[group_name].statistics.pop(k, None)
          continue
        elif k not in result.groups[group_name].statistics:
          logging.warning(
              'Found statistic "%s.%s" in individual YCSB result, '
              'but not in accumulator.',
              group_name,
              k,
          )
          result.groups[group_name].statistics[k] = copy.deepcopy(v)
          continue

        op = AGGREGATE_OPERATORS[k]
        result.groups[group_name].statistics[k] = op(
            result.groups[group_name].statistics[k], v
        )

      if measurement_type == HISTOGRAM:
        result.groups[group_name].data = CombineHistograms(
            result.groups[group_name].data, group.data
        )
      elif measurement_type == TIMESERIES:
        result.groups[group_name].data = _CombineLatencyTimeSeries(
            result.groups[group_name].data, group.data
        )
    result.client = ' '.join((result.client, indiv.client))
    result.command_line = ';'.join((result.command_line, indiv.command_line))

    if _THROUGHPUT_TIME_SERIES.value:
      result.throughput_time_series = _CombineThroughputTimeSeries(
          result.throughput_time_series, indiv.throughput_time_series
      )

  if measurement_type == HDRHISTOGRAM:
    for group_name in combined_hdr:
      if group_name in result.groups:
        result.groups[group_name].data = combined_hdr[group_name]

  return result


def ParseWorkload(contents):
  """Parse a YCSB workload file.

  YCSB workloads are Java .properties format.
  http://en.wikipedia.org/wiki/.properties
  This function does not support all .properties syntax, in particular escaped
  newlines.

  Args:
    contents: str. Contents of the file.

  Returns:
    dict mapping from property key to property value for each property found in
    'contents'.
  """
  fp = io.StringIO(contents)
  result = {}
  for line in fp:
    if (
        line.strip()
        and not line.lstrip().startswith('#')
        and not line.lstrip().startswith('!')
    ):
      k, v = re.split(r'\s*[:=]\s*', line, maxsplit=1)
      result[k] = v.strip()
  return result


@vm_util.Retry(poll_interval=10, max_retries=10)
def PushWorkload(vm, workload_file, remote_path):
  """Pushes the workload file to the VM."""
  if os.path.basename(remote_path):
    vm.RemoteCommand('sudo rm -f ' + remote_path)
  vm.PushFile(workload_file, remote_path)


def _CreateSamples(
    ycsb_result: YcsbResult, include_histogram: bool = False, **kwargs
) -> list[sample.Sample]:
  """Create PKB samples from a YCSB result.

  Args:
    ycsb_result: dict. Result of ParseResults.
    include_histogram: bool. If True, include records for each histogram bin.
      Note that this will increase the output volume significantly.
    **kwargs: Base metadata for each sample.

  Yields:
    List of sample.Sample objects.
  """
  command_line = ycsb_result.command_line
  stage = 'load' if command_line.endswith('-load') else 'run'
  base_metadata = {
      'stage': stage,
      'ycsb_tar_url': _ycsb_tar_url,
      'ycsb_version': FLAGS.ycsb_version,
  }
  if _SHOULD_RECORD_COMMAND_LINE.value:
    base_metadata['command_line'] = command_line
  base_metadata.update(kwargs)

  throughput_time_series = ycsb_result.throughput_time_series
  if throughput_time_series:
    yield sample.Sample(
        'Throughput Time Series',
        0,
        '',
        {'throughput_time_series': sorted(throughput_time_series.items())},
    )

  for group_name, group in ycsb_result.groups.items():
    meta = base_metadata.copy()
    meta['operation'] = group_name
    for statistic, value in group.statistics.items():
      if value is None:
        continue

      unit = ''
      m = re.match(r'^(.*) *\((us|ms|ops/sec)\)$', statistic)
      if m:
        statistic = m.group(1)
        unit = m.group(2)
      yield sample.Sample(' '.join([group_name, statistic]), value, unit, meta)

    if group.data and group.data_type == HISTOGRAM:
      percentiles = _PercentilesFromHistogram(group.data)
      for label, value in percentiles.items():
        yield sample.Sample(
            ' '.join([group_name, label, 'latency']), value, 'ms', meta
        )
      if include_histogram:
        for time_ms, count in group.data:
          yield sample.Sample(
              '{0}_latency_histogram_{1}_ms'.format(group_name, time_ms),
              count,
              'count',
              meta,
          )

    if group.data and group.data_type == HDRHISTOGRAM:
      # Strip percentile from the three-element tuples.
      histogram = [value_count[-2:] for value_count in group.data]
      percentiles = _PercentilesFromHistogram(histogram)
      for label, value in percentiles.items():
        yield sample.Sample(
            ' '.join([group_name, label, 'latency']), value, 'ms', meta
        )
      if include_histogram:
        histogram = []
        for _, value, bucket_count in group.data:
          histogram.append(
              {'microsec_latency': int(value * 1000), 'count': bucket_count}
          )
        hist_meta = meta.copy()
        hist_meta.update({'histogram': json.dumps(histogram)})
        yield sample.Sample(
            '{0} latency histogram'.format(group_name), 0, '', hist_meta
        )

    if group.data and group.data_type == TIMESERIES:
      for sample_time, average_latency in group.data:
        timeseries_meta = meta.copy()
        timeseries_meta['sample_time'] = sample_time
        yield sample.Sample(
            ' '.join([group_name, 'AverageLatency (timeseries)']),
            average_latency,
            'ms',
            timeseries_meta,
        )
      yield sample.Sample(
          'Average Latency Time Series',
          0,
          '',
          {'latency_time_series': group.data},
      )


class YCSBExecutor:
  """Load data and run benchmarks using YCSB.

  See core/src/main/java/com/yahoo/ycsb/workloads/CoreWorkload.java for
  attribute descriptions.

  Attributes:
    database: str.
    loaded: boolean. If the database is already loaded.
    parameters: dict. May contain the following, plus database-specific fields
      (e.g., columnfamily for HBase).
      threads: int.
      target: int.
      fieldcount: int.
      fieldlengthdistribution: str.
      readallfields: boolean.
      writeallfields: boolean.
      readproportion: float.
      updateproportion: float.
      scanproportion: float.
      readmodifywriteproportion: float.
      requestdistribution: str.
      maxscanlength: int. Number of records to scan.
      scanlengthdistribution: str.
      insertorder: str.
      hotspotdatafraction: float.
      perclientparam: list.
      shardkeyspace: boolean. Default to False, indicates if clients should have
        their own keyspace.
  """

  FLAG_ATTRIBUTES = 'cp', 'jvm-args', 'target', 'threads'

  def __init__(self, database, parameter_files=None, **kwargs):
    self.database = database
    self.loaded = False
    self.measurement_type = FLAGS.ycsb_measurement_type
    self.hdr_dir = HDRHISTOGRAM_DIR

    self.parameter_files = parameter_files or []
    self.parameters = kwargs.copy()
    self.parameters['measurementtype'] = self.measurement_type
    self.parameters['measurement.interval'] = FLAGS.ycsb_measurement_interval

    # Self-defined parameters, pop them out of self.parameters, so they
    # are not passed to ycsb commands
    self.perclientparam = self.parameters.pop('perclientparam', None)
    self.shardkeyspace = self.parameters.pop('shardkeyspace', False)

  def _BuildCommand(self, command_name, parameter_files=None, **kwargs):
    """Builds the YCSB command line."""
    command = [YCSB_EXE, command_name, self.database]

    parameters = self.parameters.copy()
    parameters.update(kwargs)

    # Adding -s prints status which includes average throughput per sec.
    if _THROUGHPUT_TIME_SERIES.value and command_name == 'run':
      command.append('-s')
      parameters['status.interval'] = _STATUS_INTERVAL_SEC

    # These are passed as flags rather than properties, so they
    # are handled differently.
    for flag in self.FLAG_ATTRIBUTES:
      value = parameters.pop(flag, None)
      if value is not None:
        command.extend(('-{0}'.format(flag), str(value)))

    for param_file in list(self.parameter_files) + list(parameter_files or []):
      command.extend(('-P', param_file))

    for parameter, value in parameters.items():
      command.extend(('-p', '{0}={1}'.format(parameter, value)))

    return 'cd %s && %s' % (YCSB_DIR, ' '.join(command))

  @property
  def _default_preload_threads(self):
    """The default number of threads to use for pre-populating the DB."""
    if FLAGS['ycsb_preload_threads'].present:
      return FLAGS.ycsb_preload_threads
    return DEFAULT_PRELOAD_THREADS

  def _Load(self, vm, **kwargs):
    """Execute 'ycsb load' on 'vm'."""
    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    for pv in FLAGS.ycsb_load_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('load', **kwargs)
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ParseResults(str(stderr + stdout), self.measurement_type)

  def _LoadThreaded(self, vms, workload_file, **kwargs):
    """Runs "Load" in parallel for each VM in VMs.

    Args:
      vms: List of virtual machine instances. client nodes.
      workload_file: YCSB Workload file to use.
      **kwargs: Additional key-value parameters to pass to YCSB.

    Returns:
      List of sample.Sample objects.
    Raises:
      IOError: If number of results is not equal to the number of VMs.
    """
    results = []

    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    if FLAGS.ycsb_field_count:
      kwargs.setdefault('fieldcount', FLAGS.ycsb_field_count)
    if FLAGS.ycsb_field_length:
      kwargs.setdefault('fieldlength', FLAGS.ycsb_field_length)

    with open(workload_file) as fp:
      workload_meta = ParseWorkload(fp.read())
      workload_meta.update(kwargs)
      workload_meta.update(
          stage='load',
          clients=len(vms) * kwargs['threads'],
          threads_per_client_vm=kwargs['threads'],
          workload_name=os.path.basename(workload_file),
      )
      self.workload_meta = workload_meta
    record_count = int(workload_meta.get('recordcount', '1000'))
    n_per_client = int(record_count) // len(vms)
    loader_counts = [
        n_per_client + (1 if i < (record_count % len(vms)) else 0)
        for i in range(len(vms))
    ]

    remote_path = posixpath.join(
        linux_packages.INSTALL_DIR, os.path.basename(workload_file)
    )

    args = [((vm, workload_file, remote_path), {}) for vm in dict.fromkeys(vms)]
    background_tasks.RunThreaded(PushWorkload, args)

    kwargs['parameter_files'] = [remote_path]

    def _Load(loader_index):
      start = sum(loader_counts[:loader_index])
      kw = copy.deepcopy(kwargs)
      kw.update(insertstart=start, insertcount=loader_counts[loader_index])
      if self.perclientparam is not None:
        kw.update(self.perclientparam[loader_index])
      results.append(self._Load(vms[loader_index], **kw))
      logging.info('VM %d (%s) finished', loader_index, vms[loader_index])

    start = time.time()
    background_tasks.RunThreaded(_Load, list(range(len(vms))))
    events.record_event.send(
        type(self).__name__,
        event='load',
        start_timestamp=start,
        end_timestamp=time.time(),
        metadata=copy.deepcopy(kwargs),
    )

    if len(results) != len(vms):
      raise IOError(
          'Missing results: only {0}/{1} reported\n{2}'.format(
              len(results), len(vms), results
          )
      )

    samples = []
    if FLAGS.ycsb_include_individual_results and len(results) > 1:
      for i, result in enumerate(results):
        samples.extend(
            _CreateSamples(
                result,
                result_type='individual',
                result_index=i,
                include_histogram=FLAGS.ycsb_histogram,
                **workload_meta,
            )
        )

    # hdr histograms not collected upon load, only upon run
    combined = _CombineResults(results, self.measurement_type, {})
    samples.extend(
        _CreateSamples(
            combined,
            result_type='combined',
            include_histogram=FLAGS.ycsb_histogram,
            **workload_meta,
        )
    )

    return samples

  def _Run(self, vm, **kwargs):
    """Run a single workload from a client vm."""
    for pv in FLAGS.ycsb_run_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('run', **kwargs)
    # YCSB version greater than 0.7.0 output some of the
    # info we need to stderr. So we have to combine these 2
    # output to get expected results.
    hdr_files_dir = kwargs.get('hdrhistogram.output.path', None)
    if hdr_files_dir:
      vm.RemoteCommand('mkdir -p {0}'.format(hdr_files_dir))
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ParseResults(str(stderr + stdout), self.measurement_type)

  def _RunThreaded(self, vms, **kwargs):
    """Run a single workload using `vms`."""
    target = kwargs.pop('target', None)
    if target is not None:
      target_per_client = target // len(vms)
      targets = [
          target_per_client + (1 if i < (target % len(vms)) else 0)
          for i in range(len(vms))
      ]
    else:
      targets = [target for _ in vms]

    results = []

    if self.shardkeyspace:
      record_count = int(self.workload_meta.get('recordcount', '1000'))
      n_per_client = int(record_count) // len(vms)
      loader_counts = [
          n_per_client + (1 if i < (record_count % len(vms)) else 0)
          for i in range(len(vms))
      ]

    def _Run(loader_index):
      """Run YCSB on an individual VM."""
      vm = vms[loader_index]
      params = copy.deepcopy(kwargs)
      params['target'] = targets[loader_index]
      if self.perclientparam is not None:
        params.update(self.perclientparam[loader_index])
      if self.shardkeyspace:
        start = sum(loader_counts[:loader_index])
        end = start + loader_counts[loader_index]
        params.update(insertstart=start, recordcount=end)
      results.append(self._Run(vm, **params))
      logging.info('VM %d (%s) finished', loader_index, vm)

    background_tasks.RunThreaded(_Run, list(range(len(vms))))

    if len(results) != len(vms):
      raise IOError(
          'Missing results: only {0}/{1} reported\n{2}'.format(
              len(results), len(vms), results
          )
      )

    return results

  def _GetRunLoadTarget(self, current_load, is_sustained=False):
    """Get load target.

    If service cannot sustain current load, adjust load applied to the serivce
    based on ycsb_dynamic_load_sustain_throughput_ratio.
    If service is capable of handling current load and we are still above
    ycsb_dynamic_load_throughput_lower_bound, keep reducing the load
    (step size=2*(1-ycsb_dynamic_load_sustain_throughput_ratio)) and
    run test for reduced duration based on ycsb_dynamic_load_sustain_timelimit.

    Args:
      current_load: float. Current client load (QPS) applied to system under
        test.
      is_sustained: boolean. Indicate if system is capable of sustaining the
        load.

    Returns:
      Total client load (QPS) to apply to system under test.
    """
    lower_bound = FLAGS.ycsb_dynamic_load_throughput_lower_bound
    step = (1 - FLAGS.ycsb_dynamic_load_sustain_throughput_ratio) * 2

    if (
        (not bool(lower_bound) and is_sustained)
        or (lower_bound and current_load < lower_bound)
        or (current_load is None)
    ):
      return None
    elif is_sustained:
      return current_load * (1 - step)
    else:
      return current_load / FLAGS.ycsb_dynamic_load_sustain_throughput_ratio

  def RunStaircaseLoads(self, vms, workloads, **kwargs):
    """Run each workload in 'workloads' in succession.

    A staircase load is applied for each workload file, for each entry in
    ycsb_threads_per_client.

    Args:
      vms: List of VirtualMachine objects to generate load from.
      workloads: List of workload file names.
      **kwargs: Additional parameters to pass to each run.  See constructor for
        options.

    Returns:
      List of sample.Sample objects.
    """
    all_results = []
    parameters = {}
    for workload_index, workload_file in enumerate(workloads):
      if FLAGS.ycsb_operation_count:
        parameters = {'operationcount': FLAGS.ycsb_operation_count}
      if FLAGS.ycsb_record_count:
        parameters['recordcount'] = FLAGS.ycsb_record_count
      if FLAGS.ycsb_field_count:
        parameters['fieldcount'] = FLAGS.ycsb_field_count
      if FLAGS.ycsb_field_length:
        parameters['fieldlength'] = FLAGS.ycsb_field_length
      if FLAGS.ycsb_timelimit:
        parameters['maxexecutiontime'] = FLAGS.ycsb_timelimit
      hdr_files_dir = posixpath.join(self.hdr_dir, str(workload_index))
      if FLAGS.ycsb_measurement_type == HDRHISTOGRAM:
        parameters['hdrhistogram.fileoutput'] = True
        parameters['hdrhistogram.output.path'] = hdr_files_dir
      if FLAGS.ycsb_requestdistribution:
        parameters['requestdistribution'] = FLAGS.ycsb_requestdistribution
      if FLAGS.ycsb_readproportion is not None:
        parameters['readproportion'] = FLAGS.ycsb_readproportion
      if FLAGS.ycsb_updateproportion is not None:
        parameters['updateproportion'] = FLAGS.ycsb_updateproportion
      if FLAGS.ycsb_scanproportion is not None:
        parameters['scanproportion'] = FLAGS.ycsb_scanproportion
      parameters.update(kwargs)
      remote_path = posixpath.join(
          linux_packages.INSTALL_DIR, os.path.basename(workload_file)
      )

      with open(workload_file) as fp:
        workload_meta = ParseWorkload(fp.read())
        workload_meta.update(kwargs)
        workload_meta.update(
            workload_name=os.path.basename(workload_file),
            workload_index=workload_index,
            stage='run',
        )

      args = [
          ((vm, workload_file, remote_path), {}) for vm in dict.fromkeys(vms)
      ]
      background_tasks.RunThreaded(PushWorkload, args)

      parameters['parameter_files'] = [remote_path]

      # _GetThreadsQpsPerLoaderList() passes tuple of (client_count, target=0)
      # if no target is passed via flags.
      for client_count, target_qps_per_vm in _GetThreadsQpsPerLoaderList():

        def _DoRunStairCaseLoad(
            client_count, target_qps_per_vm, workload_meta, is_sustained=False
        ):
          parameters['threads'] = client_count
          if target_qps_per_vm:
            parameters['target'] = int(target_qps_per_vm * len(vms))
          if is_sustained:
            parameters['maxexecutiontime'] = (
                FLAGS.ycsb_dynamic_load_sustain_timelimit
            )
          start = time.time()
          results = self._RunThreaded(vms, **parameters)
          events.record_event.send(
              type(self).__name__,
              event='run',
              start_timestamp=start,
              end_timestamp=time.time(),
              metadata=copy.deepcopy(parameters),
          )
          client_meta = workload_meta.copy()
          client_meta.update(parameters)
          client_meta.update(
              clients=len(vms) * client_count,
              threads_per_client_vm=client_count,
          )
          # Values passed in via this flag do not get recorded in metadata.
          # The target passed in is applied to each client VM, so multiply by
          # len(vms).
          for pv in FLAGS.ycsb_run_parameters:
            param, value = pv.split('=', 1)
            if param == 'target':
              value = int(value) * len(vms)
            client_meta[param] = value

          if FLAGS.ycsb_include_individual_results and len(results) > 1:
            for i, result in enumerate(results):
              all_results.extend(
                  _CreateSamples(
                      result,
                      result_type='individual',
                      result_index=i,
                      include_histogram=FLAGS.ycsb_histogram,
                      **client_meta,
                  )
              )

          if self.measurement_type == HDRHISTOGRAM:
            combined_log = self.CombineHdrHistogramLogFiles(
                parameters['hdrhistogram.output.path'], vms
            )
            parsed_hdr = ParseHdrLogs(combined_log)
            combined = _CombineResults(
                results, self.measurement_type, parsed_hdr
            )
          else:
            combined = _CombineResults(results, self.measurement_type, {})
          run_samples = list(
              _CreateSamples(
                  combined,
                  result_type='combined',
                  include_histogram=FLAGS.ycsb_histogram,
                  **client_meta,
              )
          )

          overall_throughput = 0
          for s in run_samples:
            if s.metric == 'overall Throughput':
              overall_throughput += s.value
          return overall_throughput, run_samples

        target_throughput, run_samples = _DoRunStairCaseLoad(
            client_count, target_qps_per_vm, workload_meta
        )

        # Uses 5 * unthrottled throughput as starting point.
        target_throughput *= 5
        all_results.extend(run_samples)
        is_sustained = False
        while FLAGS.ycsb_dynamic_load:
          actual_throughput, run_samples = _DoRunStairCaseLoad(
              client_count,
              target_throughput // len(vms),
              workload_meta,
              is_sustained,
          )
          is_sustained = FLAGS.ycsb_dynamic_load_sustain_throughput_ratio < (
              actual_throughput / target_throughput
          )
          for s in run_samples:
            s.metadata['sustained'] = is_sustained
          all_results.extend(run_samples)
          target_throughput = self._GetRunLoadTarget(
              actual_throughput, is_sustained
          )
          if target_throughput is None:
            break

    return all_results

  def CombineHdrHistogramLogFiles(
      self, hdr_files_dir: str, vms: Iterable[virtual_machine.VirtualMachine]
  ) -> dict[str, str]:
    """Combine multiple hdr histograms by group type.

    Combine multiple hdr histograms in hdr log files format into 1 human
    readable hdr histogram log file.
    This is done by
    1) copying hdrhistogram log files to a single file on a worker vm;
    2) aggregating file containing multiple %-tile histogram into
       a single %-tile histogram using HistogramLogProcessor from the
       hdrhistogram package that is installed on the vms. Refer to https://
       github.com/HdrHistogram/HdrHistogram/blob/master/HistogramLogProcessor

    Args:
      hdr_files_dir: directory on the remote vms where hdr files are stored.
      vms: remote vms

    Returns:
      dict of hdrhistograms keyed by group type
    """
    vms = list(vms)
    hdrhistograms = {}
    for grouptype in HDRHISTOGRAM_GROUPS:

      def _GetHdrHistogramLog(vm, group=grouptype):
        filename = f'{hdr_files_dir}{group}.hdr'
        return vm.RemoteCommand(f'touch {filename} && tail -1 {filename}')[0]

      results = background_tasks.RunThreaded(_GetHdrHistogramLog, vms)

      # It's possible that there is no result for certain group, e.g., read
      # only, update only.
      if not all(results):
        continue

      worker_vm = vms[0]
      for hdr in results[1:]:
        worker_vm.RemoteCommand(
            'sudo chmod 755 {1}{2}.hdr && echo "{0}" >> {1}{2}.hdr'.format(
                hdr[:-1], hdr_files_dir, grouptype
            )
        )
      hdrhistogram, _ = worker_vm.RemoteCommand(
          'cd {0} && ./HistogramLogProcessor -i {1}{2}.hdr'
          ' -outputValueUnitRatio 1'.format(
              self.hdr_dir, hdr_files_dir, grouptype
          )
      )
      hdrhistograms[grouptype.lower()] = hdrhistogram
    return hdrhistograms

  def Load(self, vms, workloads=None, load_kwargs=None):
    """Load data using YCSB."""
    if FLAGS.ycsb_skip_load_stage:
      return []

    workloads = workloads or GetWorkloadFileList()
    load_samples = []
    assert workloads, 'no workloads'

    def _HasInsertFailures(result_samples):
      for s in result_samples:
        if s.metric == _INCOMPLETE_LOADING_METRIC.value and s.value > 0:
          return True
      return False

    if FLAGS.ycsb_reload_database or not self.loaded:
      load_samples += list(
          self._LoadThreaded(vms, workloads[0], **(load_kwargs or {}))
      )
      if _SHOULD_FAIL_ON_INCOMPLETE_LOADING.value and _HasInsertFailures(
          load_samples
      ):
        raise errors.Benchmarks.RunError(
            'There are insert failures, so the table loading is incomplete'
        )

      self.loaded = True
    if FLAGS.ycsb_sleep_after_load_in_sec > 0:
      logging.info(
          'Sleeping %s seconds after load stage.',
          FLAGS.ycsb_sleep_after_load_in_sec,
      )
      time.sleep(FLAGS.ycsb_sleep_after_load_in_sec)
    if FLAGS.ycsb_load_samples:
      return load_samples
    else:
      return []

  def Run(self, vms, workloads=None, run_kwargs=None) -> list[sample.Sample]:
    """Runs each workload/client count combination."""
    if FLAGS.ycsb_skip_run_stage:
      return []
    workloads = workloads or GetWorkloadFileList()
    assert workloads, 'no workloads'
    if not run_kwargs:
      run_kwargs = {}
    if _BURST_LOAD_MULTIPLIER.value:
      samples = self._RunBurstMode(vms, workloads, run_kwargs)
    elif _INCREMENTAL_TARGET_QPS.value:
      samples = self._RunIncrementalMode(vms, workloads, run_kwargs)
    else:
      samples = list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))
    if (
        FLAGS.ycsb_sleep_after_load_in_sec > 0
        and not FLAGS.ycsb_skip_load_stage
    ):
      for s in samples:
        s.metadata['sleep_after_load_in_sec'] = (
            FLAGS.ycsb_sleep_after_load_in_sec
        )
    return samples

  def _SetRunParameters(self, params: Mapping[str, Any]) -> None:
    """Sets the --ycsb_run_parameters flag."""
    # Ideally YCSB should be refactored to include a function that just takes
    # commands for a run, but that will be a large refactor.
    FLAGS['ycsb_run_parameters'].unparse()
    FLAGS['ycsb_run_parameters'].parse([f'{k}={v}' for k, v in params.items()])

  def _RunBurstMode(self, vms, workloads, run_kwargs=None):
    """Runs YCSB in burst mode, where the second run has increased QPS."""
    run_params = _GetRunParameters()
    initial_qps = int(run_params.get('target', 0))

    samples = list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))

    if _BURST_LOAD_MULTIPLIER.value == -1:
      run_params.pop('target')  # Set to unlimited
    else:
      run_params['target'] = initial_qps * _BURST_LOAD_MULTIPLIER.value
    self._SetRunParameters(run_params)
    samples += list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))
    return samples

  def _GetIncrementalQpsTargets(self, target_qps: int) -> list[int]:
    """Returns incremental QPS targets."""
    qps = _INCREMENTAL_STARTING_QPS
    result = []
    while qps < target_qps:
      result.append(qps)
      qps *= 1.5
    return result

  def _SetClientThreadCount(self, count: int) -> None:
    FLAGS['ycsb_threads_per_client'].unparse()
    FLAGS['ycsb_threads_per_client'].parse([str(count)])

  def _RunIncrementalMode(
      self,
      vms: Sequence[virtual_machine.VirtualMachine],
      workloads: Sequence[str],
      run_kwargs: Mapping[str, str] = None,
  ) -> list[sample.Sample]:
    """Runs YCSB by gradually incrementing target QPS.

    Note that this requires clients to be overprovisioned, as the target QPS
    for YCSB is generally a "throttling" mechanism where the threads try to send
    as much QPS as possible and then get throttled. If clients are
    underprovisioned then it's possible for the run to not hit the desired
    target, which may be undesired behavior.

    See
    https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic
    for an example of why this is needed.

    Args:
      vms: The client VMs to generate the load.
      workloads: List of workloads to run.
      run_kwargs: Extra run arguments.

    Returns:
      A list of samples of benchmark results.
    """
    run_params = _GetRunParameters()
    ending_qps = _INCREMENTAL_TARGET_QPS.value
    ending_length = FLAGS.ycsb_timelimit
    ending_threadcount = int(FLAGS.ycsb_threads_per_client[0])
    incremental_targets = self._GetIncrementalQpsTargets(ending_qps)
    logging.info('Incremental targets: %s', incremental_targets)

    # Warm-up phase is shorter and doesn't need results parsing
    FLAGS['ycsb_timelimit'].parse(_INCREMENTAL_TIMELIMIT_SEC)
    for target in incremental_targets:
      target /= FLAGS.ycsb_client_vms
      run_params['target'] = int(target)
      self._SetClientThreadCount(min(ending_threadcount, int(target)))
      self._SetRunParameters(run_params)
      self.RunStaircaseLoads(vms, workloads, **run_kwargs)

    # Reset back to the original workload args
    FLAGS['ycsb_timelimit'].parse(ending_length)
    ending_qps /= FLAGS.ycsb_client_vms
    run_params['target'] = int(ending_qps)
    self._SetClientThreadCount(ending_threadcount)
    self._SetRunParameters(run_params)
    return list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))

  def LoadAndRun(self, vms, workloads=None, load_kwargs=None, run_kwargs=None):
    """Load data using YCSB, then run each workload/client count combination.

    Loads data using the workload defined by 'workloads', then
    executes YCSB for each workload file in 'workloads', for each
    client count defined in FLAGS.ycsb_threads_per_client.

    Generally database benchmarks using YCSB should only need to call this
    method.

    Args:
      vms: List of virtual machines. VMs to use to generate load.
      workloads: List of strings. Workload files to use. If unspecified,
        GetWorkloadFileList() is used.
      load_kwargs: dict. Additional arguments to pass to the load stage.
      run_kwargs: dict. Additional arguments to pass to the run stage.

    Returns:
      List of sample.Sample objects.
    """
    load_samples = []
    if not FLAGS.ycsb_skip_load_stage:
      load_samples = self.Load(
          vms, workloads=workloads, load_kwargs=load_kwargs
      )
    run_samples = []
    if not FLAGS.ycsb_skip_run_stage:
      run_samples = self.Run(vms, workloads=workloads, run_kwargs=run_kwargs)
    return load_samples + run_samples
