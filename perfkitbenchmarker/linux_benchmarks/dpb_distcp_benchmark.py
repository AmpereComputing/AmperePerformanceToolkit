# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Perform distributed copy of data on data processing backends.

Apache Hadoop MapReduce distcp is an open-source tool used to copy large
amounts of data. DistCp is very efficient because it uses MapReduce to copy the
files or datasets and this means the copy operation is distributed across
multiple nodes in a cluster.
Benchmark to compare the performance of of the same distcp workload on clusters
of various cloud providers.
"""

import copy
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import dpb_testdfsio_benchmark

BENCHMARK_NAME = 'dpb_distcp_benchmark'

BENCHMARK_CONFIG = """
dpb_distcp_benchmark:
  description: Run distcp on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
        AWS:
          machine_type: m4.xlarge
      disk_spec:
        GCP:
          disk_size: 1500
          disk_type: pd-standard
        AWS:
          disk_size: 1500
          disk_type: gp2
    worker_count: 8
"""

flags.DEFINE_enum(
    'distcp_source_fs',
    dpb_constants.GCS_FS,
    [dpb_constants.GCS_FS, dpb_constants.S3_FS, dpb_constants.HDFS_FS],
    'File System to use as the source of the distcp operation',
)

flags.DEFINE_enum(
    'distcp_dest_fs',
    dpb_constants.GCS_FS,
    [dpb_constants.GCS_FS, dpb_constants.S3_FS, dpb_constants.HDFS_FS],
    'File System to use as destination of the distcp operation',
)

flags.DEFINE_integer(
    'distcp_file_size_mbs',
    10,
    'File size to use for each of the distcp source files',
)

flags.DEFINE_integer('distcp_num_files', 10, 'Number of distcp source files')

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [
    dpb_constants.DATAPROC,
    dpb_constants.EMR,
    dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
]


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Configu to validate.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend for distcp. Not in:{}'.format(
            str(SUPPORTED_DPB_BACKENDS)
        )
    )


def Prepare(benchmark_spec):
  del benchmark_spec  # unused


def Run(benchmark_spec):
  """Runs distributed_copy benchmark and reports the results.

  Args:
    benchmark_spec: Spec needed to run the benchmark

  Returns:
    A list of samples
  """
  run_uri = benchmark_spec.uuid.split('-')[0]
  service = benchmark_spec.dpb_service

  if FLAGS.distcp_source_fs == dpb_constants.HDFS_FS:
    source_dir = '/pkb-{}/distcp_source/'.format(run_uri)
  elif service.base_dir.startswith(FLAGS.distcp_source_fs):
    source_dir = service.base_dir + '/distcp_source/'
  else:
    raise errors.Config.InvalidValue(
        'Service type {} cannot use distcp_source_fs: {}'.format(
            service.type, FLAGS.distcp_source_fs
        )
    )

  # Subdirectory TestDFSO writes data to
  source_data_dir = source_dir + 'io_data'

  if FLAGS.distcp_dest_fs == dpb_constants.HDFS_FS:
    destination_dir = '/pkb-{}/distcp_destination/'.format(run_uri)
  elif service.base_dir.startswith(FLAGS.distcp_dest_fs):
    destination_dir = service.base_dir + '/distcp_destination/'
  else:
    raise errors.Config.InvalidValue(
        'Service type {} cannot use distcp_dest_fs: {}'.format(
            service.type, FLAGS.distcp_destination_fs
        )
    )

  # Generate data to copy
  # TODO(saksena): Add a generic GenerateData method to dpb_service.
  dpb_testdfsio_benchmark.RunTestDfsio(
      service,
      dpb_testdfsio_benchmark.WRITE,
      source_dir,
      FLAGS.distcp_num_files,
      FLAGS.distcp_file_size_mbs,
  )

  result = benchmark_spec.dpb_service.DistributedCopy(
      source_data_dir, destination_dir
  )

  results = []
  metadata = copy.copy(benchmark_spec.dpb_service.GetResourceMetadata())
  metadata.update({'source_fs': FLAGS.distcp_source_fs})
  metadata.update({'destination_fs': FLAGS.distcp_dest_fs})
  metadata.update({'distcp_num_files': FLAGS.distcp_num_files})
  metadata.update({'distcp_file_size_mbs': FLAGS.distcp_file_size_mbs})
  if FLAGS.zone:
    zone = FLAGS.zone[0]
    region = zone.rsplit('-', 1)[0]
    metadata.update({'regional': True})
    metadata.update({'region': region})
  elif FLAGS.cloud == 'AWS':
    metadata.update({'regional': True})
    metadata.update({'region': 'aws_default'})
  service.metadata.update(metadata)

  results.append(
      sample.Sample('run_time', result.run_time, 'seconds', metadata)
  )
  return results


def Cleanup(benchmark_spec):
  """Cleans up the distcp benchmark."""
  del benchmark_spec  # unused
