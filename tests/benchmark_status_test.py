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
"""Tests for perfkitbenchmarker.benchmark_status."""

import os
import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import errors
from perfkitbenchmarker import pkb
from tests import pkb_common_test_case


class MockSpec(object):
  """A mock BenchmarkSpec class.

  We need to use this rather than a mock.MagicMock object because
  the "name" attribute of MagicMocks is difficult to set.
  """

  def __init__(self, name, uid, status, failed_substatus=None):
    self.name = name
    self.uid = uid
    self.status = status
    self.failed_substatus = failed_substatus


_BENCHMARK_SPECS = [
    MockSpec('iperf', 'iperf0', benchmark_status.SUCCEEDED),
    MockSpec('iperf', 'iperf1', benchmark_status.FAILED),
    MockSpec(
        'iperf',
        'iperf2',
        benchmark_status.FAILED,
        benchmark_status.FailedSubstatus.QUOTA,
    ),
    MockSpec('cluster_boot', 'cluster_boot0', benchmark_status.SKIPPED),
]
_STATUS_TABLE = os.linesep.join((
    '--------------------------------------------------------',
    'Name          UID            Status     Failed Substatus',
    '--------------------------------------------------------',
    'iperf         iperf0         SUCCEEDED                  ',
    'iperf         iperf1         FAILED                     ',
    'iperf         iperf2         FAILED     QUOTA_EXCEEDED  ',
    'cluster_boot  cluster_boot0  SKIPPED                    ',
    '--------------------------------------------------------',
))
_STATUS_SUMMARY = os.linesep.join((
    'Benchmark run statuses:',
    '--------------------------------------------------------',
    'Name          UID            Status     Failed Substatus',
    '--------------------------------------------------------',
    'iperf         iperf0         SUCCEEDED                  ',
    'iperf         iperf1         FAILED                     ',
    'iperf         iperf2         FAILED     QUOTA_EXCEEDED  ',
    'cluster_boot  cluster_boot0  SKIPPED                    ',
    '--------------------------------------------------------',
    'Success rate: 25.00% (1/4)',
))


class CreateSummaryTableTestCase(unittest.TestCase):

  def testCreateSummaryTable(self):
    result = benchmark_status._CreateSummaryTable(_BENCHMARK_SPECS)
    self.assertEqual(result, _STATUS_TABLE)


class CreateSummaryTestCase(unittest.TestCase):

  def testCreateSummary(self):
    result = benchmark_status.CreateSummary(_BENCHMARK_SPECS)
    self.assertEqual(result, _STATUS_SUMMARY)


class FailedSubstatusTestCase(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'Quota',
          'exception_class': errors.Benchmarks.QuotaFailure,
          'expected_substatus': benchmark_status.FailedSubstatus.QUOTA,
      },
      {
          'testcase_name': 'Capacity',
          'exception_class': errors.Benchmarks.InsufficientCapacityCloudFailure,
          'expected_substatus': (
              benchmark_status.FailedSubstatus.INSUFFICIENT_CAPACITY
          ),
      },
      {
          'testcase_name': 'KnownIntermittent',
          'exception_class': errors.Benchmarks.KnownIntermittentError,
          'expected_substatus': (
              benchmark_status.FailedSubstatus.KNOWN_INTERMITTENT
          ),
      },
      {
          'testcase_name': 'RestoreError',
          'exception_class': errors.Resource.RestoreError,
          'expected_substatus': benchmark_status.FailedSubstatus.RESTORE_FAILED,
      },
      {
          'testcase_name': 'FreezeError',
          'exception_class': errors.Resource.FreezeError,
          'expected_substatus': benchmark_status.FailedSubstatus.FREEZE_FAILED,
      },
      {
          'testcase_name': 'Uncategorized',
          'exception_class': Exception,
          'expected_substatus': benchmark_status.FailedSubstatus.UNCATEGORIZED,
      },
  )
  def testRunBenchmarkExceptionHasCorrectFailureStatus(
      self, exception_class, expected_substatus
  ):
    self.enter_context(
        mock.patch.object(
            pkb, 'DoProvisionPhase', side_effect=[exception_class()]
        )
    )
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    # Skip pickling the spec.
    self.enter_context(mock.patch.object(test_spec, 'Pickle'))

    with self.assertRaises(exception_class):
      pkb.RunBenchmark(spec=test_spec, collector=mock.Mock())

    self.assertEqual(test_spec.status, benchmark_status.FAILED)
    self.assertEqual(test_spec.failed_substatus, expected_substatus)


if __name__ == '__main__':
  unittest.main()
