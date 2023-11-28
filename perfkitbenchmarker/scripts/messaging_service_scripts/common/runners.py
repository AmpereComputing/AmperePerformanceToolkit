"""Runners for the different benchmark scenarios."""

# pylint: disable=broad-except
import abc
import json
import time
from typing import Any, Dict, List

from absl import flags
import numpy as np
from perfkitbenchmarker.scripts.messaging_service_scripts.common import client

GET_TIME_IN_MILLISECONDS = lambda: time.time() * 1000
UNIT_OF_TIME = 'milliseconds'

_WARMUP_MESSAGES = flags.DEFINE_integer(
    'warmup_messages',
    0,
    lower_bound=0,
    help=(
        'Number of messages that will be considered warm-up and will not be '
        'included into the steady_state resulting metrics. Must be greater '
        'or equal to 0 and less than number_of_messages. If set to 0, no '
        'steady_state metrics will be reported (this is the default).'
    ),
)


class BaseRunner(metaclass=abc.ABCMeta):
  """Base Runner class.

  This class is the ancestor of all the runner classes. Concrete subclasses must
  implement the run_phase method which actually contains the code for a given
  scenario.

  The actual cloud connection logic is delegated to the
  BaseMessagingServiceClient instance at self.client.
  """

  STARTUP_RUN = False

  @classmethod
  def run_class_startup(cls):
    """Requests to run the class startup code if it hasn't run yet.

    Do not override this. Instead override the on_startup method.
    """
    if not cls.STARTUP_RUN:
      cls.on_startup()
      cls.STARTUP_RUN = True

  @classmethod
  def on_startup(cls):
    """Executes code before creating the 1st instance with the factories utils.

    Optional override.
    """
    pass

  def __init__(self, client_: client.BaseMessagingServiceClient):
    self.client = client_

  def _get_summary_statistics(
      self,
      scenario: str,
      results: List[float],
      number_of_messages: int,
      failure_counter: int,
  ) -> Dict[str, Any]:
    """Getting statistics based on results from the benchmark."""
    metrics_data = {}
    common_metadata = {}

    latency_mean = np.mean(results)
    latency_percentage_received = 100 * (len(results) / number_of_messages)

    metrics_data[scenario + '_failure_counter'] = {
        'value': failure_counter,
        'unit': '',
        'metadata': common_metadata,
    }
    metrics_data[scenario + '_mean'] = {
        'value': latency_mean,
        'unit': UNIT_OF_TIME,
        'metadata': {'samples': results},
    }
    metrics_data[scenario + '_cold'] = {
        'value': results[0],
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata,
    }
    metrics_data[scenario + '_p50'] = {
        'value': np.percentile(results, 50),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata,
    }
    metrics_data[scenario + '_p99'] = {
        'value': np.percentile(results, 99),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata,
    }
    metrics_data[scenario + '_p99_9'] = {
        'value': np.percentile(results, 99.9),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata,
    }
    if _WARMUP_MESSAGES.value:
      metrics_data[scenario + '_steady_state_p50'] = {
          'value': np.percentile(results[_WARMUP_MESSAGES.value :], 50),
          'unit': UNIT_OF_TIME,
          'metadata': common_metadata,
      }
      metrics_data[scenario + '_steady_state_p99'] = {
          'value': np.percentile(results[_WARMUP_MESSAGES.value :], 99),
          'unit': UNIT_OF_TIME,
          'metadata': common_metadata,
      }
      metrics_data[scenario + '_steady_state_p99_9'] = {
          'value': np.percentile(results[_WARMUP_MESSAGES.value :], 99.9),
          'unit': UNIT_OF_TIME,
          'metadata': common_metadata,
      }
    metrics_data[scenario + '_percentage_received'] = {
        'value': latency_percentage_received,
        'unit': '%',
        'metadata': common_metadata,
    }
    return metrics_data

  @abc.abstractmethod
  def run_phase(
      self, number_of_messages: int, message_size: int
  ) -> Dict[str, Any]:
    """Runs a given benchmark based on the benchmark_messaging_service Flag.

    Args:
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the benchmark. It
        specifies the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    raise NotImplementedError

  def close(self):
    """Closes the client."""
    self.client.close()


class PullLatencyRunner(BaseRunner):
  """Runner for single pull latency measurement."""

  def run_phase(
      self, number_of_messages: int, message_size: int
  ) -> Dict[str, Any]:
    """Pull messages from messaging service and measure single pull latency.

    This function attempts to pull messages from a messaging service in a
    single stream to compute and report average stats for a single message
    pull. It measures the latency between a call to pull the message, and the
    message being successfully received on the client VM. We wait for a message
    to be pulled before attempting to pull the next one (blocking call). We also
    measure the latency between a call to pull the message, and a completed call
    to acknowledge that the message was received. In case of any failure when
    pulling a message we ignore it and proceed to attempt to pull the
    next message (success rate is one of the statistics generated by
    '_get_summary_statistics'). If some messages failed to publish, we expect
    their pull operation to fail as well. Pull failure should be very rare in
    normal conditions.

    Args:
      number_of_messages: Number of messages to pull.
      message_size: Message size. Ignored.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    pull_latencies = []
    acknowledge_latencies = []
    failure_counter = 0

    # attempt to pull 'number_of_messages' messages
    for _ in range(number_of_messages):
      start_time = GET_TIME_IN_MILLISECONDS()
      try:
        message = self.client.pull_message()
        if message is None:
          raise Exception('Could not pull the message.')
        pull_end_time = GET_TIME_IN_MILLISECONDS()
        self.client.acknowledge_received_message(message)
        acknowledge_end_time = GET_TIME_IN_MILLISECONDS()
        pull_latencies.append(pull_end_time - start_time)
        acknowledge_latencies.append(acknowledge_end_time - start_time)
      except Exception:
        failure_counter += 1

    # getting summary statistics
    pull_metrics = self._get_summary_statistics(
        'pull_latency', pull_latencies, number_of_messages, failure_counter
    )
    acknowledge_metrics = self._get_summary_statistics(
        'pull_and_acknowledge_latency',
        acknowledge_latencies,
        number_of_messages,
        failure_counter,
    )

    # merging metrics dictionaries
    metrics = {**pull_metrics, **acknowledge_metrics}

    print(json.dumps(metrics))
    return metrics


class PublishLatencyRunner(BaseRunner):
  """Runner for single publish latency measurement."""

  def run_phase(
      self, number_of_messages: int, message_size: int
  ) -> Dict[str, Any]:
    """Publish messages on messaging service and measure single publish latency.

    This function attempts to publish messages to a messaging service in a
    single stream to compute and report average stats for a single message
    publish. It measures the latency between a call to publish the message, and
    the message being successfully published. We wait for the publish message
    call to be completed (with a blocking call) before attempting to publish
    the next message. When the publish message call is completed the message
    was successfully published. In case of any failure when publishing a message
    we ignore it and proceed to attempt to publish the next message (success
    rate is one of the statistics generated by '_get_summary_statistics').
    Publish failure should be very rare in normal conditions.

    Args:
      number_of_messages: Number of messages to publish.
      message_size: Size of the messages that are being published. It specifies
        the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    publish_latencies = []
    failure_counter = 0

    # publishing 'number_of_messages' messages
    for i in range(number_of_messages):
      message_payload = self.client.generate_message(i, message_size)
      start_time = GET_TIME_IN_MILLISECONDS()
      # Publishing a message and waiting for completion
      try:
        self.client.publish_message(message_payload)
        end_time = GET_TIME_IN_MILLISECONDS()
        publish_latencies.append(end_time - start_time)
      except Exception:
        failure_counter += 1

    # getting metrics for publish, pull, and acknowledge latencies
    publish_metrics = self._get_summary_statistics(
        'publish_latency',
        publish_latencies,
        number_of_messages,
        failure_counter,
    )
    print(json.dumps(publish_metrics))
    return publish_metrics
