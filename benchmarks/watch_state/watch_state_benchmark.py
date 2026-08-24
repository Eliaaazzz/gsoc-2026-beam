"""Measure how Watch deduplication state scales as the polled set grows.

The poll function re-lists a set that gains ``--per_round`` items every round
for ``--rounds`` rounds, so the final round hands Watch ``per_round * rounds``
items to check. Each item keeps the event time of the round that created it,
the way a file keeps its modification time. Two modes are compared: the default
mode, which remembers a fingerprint of every item forever, and
``timestamp_cursor`` mode, which drops fingerprints once the cursor moves past
them.

Per-round wall time is written to a log; the first and last rounds show how the
cost changes as the remembered set grows.

    python watch_state_benchmark.py --mode default --per_round 2000 --rounds 100
    python watch_state_benchmark.py --mode cursor --runner prism \
        --prism_location /path/to/prism

Run both modes and compare the last-round timings.
"""
import argparse
import json
import os
import tempfile
import time

import apache_beam as beam
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import Watch
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

NAMESPACE = 'watch_state_benchmark'


def pipeline_options(args):
  if args.runner == 'prism':
    opts = ['--runner=PrismRunner', '--environment_type=LOOPBACK']
    if args.prism_location:
      opts.append('--prism_location=%s' % args.prism_location)
    return PipelineOptions(opts)
  return PipelineOptions(['--runner=DirectRunner'])


class GrowingListing(object):
  """Re-lists a set that gains ``per_round`` items each poll round."""
  def __init__(self, per_round, rounds, log_path):
    self.per_round = per_round
    self.rounds = rounds
    self.log_path = log_path

  def __call__(self, unused_element):
    created = []
    if os.path.exists(self.log_path):
      with open(self.log_path) as f:
        created = [float(line) for line in f if line.strip()]
    now = time.time()
    with open(self.log_path, 'a') as f:
      f.write('%f\n' % now)
    created.append(now)
    this_round = len(created)
    outputs = [
        TimestampedValue(
            'item-%08d' % i, Timestamp(created[i // self.per_round]))
        for i in range(self.per_round * this_round)
    ]
    if this_round >= self.rounds:
      return PollResult.complete(outputs)
    return PollResult.incomplete(outputs)


class Count(beam.DoFn):
  def __init__(self):
    self.emitted = Metrics.counter(NAMESPACE, 'emitted')

  def process(self, element):
    self.emitted.inc()
    yield element


def run(args):
  fd, log_path = tempfile.mkstemp(prefix='watch_state_')
  os.close(fd)
  os.remove(log_path)
  try:
    kwargs = {}
    if args.mode == 'cursor':
      kwargs = {'timestamp_cursor': True, 'allowed_lateness': 0}
    pipeline = beam.Pipeline(options=pipeline_options(args))
    _ = (
        pipeline
        | beam.Create(['dir'])
        | Watch(
            GrowingListing(args.per_round, args.rounds, log_path),
            poll_interval=0.01,
            **kwargs)
        | beam.ParDo(Count()))
    start = time.time()
    result = pipeline.run()
    result.wait_until_finish()
    wall = time.time() - start
    emitted = None
    for counter in result.metrics().query(
        MetricsFilter().with_namespace(NAMESPACE))['counters']:
      if counter.key.metric.name == 'emitted':
        emitted = counter.result
    with open(log_path) as f:
      times = [float(line) for line in f if line.strip()]
  finally:
    if os.path.exists(log_path):
      os.remove(log_path)

  deltas = [b - a for a, b in zip(times, times[1:])]
  first = deltas[:10] or [0.0]
  last = deltas[-10:] or [0.0]
  return {
      'runner': args.runner,
      'mode': args.mode,
      'per_round': args.per_round,
      'rounds': len(times),
      'items_final': args.per_round * len(times),
      'emitted': emitted,
      'wall_secs': round(wall, 1),
      'round_ms_first10': round(1000 * sum(first) / len(first), 1),
      'round_ms_last10': round(1000 * sum(last) / len(last), 1),
  }


def parse_args(argv=None):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--mode', choices=('default', 'cursor'), default='default')
  parser.add_argument('--runner', choices=('direct', 'prism'), default='direct')
  parser.add_argument('--prism_location', default=None)
  parser.add_argument('--per_round', type=int, default=2000)
  parser.add_argument('--rounds', type=int, default=100)
  return parser.parse_args(argv)


if __name__ == '__main__':
  print(json.dumps(run(parse_args())))
