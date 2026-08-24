"""Measure the UnboundedSource wrapper's own overhead.

A minimal in-memory source hands the wrapper a fixed backlog of records, so the
numbers isolate the wrapper (restriction tracking, self-checkpointing, bundle
finalization) rather than any real connector's IO.

Two shapes:

  backlog   All records available at once. Reports throughput and how the
            per-bundle cap sets the self-checkpoint count.
  live      The source releases records at a fixed rate. Reports steady-state
            throughput, per-record latency, and checkpoint cadence.

    python unbounded_source_benchmark.py --shape backlog --records 1000000 \
        --cap 10000
    python unbounded_source_benchmark.py --shape live --records 100000 \
        --rate 5000 --runner prism --prism_location /path/to/prism
"""
import argparse
import json
import os
import tempfile
import time

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

NAMESPACE = 'unbounded_source_benchmark'


def pipeline_options(args):
  if args.runner == 'prism':
    opts = ['--runner=PrismRunner', '--environment_type=LOOPBACK']
    if args.prism_location:
      opts.append('--prism_location=%s' % args.prism_location)
    return PipelineOptions(opts)
  if args.runner == 'flink':
    opts = [
        '--runner=FlinkRunner',
        '--flink_version=%s' % args.flink_version,
        '--environment_type=LOOPBACK',
        '--parallelism=1',
        '--streaming',
    ]
    if args.flink_master:
      opts.append('--flink_master=%s' % args.flink_master)
    return PipelineOptions(opts)
  if args.runner == 'spark':
    opts = ['--runner=SparkRunner', '--environment_type=LOOPBACK', '--streaming']
    if args.spark_job_server_jar:
      opts.append('--spark_job_server_jar=%s' % args.spark_job_server_jar)
    return PipelineOptions(opts)
  return PipelineOptions(['--runner=DirectRunner'])


class _Mark(CheckpointMark):
  def __init__(self, last_index, anchor):
    self.last_index = last_index
    self.anchor = anchor

  def finalize_checkpoint(self):
    pass


class _Reader(UnboundedReader):
  """Record i becomes available at anchor + max(0, (i - backlog) / rate)."""
  def __init__(self, source, start_index, anchor):
    self._s = source
    self._next = start_index
    self._anchor = anchor
    self._current = None
    self._checkpoints = Metrics.counter(NAMESPACE, 'checkpoints')

  def _available_at(self, i):
    late = max(0, i - self._s.backlog)
    return self._anchor + late / self._s.rate

  def start(self):
    return self.advance()

  def advance(self):
    if self._next >= self._s.total:
      return False
    if time.time() < self._available_at(self._next):
      return False
    self._current, self._next = self._next, self._next + 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(self._available_at(self._current))

  def get_watermark(self):
    if self._next >= self._s.total:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return Timestamp(self._available_at(self._current))

  def get_checkpoint_mark(self):
    self._checkpoints.inc()
    return _Mark(self._next - 1, self._anchor)


class _Source(UnboundedSource):
  def __init__(self, total, backlog, rate):
    self.total = total
    self.backlog = backlog
    self.rate = rate

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    if checkpoint_mark is None:
      return _Reader(self, 0, time.time())
    return _Reader(self, checkpoint_mark.last_index + 1, checkpoint_mark.anchor)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _Count(beam.DoFn):
  def __init__(self):
    self.elements = Metrics.counter(NAMESPACE, 'elements')
    self.latency_ms = Metrics.distribution(NAMESPACE, 'latency_ms')
    self.wall_ms = Metrics.distribution(NAMESPACE, 'wall_ms')

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    self.elements.inc()
    now_ms = int(time.time() * 1000)
    self.wall_ms.update(now_ms)  # min..max spans the drain, excluding startup
    self.latency_ms.update(now_ms - timestamp.micros // 1000)
    yield element


def run(args):
  backlog = args.records if args.shape == 'backlog' else 0
  rate = args.rate if args.shape == 'live' else 1e9
  source = _Source(args.records, backlog, rate)
  pipeline = beam.Pipeline(options=pipeline_options(args))
  _ = (
      pipeline
      | ReadFromUnboundedSource(source, max_records_per_bundle=args.cap)
      | beam.ParDo(_Count()))
  start = time.time()
  result = pipeline.run()
  result.wait_until_finish()
  wall = time.time() - start
  counters, dists = {}, {}
  query = result.metrics().query(MetricsFilter().with_namespace(NAMESPACE))
  for c in query['counters']:
    counters[c.key.metric.name] = c.result
  for d in query['distributions']:
    dists[d.key.metric.name] = d.result
  elements = counters.get('elements', 0)
  lat = dists.get('latency_ms')
  wall_d = dists.get('wall_ms')
  span = (wall_d.max - wall_d.min) / 1000.0 if (
      wall_d and wall_d.count and wall_d.max > wall_d.min) else wall
  out = {
      'runner': args.runner,
      'shape': args.shape,
      'records': args.records,
      'cap': args.cap,
      'rate': args.rate if args.shape == 'live' else None,
      'wall_secs': round(wall, 1),
      'drain_secs': round(span, 1),
      'throughput_rec_s': round(elements / max(span, 1e-9)),
      'checkpoints': counters.get('checkpoints', 0),
  }
  if args.shape == 'live' and lat and lat.count:
    out['latency_ms_median'] = round(lat.mean, 1)
  return out


def parse_args(argv=None):
  p = argparse.ArgumentParser(description=__doc__)
  p.add_argument('--shape', choices=('backlog', 'live'), default='backlog')
  p.add_argument(
      '--runner', choices=('direct', 'prism', 'flink', 'spark'),
      default='direct')
  p.add_argument('--prism_location', default=None)
  p.add_argument('--flink_version', default='1.20')
  p.add_argument('--flink_master', default=None)
  p.add_argument('--spark_job_server_jar', default=None)
  p.add_argument('--records', type=int, default=1000000)
  p.add_argument('--cap', type=int, default=10000)
  p.add_argument('--rate', type=float, default=5000.0)
  return p.parse_args(argv)


if __name__ == '__main__':
  print(json.dumps(run(parse_args())))
