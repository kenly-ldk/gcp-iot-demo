"""
A streaming value-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging

import json
import time

import six

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def Extracting_X_Value(text):
    x_value = json.loads(text.strip())["raw_accelerometer_data"].split(',')[0].split('=')[1]
    if x_value == "-0.0" or x_value == "0.0":
        x_value = "0.0"
    return x_value

class Alerting_X_Value(beam.DoFn):
    def process(self, word_count):
        (word, count) = word_count
        if (word == '-1.0' and count == 10):
            yield '[{}] Alerting {}: {}'.format(time.ctime(), word, count).encode()

def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_topic', required=True,
        help=('Output PubSub topic of the form '
              '"projects/<PROJECT>/topic/<TOPIC>".'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        lines = p | beam.io.ReadStringsFromPubSub(
            subscription=known_args.input_subscription)
    else:
        lines = p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic)

    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    counts = (lines
            #   | 'print1' >> beam.Map(print)
              | 'split' >> beam.Map(Extracting_X_Value)
              | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
              | beam.WindowInto(window.SlidingWindows(10, 1, 0))
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(count_ones))

    # Branch 1: Alert when x hits -1.0 = 10 x times, by writing a message to PubSub
    alert = (counts | 'filter' >> beam.ParDo(Alerting_X_Value())
                    | 'write_to_pubsub' >> beam.io.WriteStringsToPubSub(known_args.output_topic))

    # Branch 2: Print out the output
    # Format the counts into a PCollection of strings.
    def format_result(word_count):
        (word, count) = word_count
        return '[{}] {}: {}'.format(time.ctime(), word, count)

    output = (counts | 'format' >> beam.Map(format_result)
                     | 'print' >> beam.Map(print))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
