"""A streaming word-counting workflow."""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms import window


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_topic",
        required=True,
        help=(
            "Output PubSub topic of the form " '"projects/<PROJECT>/topics/<TOPIC>".'
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input_topic",
        help=("Input PubSub topic of the form " '"projects/<PROJECT>/topics/<TOPIC>".'),
    )
    group.add_argument(
        "--input_subscription",
        help=(
            "Input PubSub subscription of the form "
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'
        ),
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = p | beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            ).with_output_types(bytes)
        else:
            messages = p | beam.io.ReadFromPubSub(
                topic=known_args.input_topic
            ).with_output_types(bytes)

        lines = messages | "decode" >> beam.Map(lambda x: x.decode("utf-8"))

        # Count the occurrences of each word.
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        counts = (
            lines
            | "split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | "pair_with_one" >> beam.Map(lambda x: (x, 1))
            | beam.WindowInto(window.FixedWindows(15, 0))
            | "group" >> beam.GroupByKey()
            | "count" >> beam.Map(count_ones)
        )

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return f"{word}: {count}"

        output = (
            counts
            | "format" >> beam.Map(format_result)
            | "encode" >> beam.Map(lambda x: x.encode("utf-8")).with_output_types(bytes)
        )

        # Write to PubSub.
        # pylint: disable=expression-not-assigned
        output | beam.io.WriteToPubSub(known_args.output_topic)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
