"""A minimalist word-counting workflow that counts words in Shakespeare.

Concepts:

1. Reading data from text files
2. Specifying 'inline' transforms
3. Counting a PCollection
4. Writing data to Cloud Storage as text files

To execute this pipeline locally, first edit the code to specify the output
location. Output location could be a local file path or an output prefix
on GCS. (Only update the output location marked with the first CHANGE comment.)

To execute this pipeline remotely, first edit the code to set your project ID,
runner type, the staging location, the temp location, and the output location.
The specified GCS bucket(s) must already exist. (Update all the places marked
with a CHANGE comment.)

Then, run the pipeline as described in the README. It will be deployed and run
using the Google Cloud Dataflow Service. No args are required to run the
pipeline. You can see the results in your output bucket in the GCS browser.
"""

# pytype: skip-file

# beam-playground:
#   name: WordCountMinimal
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 120
#   categories:
#     - IO
#     - Core Transforms
#     - Flatten
#     - Options
#     - Combiners
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - count
#     - strings
#     - hellobeam

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://dataflow-samples/shakespeare/kinglear.txt",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        # CHANGE 1/6: (OPTIONAL) The Google Cloud Storage path is required
        # for outputting the results.
        default="tmp/outputfile",
        help="Output file to write results to.",
    )

    # If you use DataflowRunner, below options can be passed:
    #   CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
    #   run your pipeline on the Google Cloud Dataflow Service.
    #   '--runner=DirectRunner',
    #   CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
    #   run your pipeline on the Google Cloud Dataflow Service.
    #   '--project=SET_YOUR_PROJECT_ID_HERE',
    #   CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
    #   is required in order to run your pipeline on the Google Cloud
    #   Dataflow Service.
    #   '--region=SET_REGION_HERE',
    #   CHANGE 5/6: Your Google Cloud Storage path is required for staging local
    #   files.
    #   '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
    #   CHANGE 6/6: Your Google Cloud Storage path is required for temporary
    #   files.
    #   '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
    #   '--job_name=your-wordcount-job',

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input)

        # Count the occurrences of each word.
        counts = (
            lines
            | "Split"
            >> (
                beam.FlatMap(lambda x: re.findall(r"[A-Za-z\']+", x)).with_output_types(
                    str
                )
            )
            | "PairWithOne" >> beam.Map(lambda x: (x, 1))
            | "GroupAndSum" >> beam.CombinePerKey(sum)
        )

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return f"{word}: {count}"

        output = counts | "Format" >> beam.Map(format_result)

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        output | WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
