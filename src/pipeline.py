import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.transforms.window import Sessions, TimestampedValue


class AddTimestamp(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["userID"], element["click"])

        yield TimestampedValue(element, unix_timestamp)


with beam.Pipeline() as p:
    # fmt: off
    events = p | beam.Create(
        [
            {'userID': 'Andy', 'click': 1, 'timestamp': 1603112520},
            {'userID': 'Sam', 'click': 2, 'timestamp': 1603112340},
            {'userID': 'Andy', 'click': 3, 'timestamp': 1603115820},
            {'userID': 'Andy', 'click': 4, 'timestamp': 16031123600},
        ]
    )
    # fmt: on

    timestamped_events = events | beam.ParDo(AddTimestamp())

    windowed_events = timestamped_events | beam.WindowInto(Sessions(gap_size=30 * 60))

    sum_clicks = windowed_events | beam.CombinePerKey(sum)

    sum_clicks | WriteToText(file_path_prefix="tmp/output")
