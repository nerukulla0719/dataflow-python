import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json


def parse_message(message):
    parsed = json.loads(message)
    return {
        "transaction_id": parsed["transaction_id"],  # Unique key for deduplication
        "payload": parsed["payload"]
    }

# Function to extract transaction_id for deduplication
def get_transaction_id(record):
    return record["transaction_id"]

def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        # Input data: Stream of JSON strings
        input = (
            p
            | "ReadFromSource" >> beam.io.ReadFromText("/users/Documents/input.txt") 
        )

        # Parse messages
        parsed_messages = input | "ParseJSON" >> beam.Map(parse_message)

        # Deduplicate based on transaction_id over a 10-minute window
        deduplicated_messages = (
            parsed_messages
            | "DeduplicateMessages" >> beam.Deduplicate(beam.transforms.deduplicate.Deduplicate.<dict>(key=get_transaction_id))
        )

        # Output deduplicated messages
        deduplicated_messages | "WriteToSink" >> beam.io.WriteToText("/users/Documents/output.txt")

if __name__ == "__main__":
    run()

