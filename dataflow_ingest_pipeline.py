import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import os

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        import json
        global data
        message = element.decode('utf-8')
        data = json.loads(message)
        return [data]

class CategorizeAndWriteToBigQuery(beam.DoFn):
    def process(self, element):
        import apache_beam as beam
        if element['category'] == 'SaleInfo':
            yield beam.pvalue.TaggedOutput('SaleInfo', element)
        elif element['category'] == 'OrderInfo':
            yield beam.pvalue.TaggedOutput('OrderInfo', element)
        elif element['category'] == 'ReturnInfo':
            yield beam.pvalue.TaggedOutput('ReturnInfo', element)
        else:
            pass
class WriteToBQ(beam.DoFn):
    from apache_beam import DoFn, TimeDomain

    def __init__(self, table_reference):
        self.table_reference = table_reference
        self.rows_buffer = []

    def process(self, element,window=beam.DoFn.WindowParam):
        # print("processing elements now")
        self.rows_buffer.append(element)

        if len(self.rows_buffer) >= 2:
            # print("Batch size reached, flushing buffer")
            yield from self._flush_batch(window)
    
    def finish_bundle(self):
        from apache_beam.transforms.window import GlobalWindow
        from apache_beam.utils.windowed_value import WindowedValue
        if self.rows_buffer:
            yield from self._flush_batch(GlobalWindow())


    def _flush_batch(self,window):
        from datetime import datetime
        from google.cloud import bigquery
        import time
        from apache_beam.utils.windowed_value import WindowedValue
        from apache_beam.transforms.window import GlobalWindow
        from apache_beam.transforms.window import FixedWindows

        # print("in the flush function")
        client = bigquery.Client()

        backoff_time = 1
        max_retries = 2
        retries = 0

        while retries < max_retries:
            try:
                table_name = self.table_reference
                table = client.get_table(table_name)
                errors = client.insert_rows_json(table, self.rows_buffer)
                if not errors:
                    # print("Batch inserted successfully")
                    break
                else:
                    raise Exception(errors)
            except Exception as e:
                # print("Error inserting rows")
                retries += 1
                time.sleep(backoff_time)
                backoff_time *= 2

                if retries >= max_retries:
                    error_elements = [{
                        'message': str(row),
                        'createdby': "dataflow-error",
                        'timestamp': datetime.utcnow().isoformat() + "Z",
                        'error_message': str(e),
                        'raw_message': str(data),
                        'table_reference': self.table_reference
                    } for row in self.rows_buffer]
                    windowed_values = [WindowedValue(e, window.start, [window]) for e in error_elements]
                    for value in windowed_values:
                        yield beam.pvalue.TaggedOutput('failed', value)

        self.rows_buffer = []

def run():
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
    from apache_beam.io.gcp.pubsub import ReadFromPubSub
    from apache_beam.io.gcp.bigquery import WriteToBigQuery
    from apache_beam.transforms.window import GlobalWindows

    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )

    google_cloud_options = options.view_as(GoogleCloudOptions)

    # Set other options
    google_cloud_options.project = 'supple-hulling-431203-j4'
    google_cloud_options.region = 'us-east4'
    google_cloud_options.job_name = 'dataflow-pipeline3'
    google_cloud_options.temp_location = 'gs://ingestion-bucket-431/temp'
    google_cloud_options.staging_location = 'gs://ingestion-bucket-431/staging'

    dead_letter_schema = {
        'fields': [
            {'name': 'message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'createdby', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'table_reference', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'raw_message', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
    dead_letter_table_spec = 'supple-hulling-431203-j4:local_dataset.dead_letter_table'

    with beam.Pipeline(options=options) as p:
        messages = (p 
                    | 'ReadFromPubSub' >> ReadFromPubSub(subscription='projects/supple-hulling-431203-j4/subscriptions/Incoming_JSON_Sub')
                    | 'ParseMessages' >> beam.ParDo(ParseMessageFn())
                    | 'CategorizeAndWriteToBigQuery' >> beam.ParDo(CategorizeAndWriteToBigQuery()).with_outputs('SaleInfo', 'OrderInfo','ReturnInfo', main='main_output')
                    )
        
        for tag, table_spec in [('SaleInfo', 'supple-hulling-431203-j4.local_dataset.sales'), ('OrderInfo', 'supple-hulling-431203-j4.local_dataset.orders'),
                                ('ReturnInfo', 'supple-hulling-431203-j4.local_dataset.returns')]:
                                global_window = "Window-" + tag
                                WriteToBq = "WriteTo" + tag
                                GlobalWindow_Failed = "GlobalWindow" + tag + "-Failed"
                                Write_To_dead_letter = "Error-" + tag

                                messages_load = (
                                    messages[tag]
                                    | global_window >> beam.WindowInto(GlobalWindows())
                                    | WriteToBq >> beam.ParDo(WriteToBQ(table_spec)).with_outputs('failed')
                                    )
                                messages_failed = (
                                        messages_load.failed
                                        | GlobalWindow_Failed >> beam.WindowInto(GlobalWindows())
                                        | Write_To_dead_letter >> beam.io.WriteToBigQuery(
                                            table=dead_letter_table_spec,
                                            schema=dead_letter_schema,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                            method="STREAMING_INSERTS"
                                        )
                                    )


if __name__ == '__main__':
    run()
