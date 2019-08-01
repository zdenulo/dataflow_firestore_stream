from __future__ import absolute_import

import os
import json
import logging
import datetime
import apache_beam as beam

from apache_beam.io.gcp.pubsub import ReadFromPubSub

from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = ''
BUCKET = ''
PUBSUB_TOPIC = 'projects/{}/topics/crawled-features'.format(PROJECT)


class FirestoreWriteDoFn(beam.DoFn):
    def __init__(self):
        super(FirestoreWriteDoFn, self).__init__()

    def start_bundle(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=PROJECT)

    def process(self, element):
        fb_data = {
            'topics': element.get('page_keywords').split(','),
            'title': element.get('page_title')
        }
        logging.info('Inserting into Firebase: %s', fb_data)
        fb_doc = self.db.document('totallyNotBigtable', element.get('key'))
        result = fb_doc.create(fb_data)
        yield result


def run(run_local):
    JOB_NAME = 'firestore-stream-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    pipeline_options = {
        'project': PROJECT,
        'staging_location': 'gs://' + BUCKET + '/staging',
        'runner': 'DataflowRunner',
        'job_name': JOB_NAME,
        'disk_size_gb': 100,
        'temp_location': 'gs://' + BUCKET + '/temp',
        'save_main_session': True,
        'requirements_file': 'requirements.txt',
        'streaming': True
    }

    if run_local:
        pipeline_options['runner'] = 'DirectRunner'

    options = PipelineOptions.from_dictionary(pipeline_options)

    p = beam.Pipeline(options=options)
    crawled_features = (p
                        | 'ReadPubsub' >> ReadFromPubSub(
                topic=PUBSUB_TOPIC).with_output_types(bytes)
                        | 'JSONParse' >> beam.Map(lambda x: json.loads(x))
                        )

    firebase_stream = (crawled_features
                       | 'WriteFirebase' >> beam.ParDo(FirestoreWriteDoFn())
                       )

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_local = False
    run(run_local)
