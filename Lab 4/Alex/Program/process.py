# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run local python .\gps_rtk\process.py --input to_process --output processed --source kafka

import argparse
import json
import logging
import os
from geopy.distance import geodesic

import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from kafka import KafkaProducer


def singleton(cls):
    instances = {}

    def getinstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return getinstance


@singleton
class Model():

    def __init__(self, checkpoint):
        with tf.Graph().as_default() as graph:
            sess = tf.compat.v1.InteractiveSession()
            saver = tf.compat.v1.train.import_meta_graph(os.path.join(checkpoint, 'export.meta'))
            saver.restore(sess, os.path.join(checkpoint, 'export'))

            inputs = json.loads(tf.compat.v1.get_collection('inputs')[0])
            outputs = json.loads(tf.compat.v1.get_collection('outputs')[0])

            self.x = graph.get_tensor_by_name(inputs['image'])
            self.p = graph.get_tensor_by_name(outputs['scores'])
            self.input_key = graph.get_tensor_by_name(inputs['key'])
            self.output_key = graph.get_tensor_by_name(outputs['key'])
            self.sess = sess


class PredictDoFn(beam.DoFn):

    def process(self, element, checkpoint):
        #model = Model(checkpoint)
        id_old = element['old']['id']
        id_newer = element['newer']['id']
        old_longitude = float(element['old']['longitude'])
        new_longitude = float(element['newer']['longitude'])
        old_latitude = float(element['old']['latitude'])
        new_latitude = float(element['newer']['latitude'])
        distance = geodesic((old_latitude, old_longitude), (new_latitude, new_longitude)).m

        time_newer = int(element['newer']['time'])
        time_old = int(element['old']['time'])
        time_diff = (time_newer - time_old)/1000
        #logging.getLogger().info(str(time_diff) + "ms " + str(distance) + "m")

        result = {"distance": distance, "elapsed_ms": time_diff ,"time": time_newer}
        return [result]

class ProduceKafkaMessage(beam.DoFn):

    def __init__(self, topic, servers, *args, **kwargs):
        beam.DoFn.__init__(self, *args, **kwargs)
        self.topic = topic;
        self.servers = servers;

    def start_bundle(self):
        self._producer = KafkaProducer(**self.servers)

    def finish_bundle(self):
        self._producer.close()

    def process(self, element):
        try:
            self._producer.send(self.topic, element[1], key=element[0])
            yield element
        except Exception as e:
            raise


def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True,
                        help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output file to write results to.')
    parser.add_argument('--model', dest='model', required=False,
                        help='Checkpoint file of the model.')
    parser.add_argument('--source', dest='source', required=True,
                        help='Data source location (text|mysql|kafka).')
    parser.add_argument('--schema ', dest='schema', required=False,
                        help='Schema path ')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True;

    with beam.Pipeline(options=pipeline_options) as p:
        if known_args.source == 'kafka':
            from beam_nuggets.io import kafkaio

            bootstrap_servers = "pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092"
            username = "#"
            password = "#"

            consumer_config = {"topic": known_args.input,
                               'bootstrap_servers': bootstrap_servers, \
                               'security_protocol': 'SASL_SSL', 'sasl_mechanism': 'PLAIN',
                               'sasl_plain_username': username, \
                               'sasl_plain_password': password, \
                               'auto_offset_reset': 'latest'}
            server_config = {'bootstrap_servers': bootstrap_servers, \
                             'security_protocol': 'SASL_SSL', 'sasl_mechanism': 'PLAIN',
                             'sasl_plain_username': username, \
                             'sasl_plain_password': password}
            images = (p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(consumer_config=consumer_config, value_decoder=bytes.decode) | 'Deserializing' >> beam.Map(lambda x: json.loads(x[1])))
            predictions = (images | 'Calculate Status' >> beam.ParDo(PredictDoFn(), known_args.model) | "Serializing" >> beam.Map(lambda x: (None, json.dumps(x).encode('utf8'))));
            predictions | 'To kafka' >> beam.ParDo(ProduceKafkaMessage(known_args.output, server_config))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()