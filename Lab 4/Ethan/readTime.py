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


import argparse
import json
import logging
import os
import time

import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from kafka import KafkaProducer


class calculateTime(beam.DoFn):
    def process(self, element):
        uTime = int(element['uTime'])
        convertedTime = time.ctime(uTime/1000000)
        moving = "Stopped"

        if element['speed'] == "nan":
            speed = 0
        else:
            speed = float(element['speed'])

        if (speed > 0):
            moving = "Moving"
        result = {}
        result['gpsKey'] = (int)(element['key'])
        result['readableTime'] = convertedTime
        result['moving'] = moving
        return [result]

def _to_dictionary(line):
    result = {}
    result['uTime'], result['fixMode'], result['satNum'], result['latitude'], result['longitude'], result['altitude'], result['track'],  result['speed'] = line.split(',')
    return result

class ProduceKafkaMessage(beam.DoFn):

    def __init__(self, topic, servers, *args, **kwargs):
        beam.DoFn.__init__(self, *args, **kwargs)
        self.topic=topic
        self.servers=servers

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
  parser.add_argument('--source', dest='source', required=True,
                      help='Data source location (text|mysql|kafka).')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
    if known_args.source == 'text':
        gpsValues = (p | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)
            | 'ConvertToDict'>> beam.Map(_to_dictionary))
        processing = gpsValues | 'Time Calculation' >> beam.ParDo(calculateTime())
        processing | 'WriteToText' >> beam.io.WriteToText(known_args.output)
    elif known_args.source == 'mysql':
        from beam_nuggets.io import relational_db
        input_config = relational_db.SourceConfiguration(
            drivername='mysql+pymysql', host=known_args.input,  
            port=3306,                  username='user',
            password='SOFE4630U',       database='myDB',
        )
        output_config = relational_db.SourceConfiguration(
            drivername='mysql+pymysql', host=known_args.output,  
            port=3306,                  username='user',
            password='SOFE4630U',       database='myDB',
        )
        gpsValues= (p | "Read from SQL">>relational_db.ReadFromDB(source_config=input_config,
            table_name='gps',query='select * from gps'))
        processing = (gpsValues | 'Time Calculation' >> beam.ParDo(calculateTime()))
        table_config = relational_db.TableConfiguration(
            name='results',
            create_if_missing=True,
            primary_key_columns=['gpsKey']
        )
        processing| "To SQL">> relational_db.Write(source_config=output_config,table_config=table_config)
    elif known_args.source == 'kafka':
        from beam_nuggets.io import kafkaio
        consumer_config = {"topic": known_args.input,'bootstrap_servers':'pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092',\
            'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_plain_username':'IG72T7O3FXU5CUIW',\
            'sasl_plain_password':"5HTZDUn/27NbRhzHjAN0c00UqlZiOko9pqmdZkG8MEowhIxelhDZ4vP7xygHb3v+",\
                'auto_offset_reset':'latest'}
        server_config = {'bootstrap_servers':'pkc-v12gj.northamerica-northeast2.gcp.confluent.cloud:9092',\
            'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_plain_username':'IG72T7O3FXU5CUIW',\
            'sasl_plain_password':"5HTZDUn/27NbRhzHjAN0c00UqlZiOko9pqmdZkG8MEowhIxelhDZ4vP7xygHb3v+"}
        
        gpsValues = (p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
            consumer_config=consumer_config,value_decoder=bytes.decode) 
            | 'Deserializing' >> beam.Map(lambda x : json.loads(x[1])))
        processing = (gpsValues | 'Time Calculation' >> beam.ParDo(calculateTime())
            | "Serializing" >> beam.Map(lambda x: (None,json.dumps(x).encode('utf8'))))
        processing |'To kafka' >> beam.ParDo(ProduceKafkaMessage(known_args.output,server_config))
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
