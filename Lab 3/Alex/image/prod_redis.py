from kafka import KafkaProducer
import json
import time
import io
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import numpy as np
import os
import glob

data = json.load(open('cred.json'))
bootstrap_servers = data['bootstrap_servers']
sasl_plain_username = data['Api key']
sasl_plain_password = data['Api secret']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
                         sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password,
                         key_serializer=lambda v: v.encode())

fileNumber = 0
for filename in glob.glob('./images/*.tiff'):
    with open(os.path.join(os.getcwd(), filename), 'rb') as file:
        print("Adding ", fileNumber, ": ", filename.split('\\')[1])
        value = file.read()
        producer.send('ToRedis', value, key=str((filename.split('\\')[1])))
        fileNumber = fileNumber + 1
producer.close()
