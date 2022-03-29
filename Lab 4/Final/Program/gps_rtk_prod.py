from kafka import KafkaProducer
import json
import time
import io
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import csv
import json
import time

schemaID = 100004
CSV_FILE = 'gps_rtk.csv'
TOPIC = 'to_process'

data = json.load(open('cred.json'))
bootstrap_servers = data['bootstrap_servers']
sasl_plain_username = data['Api key']
sasl_plain_password = data['Api secret']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
                         sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password,
                         #value_serializer=lambda m: encode(m))
                        value_serializer=lambda m: json.dumps(m).encode('utf-8'))

with open(CSV_FILE) as file:
    csv_reader = csv.reader(file)
    lineNumber = 0
    lastLine = {}
    for row in csv_reader:
        if lineNumber == 0:
            lastLine = {'id':lineNumber,'time': row[0], 'latitude': row[3], 'longitude': row[4]}
            lineNumber += 1
            continue

        currentLine = {'id':lineNumber,'time': row[0], 'latitude': row[3], 'longitude': row[4]}
        bothLines = {'old':lastLine, 'newer':currentLine}
        lastLine = currentLine

        print("Producing ", lineNumber, ": ", bothLines)
        lineNumber += 1
        time.sleep(1)
        producer.send(TOPIC, bothLines)
    print("Added all data")
    producer.close()
