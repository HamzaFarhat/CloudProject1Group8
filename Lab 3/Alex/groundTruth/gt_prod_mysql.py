from kafka import KafkaProducer
import json
import time
import io
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import csv
import json
import time

schemaID = 100002
CSV_FILE = 'groundtruth_2012-01-08.csv'
TOPIC = 'groundtruth'

data = json.load(open('cred.json'))
bootstrap_servers = data['bootstrap_servers']
sasl_plain_username = data['Api key']
sasl_plain_password = data['Api secret']

schema = avro.schema.parse(open("./schema.avsc").read())
writer = DatumWriter(schema)


def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big') + bytes_writer.getvalue()


producer = KafkaProducer(bootstrap_servers=bootstrap_servers, security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
                         sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password,
                         value_serializer=lambda m: encode(m))

with open(CSV_FILE) as file:
    csv_reader = csv.reader(file)
    lineNumber = 0
    for row in csv_reader:
        addData = {'id':lineNumber,'time': row[0], 'one': row[1], 'two': row[2], 'three': row[3], 'four': row[4], 'five': row[5], 'six': row[6]}
        print("Producing ", lineNumber, ": ", addData)
        lineNumber += 1
        time.sleep(1)
        producer.send(TOPIC, addData)
    print("Added all data")
    producer.close()

#value = {'id': 15, 'name': "user", 'email': 'test@gmail.com', 'department': "IT", 'modified': int(1000 * time.time())}
#producer.send('ToMySQL', value)
#producer.close()
