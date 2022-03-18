from kafka import KafkaProducer;
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import pandas as pd
3
# assign dataset names
schemaID=100003

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers']
sasl_plain_username=data['Api key']
sasl_plain_password=data['Api secret']

schema = avro.schema.parse(open("./schema.avsc").read())
writer = DatumWriter(schema)

# create empty list
dataframes_list = []
line = 0
  
# append datasets into teh list
sensor_df = pd.read_csv("D:/sensor.csv").values.tolist()
groundtruth_df = pd.read_csv("D:/groundtruth.csv").values.tolist()
for x in range(50):
    dataframes_list.append(sensor_df[x] + groundtruth_df[x])


def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big')+bytes_writer.getvalue()

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: encode(m))

for row in dataframes_list:
    value ={'id':line+1,'sensor1':str(row[0]),'sensor2': str(row[1]),'ground_truth1': str(row[2]),'ground_truth2': str(row[3]),'ground_truth3':str(row[4]),'ground_truth4':str(row[5]),'ground_truth5':str(row[6]),'ground_truth6':str(row[7]),'ground_truth7':str(row[8]), 'modified':int(1000*time.time())}
    producer.send('ToMySQLSensor', value)
    line+=1

producer.close()
