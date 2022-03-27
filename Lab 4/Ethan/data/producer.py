import time
import json;
import io;
from kafka import KafkaProducer

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers']
sasl_plain_username=data['Api key']
sasl_plain_password=data['Api secret']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,\
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    
file1 = open('gps.csv', 'r')

key = 1
while True:
    line = file1.readline()
 
    if not line:
        break
    arr=line.split(",")
    value={'key': key,'uTime':arr[0], 'fixMode':arr[1], 'satNum':arr[2],'latitude':arr[3],'longitude':arr[4],'altitude':arr[5],'track':arr[6],'speed':arr[7],}
    producer.send('input', value)
    print("Image with key "+str(key)+" is sent")
    time.sleep(0.1)
    key+=1
producer.close()
