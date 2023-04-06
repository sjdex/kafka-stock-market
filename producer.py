import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json

producer = KafkaProducer(bootstrap_servers=['<ip here>:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
producer.send('demo_test', value={'testkey':'testvalue'})

df = pd.read_csv("sample.csv")
print(df.head())

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)

producer.flush() 
