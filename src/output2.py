import csv
import json
from time import sleep
import time
from json import dumps
from kafka import KafkaProducer
import numpy as np
from datetime import datetime
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

tab_data = []
for i in range(0,50):
    n = np.random.rand(1,24)
    n = n.tolist()
    tab_data.append(n)
    print(tab_data)
tab_data_csv = []

for e in range(24):
    print(str(e))
    timestamp = e
    ran = float(np.random.normal(0, 0.1)) 

    data = {"time": float(time.time()),
    "val1" : e,
    "val2" : e+1
    }

    producer.send('topic1', value=data)
    sleep(1)