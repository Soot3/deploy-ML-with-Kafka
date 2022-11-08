from pandas import DataFrame
import time, json
import datetime as dt
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import numpy as np
from kafka import KafkaProducer
from kafka import KafkaConsumer
import tensorflow_text as text

consumer = KafkaConsumer(group_id="python-consumer", bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x:
                         json.loads(x))
consumer.subscribe("test")
print('Initialized Kafka consumer at {}'.format(dt.datetime.utcnow()))
model = keras.models.load_model('/home/soot3/kafka_2.12-3.3.1/sample_project/content/sved')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:
                         json.dumps(x,default=str).encode('utf-8'))
print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

for message in consumer:
    # parsing data
    d=message.value
    df=DataFrame(d).reset_index()

    dff = pd.Series(df.payload[2])
    # making predictions
    inference = (model.predict(dff) > 0.5).astype(int) 

    producer.send(topic="inferences",value=inference)
    print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))
