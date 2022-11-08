

# importing packages
import pandas as pd
import time, json
import datetime as dt
import requests
from kafka import KafkaProducer


df = pd.read_csv("spam.csv")

print(df.head())

# One hot encoding of Category label (spam:1, ham:0)
df['Category'] = df['Category'].apply(lambda x:0 if x=='ham' else 1)

# train and test dataset
df_train = df.sample(frac = 0.7)
 
df_test = df.drop(df_train.index)
df_test.to_csv("test.csv", index=False)

# initializing Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x,default=str).encode('utf-8'))

print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))
for i in df_train.index:
        
    # add the schema for Kafka
    data = {'schema': {
        'type': 'struct',
        'fields': [{'type': 'string', 'optional': False, 'field': 'Message'
                }, {'type': 'int', 'optional': False, 'field': 'Category'
                }]
        }, 'payload': {'Message': df_train['Message'][i],
                    'Category': df_train['Category'][i]}}
    print(data)        
    producer.send(topic="test",value=data)
        
print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))