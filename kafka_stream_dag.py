import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import SerializingProducer

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery Failed {msg.key()} : {err}")
        return
    print(f"Record {msg.key()} produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    import time
    import logging
    kafka_config = {
    'bootstrap.servers':'pkcxxxxxxx.xxxx.xxxx.xx:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username' : 'V2DGxxxxxxxxxxx',
    'sasl.password' : '3w+T8TwoEJxxxxxxxxxxxxxxxxxkehUdJ9yOpMYxT9/PD'
    }
    producer = SerializingProducer(kafka_config)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 300:
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.produce(topic = 'user_data',key  = str(res['id']),value = json.dumps(res).encode('utf-8'),on_delivery=delivery_report)
            producer.flush()
            print("Records Inserted!!")
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

default_args = {
    'owner' : 'airScholar',
    'start_date' : datetime(2023,9,3,10,00)
}
dag = DAG('user_automation',
default_args = default_args,
schedule_interval = '@daily',catchup = False)

streaming_task = PythonOperator(task_id = 'ingest_data_from_api',
python_callable = stream_data,
dag = dag)




    