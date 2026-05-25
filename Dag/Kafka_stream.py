from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 11)
}

def stream_data(): 
    import time
    import json
    import uuid
    from kafka import KafkaProducer
    import requests
    
    logging.info("Starting Kafka stream...")
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = requests.get("https://randomuser.me/api/")
            res = res.json()
            res = res['results'][0]
            
            location = res['location']
            data = {
                'id': str(uuid.uuid4()),
                'first_name': res['name']['first'],
                'last_name': res['name']['last'],
                'gender': res['gender'],
                'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
                'post_code': location['postcode'],
                'email': res['email'],
                'username': res['login']['username'],
                'registered_date': res['registered']['date'],
                'phone': res['phone'],
                'picture': res['picture']['medium']
            }
            
            producer.send('users_created', json.dumps(data).encode('utf-8'))
            logging.info(f"Sent: {data['first_name']} {data['last_name']}")
        except Exception as e:
            logging.error(f'Error: {e}')
            continue
    
    producer.close()
    logging.info("Kafka stream completed")

with DAG(
    'api_to_kafka_streaming',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['kafka', 'streaming']
) as dag:
    streaming_task = PythonOperator(
        task_id='streaming_from_api_to_kafka',
        python_callable=stream_data
    )
