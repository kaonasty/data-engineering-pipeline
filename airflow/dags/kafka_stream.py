from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def consume_messages():
    logging.info("Starting Kafka Consumer...")
    consumer = KafkaConsumer(
        'users_stream',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Consume a few messages then exit
    message_count = 0
    for message in consumer:
        logging.info(f"Received message: {message.value}")
        message_count += 1
        if message_count >= 5:
            break
            
    logging.info("Finished consuming messages")

with DAG(
    'kafka_stream_test',
    default_args=default_args,
    description='A simple DAG to test Kafka connectivity',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    consume_task = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_messages,
    )
