# generator/app.py
import json
import os
from kafka import KafkaProducer
from time import sleep
from transactions import create_random_transaction

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode()
    )
    while True:
        transaction = create_random_transaction()
        # message = json.dumps(transaction)
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        print(transaction) # test
        sleep(SLEEP_TIME)
