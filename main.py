import random
import time
from datetime import datetime
from multiprocessing import Process

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.queue_declare(queue="hello")


def call_back(ch, method, properties, body):
    print(f"{datetime.now()} {body}")


def publish():
    while True:
        time.sleep(random.randint(1, 3))
        channel.basic_publish(exchange="", routing_key="hello", body="hello world")


def consume():
    print("start consuming")
    channel.basic_consume(queue="hello", auto_ack=True, on_message_callback=call_back)
    channel.start_consuming()


if __name__ == "__main__":
    producer = Process(target=publish, args=())
    consumer = Process(target=consume, args=())
    prcs = [producer, consumer]
    for prc in prcs:
        prc.start()
    consumer.join()
