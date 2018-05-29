import boto3
import sys
import botocore
import threading, logging, time
import requests
import json
from pprint import pprint
from kafka import KafkaConsumer

class Consumer(threading.Thread):

    def run(self):
        print("Staring Consumer......")
        consumer = KafkaConsumer(bootstrap_servers='54.148.157.246:9092', consumer_timeout_ms=10000)
        consumer.subscribe(['airmentions'])
        print(consumer)
        global done
        for message in consumer:
            m=message.value.strip()
            print("message:"+m)
            headers = {'content-type': 'application/json'}
            res = requests.post("http://52.88.183.38:9200/newsearchmentions/_doc/",data = m, headers=headers)
            print(res.status_code)

        consumer.close()

def main():
    thread = Consumer()
    thread.start()

if __name__ == "__main__":
        main()
