import boto3    
import botocore
import threading, logging, time
import json
from datetime import datetime
import re
from kafka import KafkaProducer

done="False"
prev="False"

class Producer(threading.Thread):

    def read_s3(self,bucket_name):
        s3 = boto3.resource('s3')
        return s3.Bucket(bucket_name)

    def run(self):
        time.sleep(15)
        producer = KafkaProducer(bootstrap_servers='52.26.109.173:9092')
        print("Starting the Producer")
        global done
        global prev
        bucket_name="gdelt-open-data"
        bucket = self.read_s3(bucket_name)

        prefix="v2/mentions/"+str(sys.argv[1])+".mentions.csv"

        for obj in bucket.objects.filter(Prefix=prefix):
            data = obj.get()['Body']
            lines = data.read().splitlines()
            for line in lines:
                if line is None:
                    done="True"
                    break
                else:
                    tokens=line.split("\t")
                    dict={}
                    if tokens[0].strip() != "":
                        dict["globaleventid"]=tokens[0] 
                    if tokens[5].strip() != "":
                        dict["mentionurl"]=tokens[5]
                    json_text=json.dumps(dict)
                    if json_text != "":
                        prev="True"
                    producer.send("airmentions", json_text)
                    print("ATTRIBUTE : {0}".format(json_text))
        done="True"



def main():
    producer = Producer()
    producer.start()
    while True:
        if done=="True" and prev=="True":
            raise SystemExit

if __name__ == "__main__":
    main()

