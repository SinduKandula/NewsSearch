import boto3    
import botocore
import threading, logging, time
import json
from datetime import datetime
import re
import sys
from kafka import KafkaProducer
from data_dict import codedict, countrydict, ethnicdict, eventdict, groupdict, religiondict

done="False"
prev="False"

class Producer(threading.Thread):

    def read_s3(self,bucket_name):
        s3 = boto3.resource('s3')
        return s3.Bucket(bucket_name)

    def breakWords(self, code, dict_name):
       words = re.findall("...", code)
       word_str="" 
       for word in words:
            if dict_name.get(word) is not None:
                word_str += dict_name.get(word) + " "
       return word_str

    def getEvent(self,code):
        cd =eventdict.get(code)
        return cd

    def run(self):
        time.sleep(15)
        producer = KafkaProducer(bootstrap_servers='50.112.229.147:9092')
        bucket_name="gdelt-open-data"
        bucket = self.read_s3(bucket_name)

        prefix="v2/events/"+str(sys.argv[1])+".export.csv"
        global done
        global prev
        print(prefix)

        for obj in bucket.objects.filter(Prefix=prefix):
            data = obj.get()['Body']
            lines = data.read().splitlines()
            for line in lines:
                if line is None:
                    done="True"
                    break
                else :
                    tokens=line.split("\t")
                    dict={}
                    dict["Topic"] = "" 
                    if tokens[0].strip() != "":
                        dict["globaleventid"]=tokens[0] 
                    if tokens[1].strip() != "":
                        dict["SQLDATE"]=tokens[1]
                    if tokens[2].strip() != "":
                        dict["MonthYear"]=tokens[2]
                    if tokens[3].strip() != "":
                        dict["Year"]=tokens[3]
                    if tokens[6].strip() != "":
                        dict["Topic"]=tokens[6]
                    if tokens[7].strip() != "":
                        dict["Country1"]=self.breakWords(tokens[7], countrydict)
                        dict["Topic"]+=" "+self.breakWords(tokens[7], countrydict)
                    if tokens[8].strip() != "":
                        dict["Topic"]+=" "+self.breakWords(tokens[8], groupdict)
                    if tokens[9].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[9], ethnicdict)
                    if tokens[10].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[10], religiondict)
                    if tokens[11].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[11], religiondict)
                    if tokens[12].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[12], codedict)
                    if tokens[13].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[13], codedict)
                    if tokens[14].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[14], codedict)
                    if tokens[16].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+tokens[16]
                    if tokens[17].strip() != "":
                        dict["Country2"]=self.breakWords(tokens[17], countrydict)
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[17], countrydict)
                    if tokens[18].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[18], groupdict)
                    if tokens[19].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[19], ethnicdict)
                    if tokens[20].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[20], religiondict)
                    if tokens[21].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[21], religiondict)
                    if tokens[22].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[22], codedict)
                    if tokens[23].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[23], codedict)
                    if tokens[24].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[24], codedict)
                    if tokens[31].strip() != "":
                        dict["NumMentions"]=tokens[31]
                    if tokens[34].strip() != "":
                        dict["AvgTone"]=tokens[34]
                    if tokens[36].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+tokens[36]
                    if tokens[37].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[37], countrydict)
                    if tokens[44].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+tokens[44]
                    if tokens[45].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[45], countrydict)
                    if tokens[52].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+tokens[52]
                    if tokens[53].strip() != "":
                        dict["Topic"]=dict.get("Topic")+" "+self.breakWords(tokens[53], countrydict)
                    if tokens[59].strip() != "":
                        dict["DATEADDED"]=tokens[59]
                    if tokens[60].strip() != "":    
                        dict["SOURCEURL"]=tokens[60]
                        url=tokens[60].split("/")
                        i=0
                        maxl=0
                        maxi=0
                        while i < len(url):
                            if len(url[i])>maxl:
                                maxl=len(url[i])
                                maxi=i
                            i+=1
                        words=url[maxi].split("-")
                        words =' '.join(words)
                        dict["Topic"]+=" "+words
                    json_text=json.dumps(dict)
                    if json_text != "":
                        prev="True"
                    producer.send("airevents", json_text)
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

