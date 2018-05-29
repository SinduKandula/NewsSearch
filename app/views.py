from app import app
import requests
import json
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
 
es = Elasticsearch('http://52.88.183.38:9200')  

@app.route('/', methods=["GET","POST"])
def index():

	stext=request.form.get("stext")
	if stext is not None:
		resp = es.search(index="newsearchevents", body={"query": {"match": {"Topic": stext}}})
        	data = [doc for doc in resp['hits']['hits']]
		eventid=[]
		for doc in data:
			eventid.append(doc['_source']['globaleventid'])
		datalink=[]
		url=set()
		for eid in eventid:
			print eid
			result = es.search(index="newsearchmentions", body={"query": {"match": {"globaleventid": eid}}})
			datalink =[doc for doc in result['hits']['hits']]
			for doc in datalink:
				url.add(doc['_source']['mentionurl'])
				
		

		return render_template("title.html",stext=stext, response=url)
	

	return render_template('title.html')

