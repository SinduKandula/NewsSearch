# NewsSearch

Project Idea:
To develop a search engine that helps to search for latest news and also implement scheduling.

Purpose and use case:
The purpose is to search for latest news and see the results according to relevance. The use case is to search and find the latest news from around the world.

Technologies:
1.Kafka
2.Airflow
3.Amazon S3
4.Elastic Search
5.Flask.

Proposed architecture:
Amazon S3 -> Kafka -> Elastic Search -> Flask.

Challenges:
1. To get the data from Amazon S3 for every 15 minutes and update the Elasticsearch index accordingly.
2. To automate the project.

Specifications:


