# Introduction

This project was create for learn about kafka.

Create a account in the Bonsai ElasticSearch plataform:

https://bonsai.io/

Command for create the kafka topic for and produce some tweets for be consumed and saved in the elasticsearch:
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter_tweets --partitions 3 --replication-factor 1
```
