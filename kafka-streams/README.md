
# Introduction

For work with the streams you need to create a topic and consumer(for see the result):

Create important_tweets topic:
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic important_tweets --partitions 3 --replication-factor 1
```
Create consumer results:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic important_tweets
```
