# Introduction

This project was create for learn about kafka.

Create a developer account and app in the Twitter plataform:

https://developer.twitter.com/content/developer-twitter/en.html

Command for create the kafka topic for receive the tweets:
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter_tweets --partitions 3 --replication-factor 1
```
