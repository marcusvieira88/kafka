# Introduction

This project was create for learn about kafka.

Download and install kafka:

https://kafka.apache.org/downloads

And add the kafka_2.12-2.0.0/bin in the PATH:
```
nano ~/.bashrc
export PATH=~/.local/bin:$PATH
export PATH=/home/marcus/kafka_2.12-2.0.0/bin:$PATH
```
Configure the data folder for the Zookeeper and Kafka:
```
mkdir data (inside kafka_2.12-2.0.0)
cd data/ (inside kafka_2.12-2.0.0)
mkdir zookeeper (inside kafka_2.12-2.0.0)
mkdir kafka (inside kafka_2.12-2.0.0)
```
Open the zookeeper.properties and change the dataDir to dataDir=/home/marcus/kafka_2.12-2.0.0/data/zookeeper
```
nano kafka_2.12-2.0.0/config/zookeeper.properties
```
Open the server.properties and change the log.dirs to log.dirs=/home/marcus/kafka_2.12-2.0.0/data/kafka
```
nano kafka_2.12-2.0.0/config/server.properties
```
Command for create the kafka test topics:
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic first_topic --partitions 3 --replication-factor 1
```
For check the topic details:
```
kafka-topics.sh --zookeeper localhost:2181 --describe --topic first_topic --partitions 3 --replication-factor 1
```
For list all consumer groups:
```
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list
```
For check the consumer group info:
```
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group my-first-application --describe
```
