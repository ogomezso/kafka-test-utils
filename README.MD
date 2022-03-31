#**Disclaimer: The use of this library is only recommended for testing and should not be used on production code**

# Kafka Test Utils

## General Purpose

This library provides out of the box working clients and environment for **testing purpose**.

## Configuration

Each client rely on client property files provided on time of costruction.

The recommended way to create provide it is having a properties file on the resources folder of the test package.

A `kafka-config.properties` example looks like:

````properties
bootstrap.servers=127.0.0.1:1092,127.0.0.1:2092,127.0.0.1:3092
acks=all
client.id=client-test-producer
key.serializer=org.apache.kafka.common.serialization.IntegerSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
````
To make it available as `Properties` class:

```java
KafkaConfig kafkaConfig = new KafkaConfig("kafka-config.properties");
```
where `kafka-config.properties` should be on your test classpath.

## Basic Clients

### KafkaTestAdminClient

### KafkaTestPlainProducer

### KafkaTestPlainConsumer

## Objects

### TestTopicConfig

### PlainRecord