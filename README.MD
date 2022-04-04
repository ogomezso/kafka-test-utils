# **Disclaimer: The use of this library is only recommended for testing and should not be used on production code**

# Kafka Test Utils

## General Purpose

This library provides out of the box working clients and environment for **testing purpose**.

## Configuration

Each client rely on client property files provided on time of construction.

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
KafkaConfig kafkaConfig=new KafkaConfig("kafka-config.properties");
```

where `kafka-config.properties` should be on your test classpath.

## Objects

### TestTopicConfig

```TestTopicConfig``` object wraps the basic configuration for kafka client `NewTopic` object such as:

- Topic Name
- Partition Number
- Replication Factor

This is an _inmutable object_ that provides _builder_ pattern and _getters_

### TestRecord

`TestRecord` is a _parametrized generic_ inmutable POJO that models a _Kafka Record_.

To instantiate a new object you need to parametrize **according you key & value types**

The class provides _builder_ and getter _methods_

Example:

````
TestRecord<Integer, String>> recordsToProduce = PlainRecord.<Integer, String>builder()
    .key(1)
    .value("1.a")
    .build()
````

## Basic Clients

### KafkaTestAdminClient

KafkaTest Admin client instantiate a kafka admin client and provides functionality for:

- Create new Topics
- Describe existing Topics
- Delete existing Topics

#### Client Instantiation

1. ``KafkaConfig`` Class: Instantiate a new Config Class as related on _Configuration Section_
2. Create a new Instance of ```KafkaTestAdminClient``` as follows:

```
KafkaConfig kafkaConfig = new KafkaConfig("kafka-config.properties");
KafkaTestAdminClient adminClient = new KafkaTestAdminClient(kafkaConfig);
```

#### Topic Creation

``createTopics()`` creates a list of topics from `TestTopicConfig` object.

Example of a single topic creation:

````
TestTopicConfig topicConfig = TestTopicConfig.builder()
    .topicName("example-topic")
    .partitionNumber(3)
    .replicationFactor((short) 3)
    .build();
KafkaConfig kafkaConfig = new KafkaConfig("kafka-config.properties");
KafkaTestAdminClient adminClient = new KafkaTestAdminClient(kafkaConfig);
adminClient.createTopics(Collections.singletonList(topicConfig));
````

**This method has no effects on existing topics**

### Describe Topic

`describeTopics` retrieves a list of `TopicDescription` kafka objects containing all the topic information from a list
of `TestTopicConfig` object.

```
List<TopicDescription> topicDescriptionList = adminClient
    .describeTopics(Collections.singletonList(topicConfig.getTopicName()));
topicDescriptionList.forEach(description -> {
    System.out.println("************************");
    System.out.println("Topic Name:" + description.name());
    System.out.println("Topic Id:" + description.topicId());
    System.out.println("isInternal" + description.isInternal());
    System.out.println("Topic ACLS:");
    Optional.ofNullable(description.authorizedOperations()).ifPresent(ops ->
        ops.forEach(aclOperation ->
            Optional.ofNullable(aclOperation).ifPresent(System.out::println)
        ));
    System.out.println("Partitions:");
    description.partitions().forEach(partition -> {
        System.out.println("PartitionId: " + partition.partition());
        System.out.println("Replicas:");
        partition.replicas().forEach(replica -> {
            System.out.println("Id:" + replica.id());
            System.out.println("Host: " + replica.host());
            System.out.println("Rack: " + replica.rack());
        });
        System.out.println("Leader: " + partition.leader());
    });
    System.out.println("************************");
});
```

### Delete Topics

`deleteTopics()` delete a given list of `topics` passed as List of String arguments.

```
adminClient.deleteTopics(Collections.singletonList(topicConfig.getTopicName()));
```

### KafkaTestPlainProducer

This client wraps `KafkaProducer` class; provide a parametrized instance that **must match with the serializers configured on KafkaConfig class**.

Implements `produceMessages` method that:
- Accepts a list of `TestRecord` objects parametrized as _simple java types_ and `topic name string` as parameter.
- Do the flush and the producer close after producing the whole list.
- Provide abstract methods to customize the process and error handling of the producer:
- `processResult`: takes the list of `ProducerRecords<K,V>` returned by kafka client's `send` method as parameter and return a list of `ProducerRecords<K,V>`. Use this method for the post-production process you need to implement.
- `handleError`: takes and Exception and the list of `ProducerRecords<K,V>` returned by kafka client's `send` method as parameters. Use this method for error handling during the message production.
- 
**Note:**

**`processResult` method will be executed between flush and close of the `KafkaProducer`**

**`handleError` method will be execute in case of exception on `KafkaProducer` send method.**

Examples:

```producer-records
 KafkaConfig kafkaProducerConfig = new KafkaConfig("kafka-producer-config.properties");
 KafkaTestPlainProducer<Integer, String> plainProducer = new KafkaTestPlainProducer<>(kafkaProducerConfig);
 List<TestRecord<Integer, String>> recordsToProduce = List.of(
    TestRecord.<Integer, String>builder().key(1).value("1.a").build(),
    TestRecord.<Integer, String>builder().key(1).value("1.b").build(),
    TestRecord.<Integer, String>builder().key(1).value("1.c").build(),
    TestRecord.<Integer, String>builder().key(1).value("1.d").build(),
    TestRecord.<Integer, String>builder().key(1).value("1.e").build(),
    TestRecord.<Integer, String>builder().key(2).value("2.a").build(),
    TestRecord.<Integer, String>builder().key(2).value("2.b").build(),
    TestRecord.<Integer, String>builder().key(2).value("2.c").build(),
    TestRecord.<Integer, String>builder().key(2).value("2.d").build(),
    TestRecord.<Integer, String>builder().key(2).value("2.e").build(),
    TestRecord.<Integer, String>builder().key(3).value("3.a").build(),
    TestRecord.<Integer, String>builder().key(3).value("3.b").build(),
    TestRecord.<Integer, String>builder().key(3).value("3.c").build(),
    TestRecord.<Integer, String>builder().key(3).value("3.d").build(),
    TestRecord.<Integer, String>builder().key(3).value("3.e").build(),
    TestRecord.<Integer, String>builder().key(4).value("4.a").build(),
    TestRecord.<Integer, String>builder().key(4).value("4.b").build(),
    TestRecord.<Integer, String>builder().key(4).value("4.c").build(),
    TestRecord.<Integer, String>builder().key(4).value("4.d").build(),
    TestRecord.<Integer, String>builder().key(4).value("4.e").build()
 );
 plainProducer.produceMessages(topicConfig.getTopicName(), recordsToProduce);
```
```kafka-producer-config.properties
bootstrap.servers=127.0.0.1:1092,127.0.0.1:2092,127.0.0.1:3092
acks=all
client.id=client-test-producer
key.serializer=org.apache.kafka.common.serialization.IntegerSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

### KafkaTestPlainConsumer

This client wraps `KafkaConsumer` class; provide a parametrized instance that **must match with the serializers configured on KafkaConfig class**.

Implements `pollOrTimeout` method that:
- Accepts a list of `String` with the names of the topics to suscribe
- Provide a abstract method `processRecords` that take as parameter and returns a list of `ConsumerRecord<K,V>`. **This method will be executed on the resolution of the poll** 
- Timeout duration of the poll. After this time consumer will be closed with the already consumed records as return value
- The number of records you want to consume. After consuming this number of records the consumer will be closed with the already consumed records as return value.
- Returns a list of `ConsumerRecords` of previous parametrized types with the already consumed records.
- To create multiple instances of consumer group just instantiate more than one class with the same `group.id`.

```consumer example
KafkaConfig kafkaConsumerConfig = new KafkaConfig("kafka-consumer-config.properties");
KafkaTestPlainConsumer<Integer, String> plainConsumer = new KafkaTestPlainConsumer<>(kafkaConsumerConfig);
List<ConsumerRecord<Integer, String>> recordsConsumed = plainConsumer.runOrTimeout(Duration.ofSeconds(30L), 1000L,
Collections.singletonList(topicConfig.getTopicName()));
System.out.println("Total Records Consumed:" + recordsConsumed.size());
recordsConsumed.forEach(record -> System.out.println("Message: " + record));
```