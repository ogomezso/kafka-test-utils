# **Disclaimer: The use of this library is only recommended for testing and should not be used on production code**

# Kafka Test Utils

## General Purpose

This library provides out of the box working kafka clients for **testing purpose**.

## Configuration

Each client rely on client properties files provided on time of construction.

A `kafka-config.properties` example looks like:

~~~properties
bootstrap.servers=127.0.0.1:1092,127.0.0.1:2092,127.0.0.1:3092
acks=all
client.id=client-test-producer
key.serializer=org.apache.kafka.common.serialization.IntegerSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
~~~

To make it available as `Properties` class:

~~~java
KafkaConfig kafkaConfig=new KafkaConfig("kafka-config.properties");
~~~

where `kafka-config.properties` should be on your test classpath.

The recommended way to create provide it is having a properties file on the resources folder of the test package.

but you can always pass as parameter a path to the properties file by indicating so in the constructor:
~~~java
KafkaConfig kafkaConfig=new KafkaConfig("kafka-config.properties",true);
~~~

## Objects

### TestTopicConfig

`TestTopicConfig` object wraps the basic configuration for kafka client `NewTopic` object such as:

- Topic Name
- Partition Number
- Replication Factor

This is an _inmutable object_ that provides _builder_ pattern and _getters_

### TestRecord

`TestRecord` is a _parametrized generic_ inmutable POJO that models a _Kafka Record_.

To instantiate a new object you need to parametrize **according you key & value types**

The class provides _builder_ and getter _methods_

Example:

~~~java
TestRecord<Integer, String>> recordsToProduce = PlainRecord.<Integer, String>builder()
    .key(1)
    .value("1.a")
    .build()
~~~

## Basic Clients

### KafkaTestAdminClient

KafkaTest Admin client instantiate a kafka admin client and provides functionality for:

- Create new Topics
- Describe existing Topics
- Delete existing Topics

#### Client Instantiation

1. ``KafkaConfig`` Class: Instantiate a new Config Class as related on _Configuration Section_
2. Create a new Instance of ```KafkaTestAdminClient``` as follows:

~~~java
KafkaConfig kafkaConfig = new KafkaConfig("kafka-config.properties");
KafkaTestAdminClient adminClient = new KafkaTestAdminClient(kafkaConfig);
~~~

#### Topic Creation

``createTopics()`` creates a list of topics from `TestTopicConfig` object.

Example of a single topic creation:

~~~java
TestTopicConfig topicConfig = TestTopicConfig.builder()
    .topicName("example-topic")
    .partitionNumber(3)
    .replicationFactor((short) 3)
    .build();
KafkaConfig kafkaConfig = new KafkaConfig("kafka-config.properties");
KafkaTestAdminClient adminClient = new KafkaTestAdminClient(kafkaConfig);
adminClient.createTopics(Collections.singletonList(topicConfig));
~~~

**This method has no effects on existing topics**

### Describe Topic

`describeTopics` retrieves a list of `TopicDescription` kafka objects containing all the topic information from a list
of `TestTopicConfig` object.

~~~java
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
~~~

### Delete Topics

`deleteTopics()` delete a given list of `topics` passed as List of String arguments.

~~~java
adminClient.deleteTopics(Collections.singletonList(topicConfig.getTopicName()));
~~~

### KafkaTestProducer

This client wraps `KafkaProducer` class; provide a parametrized instance that **must match with the serializers configured on KafkaConfig class**.

Implements `produceMessages` method that:
- Accepts a list of `TestRecord` objects parametrized as _the same Type you're using on the serializer config_ and `topic name string` as parameter.
- Do the flush and the producer close after producing the whole list.
- Provide abstract methods to customize the process and error handling of the producer:
- `processResult`: takes the list of `ProducerRecords<K,V>` returned by kafka client's `send` method as parameter and return a list of `ProducerRecords<K,V>`. Use this method for the post-production process you need to implement.
- `handleError`: takes and Exception and the list of `ProducerRecords<K,V>` returned by kafka client's `send` method as parameters. Use this method for error handling during the message production.
- **Key is optional**, but type is still needed on the parametrized constructor (String Type recommended)

**Note:**

**`processResult` method will be executed between flush and close of the `KafkaProducer`**

**`handleError` method will be executed in case of exception on `KafkaProducer` send method.**

Examples:

##### Producer Records
~~~java
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
~~~

##### Kafka Properties

~~~properties
bootstrap.servers=127.0.0.1:1092,127.0.0.1:2092,127.0.0.1:3092
acks=all
client.id=client-test-producer
key.serializer=org.apache.kafka.common.serialization.IntegerSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
~~~

### KafkaTestConsumer

This client wraps `KafkaConsumer` class provides a parametrized instance that **must match with the serializers configured on KafkaConfig class**.

Implements `pollOrTimeout` method that:
- Accepts a list of `String` with the names of the topics to suscribe
- Provide a abstract method `processRecords` that take as parameter and returns a list of `ConsumerRecord<K,V>`. **This method will be executed on the resolution of the poll** 
- Timeout duration of the poll. After this time consumer will be closed with the already consumed records as return value
- The number of records you want to consume. After consuming this number of records the consumer will be closed with the already consumed records as return value.
- Returns a list of `ConsumerRecords` of previous parametrized types with the already consumed records.
- To create multiple instances of consumer group just instantiate more than one class with the same `group.id`.

##### Consumer Example
~~~java
KafkaConfig kafkaConsumerConfig = new KafkaConfig("kafka-consumer-config.properties");
KafkaTestPlainConsumer<Integer, String> plainConsumer = new KafkaTestPlainConsumer<>(kafkaConsumerConfig);
List<ConsumerRecord<Integer, String>> recordsConsumed = plainConsumer.runOrTimeout(Duration.ofSeconds(30L), 1000L,
Collections.singletonList(topicConfig.getTopicName()));
System.out.println("Total Records Consumed:" + recordsConsumed.size());
recordsConsumed.forEach(record -> System.out.println("Message: " + record));
~~~

**Note: The number of records consumed depends on the batch served on each poll interval, so the number of records that will be finally consumed not only depends on the configuration passed to pollOrTimeout method but on this poll size too.**

## Explicit Clients

### KafkaTestTextFileProducer

This client extends `KafkaTestProducer`, read a text file line by line and produce **String typed messages** in 2 possible ways:

- **Messages with Key**  using `produceFromFile` method that takes as parameter:
  - `topicName`: Topic to produce
  - `filePath`: Path to the data file.
  - `keySeparator`: String to use to differentiate the key. **Key must be at the start of each line**.

- **Messages without Key**  using `produceFromFileWithout` method that takes as parameter:
    - `topicName`: Topic to produce
    - `filePath`: Path to the data file.

both methods return `List<ProducerRecord<String,String>` and expose the same abstract methods that `KafkaTestProducer`

### In memory Cluster
 
**The recommended for testing Kafka Clients within a server is [Test-Containers](https://www.testcontainers.org/modules/kafka/)** but for the ones of you that can not use `docker` on local or CI environments this library provides a very basic in-memory cluster with:

> 1. Zookeeper Server attending on `127.0.0.1:2181`
> 2. Single Broker attending on `127.0.0.1:9092`
> 3. Schema Registry server on `http://127.0.0.1:8081`
> 
> **All servers use `plaintext` as security protocol** 

You can easily start the cluster by instantiating `EmbeddedSingleNodeCluster()` as follows:

~~~java
EmbeddedSingleNodeCluster cluster = new EmbeddedSingleNodeCluster();
~~~

You should `shutdown` the cluster after finishing the test:

~~~java
cluster.shutdown();
~~~