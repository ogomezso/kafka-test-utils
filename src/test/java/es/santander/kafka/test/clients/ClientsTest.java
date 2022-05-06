package es.santander.kafka.test.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.santander.kafka.test.config.KafkaTestConfig;
import es.santander.kafka.test.objects.TestRecord;
import es.santander.kafka.test.objects.TestTopicConfig;
import es.santander.kafka.test.server.EmbeddedSingleNodeCluster;

public class ClientsTest {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();
    private KafkaTestConfig adminConfig = new KafkaTestConfig("kafka-config.properties");
    private KafkaTestConfig producerConfig = new KafkaTestConfig("kafka-producer-config.properties");
    private KafkaTestConfig consumerConfig = new KafkaTestConfig("kafka-consumer-config.properties");

    public ClientsTest() throws Exception {
    }

    @AfterClass
    public static void cleanup() throws Exception {
        folder.delete();
    }

    @Test
    public void clientTests() throws Exception {
        EmbeddedSingleNodeCluster cluster = new EmbeddedSingleNodeCluster(folder.newFolder("kafka"));
        cluster.start();
        List<TestTopicConfig> expected = createTestTopics();
        adminConfig.setProperty("bootstrap.servers", cluster.getBrokerConnectString());
        KafkaTestAdminClient testAdminClient = new KafkaTestAdminClient(adminConfig);
        testAdminClient.createTopics(expected);
        List<String> topicsToDescribe = expected.stream().map(TestTopicConfig::getTopicName)
                .collect(Collectors.toList());
        List<TopicDescription> actual = testAdminClient.describeTopics(topicsToDescribe);

        assertEquals(3, actual.size());
        actual.forEach(
                topicDescription -> assertTrue(topicsToDescribe.contains(topicDescription.name())));

        testAdminClient.deleteTopics(Collections.singletonList("test-topic-1"));

        // List<TopicDescription> actualAfterDeletion =
        // testAdminClient.describeTopics(topicsToDescribe);
        // assertEquals(2, actualAfterDeletion.size());

        // List<String> topicNamesAfterDeletion = actualAfterDeletion
        // .stream()
        // .map(TopicDescription::name)
        // .collect(Collectors.toList());
        // assertThat(topicNamesAfterDeletion).doesNotContain("topic-test-1");

        producerConfig.setProperty("bootstrap.servers", cluster.getBrokerConnectString());
        KafkaTestProducer<Integer, String> producer = new KafkaTestProducer<Integer, String>(producerConfig) {
            @Override
            public List<ProducerRecord<Integer, String>> processResult(
                    List<ProducerRecord<Integer, String>> producerRecords) {
                return producerRecords;
            }

            @Override
            public void handleError(Exception e, ProducerRecord<Integer, String> record) {
                throw (new RuntimeException("Error producing: " + record, e));

            }
        };

        consumerConfig.setProperty("bootstrap.servers", cluster.getBrokerConnectString());
        KafkaTestConsumer<Integer, String> consumer = new KafkaTestConsumer<Integer, String>(consumerConfig) {
            @Override
            public List<ConsumerRecord<Integer, String>> processRecords(List<ConsumerRecord<Integer, String>> records) {
                return records;
            }
        };

        String topicName = expected.get(0).getTopicName();

        List<TestRecord<Integer, String>> expectedRecords = createTestRecords();

        List<ProducerRecord<Integer, String>> actualProducedRecords = producer.produceMessages(topicName,
                expectedRecords);

        assertEquals(expectedRecords.size(), actualProducedRecords.size());

        List<ConsumerRecord<Integer, String>> actualConsumedRecords = consumer.pollOrTimeout(Duration.ofSeconds(5L), 5L,
                Collections.singletonList(topicName));

        assertEquals(actualProducedRecords.size(), actualConsumedRecords.size());

        consumer.close();

        cluster.shutdown();
    }

    private List<TestRecord<Integer, String>> createTestRecords() {
        return Arrays.asList(
                TestRecord.<Integer, String>builder().key(1).value("1.b").build(),
                TestRecord.<Integer, String>builder().key(1).value("1.a").build(),
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
                TestRecord.<Integer, String>builder().key(4).value("4.e").build());
    }

    private List<TestTopicConfig> createTestTopics() {
        return Arrays.asList(
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 1)
                        .topicName("test-topic-1")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 1)
                        .topicName("test-topic-2")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 1)
                        .topicName("test-topic-3")
                        .build()

        );
    }

}