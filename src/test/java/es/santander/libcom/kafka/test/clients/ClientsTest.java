package es.santander.libcom.kafka.test.clients;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.santander.libcom.kafka.test.config.KafkaTestConfig;
import es.santander.libcom.kafka.test.objects.TestRecord;
import es.santander.libcom.kafka.test.objects.TestTopicConfig;
import es.santander.libcom.kafka.test.server.EmbeddedSingleNodeCluster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private KafkaTestConfig adminConfig = new KafkaTestConfig("kafka-config.properties");
    private KafkaTestConfig producerConfig = new KafkaTestConfig("kafka-producer-config.properties");
    private KafkaTestConfig consumerConfig = new KafkaTestConfig("kafka-consumer-config.properties");

    public ClientsTest() throws Exception {
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
                records.forEach(record -> log.debug("record consumed: {} from partition: {}", record.value(),
                        record.partition()));
                return records;
            }
        };

        String topicName = expected.get(0).getTopicName();

        List<TestRecord<Integer, String>> expectedRecords = createTestRecords();

        List<ProducerRecord<Integer, String>> actualProducedRecords = producer.produceMessages(topicName,
                expectedRecords);

        assertEquals(expectedRecords.size(), actualProducedRecords.size());

        consumer.pollOrTimeout(Duration.ofSeconds(5L), 5L,
                Collections.singletonList(topicName));

        consumer.close();

        cluster.shutdown();
    }

    private List<TestRecord<Integer, String>> createTestRecords() {
        return Arrays.asList(
                TestRecord.<Integer, String>builder().key(1).value("1.b").build(),
                TestRecord.<Integer, String>builder().key(1).value("1.a").build(),
                TestRecord.<Integer, String>builder().key(1).value("1.c").build(),
                TestRecord.<Integer, String>builder().key(1).value("1.d").build(),
                TestRecord.<Integer, String>builder().key(1).value("1.e").build());
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