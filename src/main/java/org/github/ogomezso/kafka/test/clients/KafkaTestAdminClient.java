package org.github.ogomezso.kafka.test.clients;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.github.ogomezso.kafka.test.config.KafkaConfig;
import org.github.ogomezso.kafka.test.objects.TestTopicConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaTestAdminClient {

    private final AdminClient adminClient;

    public KafkaTestAdminClient(KafkaConfig kafkaConfig) {
        this.adminClient = AdminClient.create(kafkaConfig.getKafkaProperties());
    }

    public void createTopics(List<TestTopicConfig> topicsToCreate) throws ExecutionException, InterruptedException {

        ListTopicsResult topics = adminClient.listTopics();
        Set<String> names = topics.names().get();

        List<TestTopicConfig> filteredTopics = topicsToCreate.stream().filter(topic -> !names.contains(topic.getTopicName())).collect(Collectors.toList());
        List<NewTopic> newTopics = filteredTopics.stream().map(topic -> new NewTopic(topic.getTopicName(), topic.getPartitionNumber(), topic.getReplicationFactor())).collect(Collectors.toList());

        CreateTopicsResult result = adminClient.createTopics(newTopics);
        result.values().forEach((key, value1) -> {
            log.info("topic {} successfully created", key);
        });
    }

    public void deleteTopics(List<String> topicsToDelete) {
        DeleteTopicsResult result = adminClient.deleteTopics(topicsToDelete);
        result.topicNameValues().forEach((key, value) -> log.info("Topic {} successfully deleted", key));
    }

    public List<TopicDescription> describeTopics(List<String> topicsToDescribe) {
       List<TopicDescription> result = new ArrayList<>();
        DescribeTopicsResult topicDescriptions = adminClient.describeTopics(topicsToDescribe);
       topicDescriptions.topicNameValues().forEach((name,description) -> {
           try {
               TopicDescription topicDescription = description.get();
               log.info("Topic name:{}, isInternal: {}, authorizedOps: {}, partitions {}", topicDescription.name(), topicDescription.isInternal(),topicDescription.authorizedOperations(),topicDescription.partitions());
               result.add(topicDescription);
           } catch (InterruptedException | ExecutionException e) {
               e.printStackTrace();
           }
       });
       return result;
    }
}
