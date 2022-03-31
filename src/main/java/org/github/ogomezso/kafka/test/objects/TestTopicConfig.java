package org.github.ogomezso.kafka.test.objects;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class TestTopicConfig {
    String topicName;
    int partitionNumber;
    short replicationFactor;
}
