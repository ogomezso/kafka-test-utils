package org.github.ogomezso.kafka.test.objects;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class TestRecord<K, V>{
    K key;
    V value;
}
