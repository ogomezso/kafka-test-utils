package org.github.ogomezso.kafka.test.objects;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Record<K, V>{
    K key;
    V value;
}
