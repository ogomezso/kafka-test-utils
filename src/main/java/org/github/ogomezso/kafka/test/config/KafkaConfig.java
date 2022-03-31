package org.github.ogomezso.kafka.test.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {

    private final Properties kafkaProperties;

    public KafkaConfig(String propertiesName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesName);
        kafkaProperties = new Properties();
        kafkaProperties.load(inputStream);
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    public <K,V> KafkaProducer<K, V> createKafkaPlainProducer() {
        return new KafkaProducer<>(kafkaProperties);
    }

    public <K,V> KafkaConsumer<K, V> createKafkaPlainConsumer() {
        return new KafkaConsumer<>(kafkaProperties);
    }
}