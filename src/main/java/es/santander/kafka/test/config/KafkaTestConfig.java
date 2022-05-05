package es.santander.kafka.test.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

public class KafkaTestConfig {

    private final Properties kafkaProperties;

    public KafkaTestConfig(String propertiesName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesName);
        kafkaProperties = new Properties();
        kafkaProperties.load(inputStream);
    }

    public KafkaTestConfig(String propertiesName, Boolean isPropertiesInClasspath) throws IOException {
        InputStream inputStream;
        if (isPropertiesInClasspath) {
            inputStream = getClass().getClassLoader().getResourceAsStream(propertiesName);
        } else {
            File propertiesFile = new File(propertiesName);
            inputStream = Files.newInputStream(propertiesFile.toPath());
        }
        kafkaProperties = new Properties();
        kafkaProperties.load(inputStream);
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    public void setProperty(String key, String value) {
        kafkaProperties.setProperty(key, value);
    }

    public <K, V> KafkaProducer<K, V> createKafkaProducer() {
        return new KafkaProducer<>(kafkaProperties);
    }

    public <K, V> KafkaConsumer<K, V> createKafkaConsumer() {
        return new KafkaConsumer<>(kafkaProperties);
    }
}