package es.santander.kafka.test.config;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

public class KafkaConfigTest {

    @Test
    public void givenPropertiesInClasspathKafkaConfigCorrectlyCreated() throws IOException {

        KafkaConfig actualValue = new KafkaConfig("kafka-config.properties");

        assertThat(actualValue.getKafkaProperties())
                .containsKey("bootstrap.servers")
                .containsValue("127.0.0.1:9092");
    }

    @Test
    public void givenPropertiesOutsideClasspathKafkaConfigCorrectlyCreated() throws IOException {

        KafkaConfig actualValue = new KafkaConfig("testProperties/kafka-config.properties", false);

        assertThat(actualValue.getKafkaProperties())
                .containsKey("bootstrap.servers")
                .containsValue("127.0.0.1:9092");
    }

    @Test(expected = NullPointerException.class)
    public void givenNoExistingPropertiesNameThrowsException() throws IOException {

       new KafkaConfig("afka-config.properties");
    }

    @Test(expected = FileNotFoundException.class)
    public void givenNonExistingPropertiesOutsideClasspathKafkaConfigCorrectlyCreated() throws IOException {

        new KafkaConfig("fake/kafka-config.properties", false);
    }
}