package es.santander.kafka.test.config;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaTestConfigTest {

    @Test
    public void givenPropertiesInClasspathKafkaConfigCorrectlyCreated() throws IOException {

        KafkaTestConfig actualValue = new KafkaTestConfig("kafka-config.properties");

        assertThat(actualValue.getKafkaProperties())
                .containsKey("bootstrap.servers")
                .containsValue("127.0.0.1:9092");
    }

    @Test
    public void givenPropertiesOutsideClasspathKafkaConfigCorrectlyCreated() throws IOException {

        KafkaTestConfig actualValue = new KafkaTestConfig("testProperties/kafka-config.properties", false);

        assertThat(actualValue.getKafkaProperties())
                .containsKey("bootstrap.servers")
                .containsValue("127.0.0.1:9092");
    }

    @Test(expected = NullPointerException.class)
    public void givenNoExistingPropertiesNameThrowsException() throws IOException {

        new KafkaTestConfig("afka-config.properties");
    }

    @Test(expected = NoSuchFileException.class)
    public void givenNonExistingPropertiesOutsideClasspathKafkaConfigCorrectlyCreated() throws IOException {

        new KafkaTestConfig("fake/kafka-config.properties", false);
    }
}