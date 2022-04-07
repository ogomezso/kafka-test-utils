package es.santander.kafka.test.clients;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import es.santander.kafka.test.config.KafkaConfig;
import es.santander.kafka.test.objects.TestRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public abstract class KafkaTestProducer<K, V> {

    private final KafkaProducer<K, V> plainProducer;


    public KafkaTestProducer(KafkaConfig kafkaConfig) {
        plainProducer = kafkaConfig.createKafkaProducer();
    }


    public abstract List<ProducerRecord<K, V>> processResult(List<ProducerRecord<K, V>> records);

    public abstract void handleError(Exception e, ProducerRecord<K, V> record);

    public List<ProducerRecord<K, V>> produceMessages(String topicName, List<TestRecord<K, V>> messages) {
        List<ProducerRecord<K, V>> recordsProduced = new ArrayList<>();
        messages.forEach(message -> {
            ProducerRecord<K, V> msg = Optional.ofNullable(message.getKey())
                    .map(k -> sendMessage(topicName, message.getKey(), message.getValue()))
                    .orElseGet(() -> sendMessageWithoutKey(topicName, message.getValue()));
            recordsProduced.add(msg);
        });
        plainProducer.flush();
        List<ProducerRecord<K, V>> processedRecords = processResult(recordsProduced);
        plainProducer.close();
        return processedRecords;
    }

    private ProducerRecord<K, V> sendMessage(String topicName, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, value);
        plainProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                log.debug("Record written {}", record);
            } else {
                handleError(exception, record);
            }
        });
        return record;
    }

    private ProducerRecord<K, V> sendMessageWithoutKey(String topicName, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, value);
        plainProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                log.debug("Record written {}", record);
            } else {
                handleError(exception, record);
            }
        });
        return record;
    }
}
