package org.github.ogomezso.kafka.test.clients;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.ogomezso.kafka.test.config.KafkaConfig;
import org.github.ogomezso.kafka.test.objects.TestRecord;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class KafkaTestPlainProducer<K, V> {

    private final KafkaProducer<K, V> plainProducer;


    public KafkaTestPlainProducer(KafkaConfig kafkaConfig) {
        plainProducer = kafkaConfig.createKafkaPlainProducer();
    }


    public List<ProducerRecord<K, V>> produceMessages(String topicName, List<TestRecord<K, V>> messages) {
        List<ProducerRecord<K, V>> recordsProduced = new ArrayList<>();
        messages.forEach(
                message -> {
                    ProducerRecord<K, V> msg = null;
                    try {
                        msg = sendMessage(topicName, message.getKey(), message.getValue());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    recordsProduced.add(msg);
                }
        );
        plainProducer.flush();
        plainProducer.close();
        return recordsProduced;
    }

    public ProducerRecord<K, V> sendMessage(String topicName, K key, V value) throws InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, value);
        plainProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                log.info("Record written {}", record);
            } else {
                exception.printStackTrace(System.err);
            }
        });
        return record;
    }
}
