package es.santander.kafka.test.clients;

import es.santander.kafka.test.config.KafkaTestConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import es.santander.kafka.test.objects.TestRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
public abstract class KafkaTestProducer<K, V> {

    private final KafkaProducer<K, V> plainProducer;


    public KafkaTestProducer(KafkaTestConfig kafkaTestConfig) {
        plainProducer = kafkaTestConfig.createKafkaProducer();
    }


    public abstract List<ProducerRecord<K, V>> processResult(List<ProducerRecord<K, V>> records);

    public abstract void handleError(Exception e, ProducerRecord<K, V> record);

    public List<ProducerRecord<K, V>> produceMessages(String topicName, List<TestRecord<K, V>> messages) {
        List<ProducerRecord<K, V>> recordsProduced = new ArrayList<>();
        messages.forEach(message -> {
            Optional<ProducerRecord<K, V>> msg = Optional.ofNullable(message.getKey())
                    .map(key -> sendMessage(new ProducerRecord<>(topicName, message.getKey(), message.getValue())))
                    .orElseGet(() -> sendMessage(new ProducerRecord<>(topicName, message.getValue())));
            msg.ifPresent(recordsProduced::add);
        });
        plainProducer.flush();
        List<ProducerRecord<K, V>> processedRecords = processResult(recordsProduced);
        plainProducer.close();
        return processedRecords;
    }

    private Optional<ProducerRecord<K, V>> sendMessage(ProducerRecord<K,V> record) {
        try {
            plainProducer.send(record).get();
            return Optional.of(record);
        } catch (InterruptedException | ExecutionException e) {
            handleError(e, record);
        }
        return Optional.empty();
    }

}