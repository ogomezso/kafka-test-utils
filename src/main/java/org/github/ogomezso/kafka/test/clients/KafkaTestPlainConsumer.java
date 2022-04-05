package org.github.ogomezso.kafka.test.clients;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.ogomezso.kafka.test.config.KafkaConfig;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public abstract class KafkaTestPlainConsumer<K,V> {

    private final KafkaConsumer<K, V> consumer;

    public KafkaTestPlainConsumer(KafkaConfig kafkaConfig) {
        consumer = kafkaConfig.createKafkaPlainConsumer();
    }

    public abstract List<ConsumerRecord<K, V>> processRecords(List<ConsumerRecord<K, V>> record);


    public List<ConsumerRecord<K,V>> pollOrTimeout(Duration timeout, long numberOfRecords, List<String> topics) throws InterruptedException, ExecutionException {
        List<ConsumerRecord<K, V>> recordsConsumed = new ArrayList<>();
        consumer.subscribe(topics);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<List<ConsumerRecord<K, V>>> pollQueryResults = executorService.submit(() -> {
            boolean isRunning = true;
            while (isRunning) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));

                records.forEach(record -> {
                    log.debug("Record: {} received at {}", record, LocalDateTime.now());
                    recordsConsumed.add(record);
                });
                if (recordsConsumed.size() >= numberOfRecords) isRunning = false;
            }
            return recordsConsumed;
        });

        try {
           return processRecords(pollQueryResults.get(timeout.getSeconds(), TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            log.debug("Poll cancelled after {} seconds", timeout.getSeconds());
            pollQueryResults.cancel(true);
        } finally {
            executorService.shutdown();
        }
        return processRecords(recordsConsumed);
    }
}