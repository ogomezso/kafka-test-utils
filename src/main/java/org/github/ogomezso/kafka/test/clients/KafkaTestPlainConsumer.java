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
public class KafkaTestPlainConsumer<K,V> {

    private final KafkaConsumer<K, V> consumer;

    public KafkaTestPlainConsumer(KafkaConfig kafkaConfig) {
        consumer = kafkaConfig.createKafkaPlainConsumer();
    }

    public ConsumerRecord<K, V> process(ConsumerRecord<K, V> record) {
        log.info("Record: {} consumed at {}.", record, LocalDateTime.now());
        return record;
    }


    public List<ConsumerRecord<K,V>> runOrTimeout(Duration timeout, long numberOfRecords, List<String> topics) throws InterruptedException, ExecutionException, TimeoutException {
        List<ConsumerRecord<K, V>> recordsConsumed = new ArrayList<>();
        consumer.subscribe(topics);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<List<ConsumerRecord<K, V>>> pollQueryResults = executorService.submit(() -> {
            boolean isRunning = true;
            while (isRunning) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> recordsConsumed.add(process(record)));
                if (recordsConsumed.size() >= numberOfRecords) isRunning = false;
            }
            return recordsConsumed;
        });

        try {
           return pollQueryResults.get(timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            pollQueryResults.cancel(true);
        } finally {
            executorService.shutdown();
        }
        return recordsConsumed;
    }

}