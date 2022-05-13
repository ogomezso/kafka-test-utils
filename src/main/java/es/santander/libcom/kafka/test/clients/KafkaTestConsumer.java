package es.santander.libcom.kafka.test.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import es.santander.libcom.kafka.test.config.KafkaTestConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class KafkaTestConsumer<K,V> {

    private final KafkaConsumer<K, V> consumer;

    public KafkaTestConsumer(KafkaTestConfig kafkaTestConfig) {
        consumer = kafkaTestConfig.createKafkaConsumer();
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
                    recordsConsumed.add(record);
                });
                if (recordsConsumed.size() >= numberOfRecords) isRunning = false;
            }
            consumer.commitSync(timeout);
            return recordsConsumed;
        });
        try {
           return processRecords(pollQueryResults.get(timeout.getSeconds(), TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            pollQueryResults.cancel(true);
        } finally {
            executorService.shutdown();
        }
        return processRecords(recordsConsumed);
    }

    public void close(){
         consumer.unsubscribe();
        consumer.close(Duration.ofSeconds(30L));
    }
}