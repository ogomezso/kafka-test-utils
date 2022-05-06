package es.santander.libcom.kafka.test.clients;

import org.apache.kafka.clients.producer.ProducerRecord;

import es.santander.libcom.kafka.test.config.KafkaTestConfig;
import es.santander.libcom.kafka.test.objects.TestRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public abstract class KafkaTestTextFileProducer extends KafkaTestProducer<String, String> {
    public KafkaTestTextFileProducer(KafkaTestConfig kafkaTestConfig) {
        super(kafkaTestConfig);
    }

    public abstract List<ProducerRecord<String, String>> processResult(List<ProducerRecord<String, String>> producerRecords);

    public abstract void handleError(Exception e, ProducerRecord<String, String> record);

    public List<ProducerRecord<String, String>> produceFromFile(String topicName, String filePath, String keySeparator) throws IOException {

        return readFileAndProduce(topicName, filePath, keySeparator);
    }

    public List<ProducerRecord<String, String>> produceFromFileWithoutKey(String topicName, String filePath) throws IOException {

        return readFileAndProduce(topicName, filePath);
    }

    private List<ProducerRecord<String, String>> readFileAndProduce(String topicName, String filePath) throws IOException {
        return readFileAndProduce(topicName, filePath, null);
    }

    private List<ProducerRecord<String, String>> readFileAndProduce(String topicName, String filePath, String keySeparator) throws IOException {
        Path path = Paths.get(filePath);
        Charset charset = StandardCharsets.UTF_8;

        BufferedReader bufferedReader = Files.newBufferedReader(path, charset);
        String line;
        List<TestRecord<String, String>> recordsToProduce = new ArrayList<>();
        while ((line = bufferedReader.readLine()) != null) {
            String finalLine = line;
            TestRecord<String, String> record = Optional.ofNullable(keySeparator)
                    .map(k -> processLineWithKey(finalLine, k))
                    .orElse(processLineWithoutKey(finalLine));
            recordsToProduce.add(record);
        }
        return super.produceMessages(topicName, recordsToProduce);
    }
    private TestRecord<String, String> processLineWithoutKey(String line) {
        return TestRecord.<String, String>builder()
                .key(null)
                .value(line)
                .build();
    }

    private TestRecord<String, String> processLineWithKey(String line, String keySeparator) {
        String[] chunks = line.split(Pattern.quote(keySeparator));
        return TestRecord.<String, String>builder()
                .key(chunks[0])
                .value(chunks[1])
                .build();
    }
}
