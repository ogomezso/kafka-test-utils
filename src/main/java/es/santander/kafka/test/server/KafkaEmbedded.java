package es.santander.kafka.test.server;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class KafkaEmbedded {
    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";
    private final Properties effectiveConfig;
    private final KafkaServer kafka;

    public KafkaEmbedded(File tmpDir) throws IOException {

        String logDir = tmpDir.getAbsolutePath();
        effectiveConfig = effectiveConfigFrom(logDir);
        final boolean loggingEnabled = true;

        final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
        log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...", logDir, zookeeperConnect());
        kafka = createServer(kafkaConfig);
        log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...", kafkaConfig.listeners(), zookeeperConnect());
    }

    private KafkaServer createServer(KafkaConfig kafkaConfig) {
        KafkaServer server = new KafkaServer(kafkaConfig, Time.SYSTEM, Option.empty(), false);
        server.startup();
        return server;
    }

    private Properties effectiveConfigFrom(String logDir) throws IOException {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://127.0.0.1:9092");
        effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
        effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir);
        effectiveConfig.setProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.DefaultReplicationFactorProp(), "1");
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), "1");
        return effectiveConfig;
    }

    public String zookeeperConnect() {
        return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
    }

    /**
     * Stop the broker.
     */
    public void stop() {
        log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...", kafka.config().listeners(), zookeeperConnect());
        kafka.shutdown();
        kafka.awaitShutdown();
        log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...", kafka.config().listeners(), zookeeperConnect());
    }

    KafkaServer kafkaServer() {
        return kafka;
    }
}
