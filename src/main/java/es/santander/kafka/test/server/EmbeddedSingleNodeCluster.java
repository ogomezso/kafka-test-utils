package es.santander.kafka.test.server;

import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class EmbeddedSingleNodeCluster {

    private final File tmpDir = Files.createTempDir();
    private final ZooKeeperEmbedded zk;
    private final KafkaEmbedded broker;
    private final SchemaRegistryEmbedded sr;

    public EmbeddedSingleNodeCluster() throws Exception {
        log.info("-------------------------------------------");
        log.info("Starting in-memory Zookeeper node ");
        log.info("-------------------------------------------");
        zk = new ZooKeeperEmbedded(tmpDir);
        log.info("-------------------------------------------");
        log.info("Zookeeper node started");
        log.info("-------------------------------------------");
        log.info("-------------------------------------------");
        log.info("Starting in-memory broker node ");
        log.info("-------------------------------------------");
        broker = new KafkaEmbedded(tmpDir);
        log.info("-------------------------------------------");
        log.info("In-memory broker node started");
        log.info("-------------------------------------------");
        log.info("-------------------------------------------");
        log.info("Starting in-memory schema registry node ");
        log.info("-------------------------------------------");
        sr = new SchemaRegistryEmbedded();
        log.info("-------------------------------------------");
        log.info("In-memory schema registry node started");
        log.info("-------------------------------------------");
    }

    public void shutdown() throws Exception {
        sr.stop();
        broker.stop();
        zk.stop();
        tmpDir.deleteOnExit();
    }
}
