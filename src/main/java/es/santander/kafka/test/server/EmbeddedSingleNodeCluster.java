package es.santander.kafka.test.server;

import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class EmbeddedSingleNodeCluster {

    private final File tmpDir;
    private ZooKeeperEmbedded zk;
    private KafkaEmbedded broker;
    private SchemaRegistryEmbedded sr;

    public EmbeddedSingleNodeCluster(File tmpDir) throws Exception {
        this.tmpDir = tmpDir;
    }

    public void start() throws Exception {
        log.info("-------------------------------------------");
        log.info("Starting in-memory Zookeeper node ");
        log.info("-------------------------------------------");
        zk = new ZooKeeperEmbedded(this.tmpDir);
        log.info("-------------------------------------------");
        log.info("Zookeeper node started");
        log.info("-------------------------------------------");
        log.info("-------------------------------------------");
        log.info("Starting in-memory broker node ");
        log.info("-------------------------------------------");
        broker = new KafkaEmbedded(this.tmpDir);
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
