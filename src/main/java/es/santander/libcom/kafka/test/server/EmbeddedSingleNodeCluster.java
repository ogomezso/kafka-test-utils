package es.santander.libcom.kafka.test.server;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

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
        zk = new ZooKeeperEmbedded(this.tmpDir, generateRandomPort());
        log.info("-------------------------------------------");
        log.info("Zookeeper node started on: {}", zk.connectString());
        log.info("-------------------------------------------");
        log.info("-------------------------------------------");
        log.info("Starting in-memory broker node ");
        log.info("-------------------------------------------");
        broker = new KafkaEmbedded(this.tmpDir, zk.connectString(), generateRandomPort());
        log.info("-------------------------------------------");
        log.info("In-memory broker node started on: {}", broker.brokerConnect());
        log.info("-------------------------------------------");
        log.info("-------------------------------------------");
        log.info("Starting in-memory schema registry node ");
        log.info("-------------------------------------------");
        sr = new SchemaRegistryEmbedded(generateRandomPort(), zk.connectString(), broker.brokerConnect());
        log.info("-------------------------------------------");
        log.info("In-memory schema registry node started on: {}", sr.url());
        log.info("-------------------------------------------");
    }

    public void shutdown() throws Exception {
        sr.stop();
        broker.stop();
        zk.stop();
        tmpDir.deleteOnExit();
    }

    public String  getZkConnectString() {
        return zk.connectString();
    }

    public String getBrokerConnectString() {
        return broker.brokerConnect();
    }

    public String getSrConnectString() {
        return sr.url();
    }
    public int generateRandomPort() {
        ServerSocket s = null;
        try {
            // ServerSocket(0) results in availability of a free random port
            s = new ServerSocket(0);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            assert s != null;
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
