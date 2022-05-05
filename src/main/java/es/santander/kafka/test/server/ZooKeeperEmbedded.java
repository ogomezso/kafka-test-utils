package es.santander.kafka.test.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;

@Slf4j
public class ZooKeeperEmbedded {
    private final TestingServer server;
    private final int port;

    public ZooKeeperEmbedded(File tmpDir, int port) throws Exception {
        log.info("Starting embedded ZooKeeper server...");
        this.port = port;
        this.server = new TestingServer(port,  tmpDir);
        log.info("Embedded ZooKeeper server at {} uses the temp directory at {}",
                server.getConnectString(), server.getTempDirectory());
    }

    public void stop() throws IOException {
        log.debug("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
        server.stop();
        server.close();
        log.debug("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
    }

    public String connectString() {
        return server.getConnectString();
    }

    /**
     * The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
     */
    public String hostname() {
        // "server:1:2:3" -> "server:1:2"
        return connectString().substring(0, connectString().lastIndexOf(':'));
    }

    public int port() {
        return this.port;
    }

}