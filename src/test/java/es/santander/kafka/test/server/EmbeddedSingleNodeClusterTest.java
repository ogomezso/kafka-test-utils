package es.santander.kafka.test.server;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class EmbeddedSingleNodeClusterTest {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @AfterClass
    public static void cleanup() {
        folder.delete();
    }

    @Test
    public void givenTemporaryFolderEmbeddedClusterShouldStart() throws Exception {

        EmbeddedSingleNodeCluster testCluster = new EmbeddedSingleNodeCluster(folder.newFolder("kafka"));
        testCluster.start();
        testCluster.shutdown();
        assertThatNoException();
    }

}