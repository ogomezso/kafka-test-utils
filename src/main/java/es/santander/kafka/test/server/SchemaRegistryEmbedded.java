package es.santander.kafka.test.server;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import java.util.Properties;

public class SchemaRegistryEmbedded {
    private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
    private static final String KAFKASTORE_DEBUG = "true";
    private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    private static final String AVRO_COMPATIBILITY_TYPE = CompatibilityLevel.NONE.name;

    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";
    private static final String DEFAULT_BOOSTRAP_SERVER = "127.0.0.1:9092";

    private final RestApp schemaRegistry;

    public SchemaRegistryEmbedded() throws Exception {

        schemaRegistry = new RestApp(8081, DEFAULT_ZK_CONNECT, DEFAULT_BOOSTRAP_SERVER, KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE, true, schemaRegistryConfig());
        schemaRegistry.start();
    }

    private Properties schemaRegistryConfig() {
        final Properties schemaRegistryProps = new Properties();
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
        schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);

        return schemaRegistryProps;
    }

    public void stop() throws Exception {
        schemaRegistry.stop();
    }
}
