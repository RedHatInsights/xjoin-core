package com.redhat.console.testprofile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

//overrides for config values used by unit tests
public class HostTestProfile implements QuarkusTestProfile {
    public Map<String, String> getConfigOverrides() {
        Map<String, String> overrides = new HashMap<>();

        overrides.put("SINK_TOPIC", "test-sink-topic");
        overrides.put("SOURCE_TOPICS", "test-topic");
        overrides.put("SCHEMA_REGISTRY_URL", "http://localhost:8080/apis/registry/v2");
        overrides.put("KAFKA_BOOTSTRAP", "localhost:9092");

        overrides.put("SINK_SCHEMA", loadSchemaFromFile("hostSchemas/sinkValueSchema.json"));
        overrides.put("SINK_VALUE_SCHEMA_META", loadSchemaFromFile("hostSchemas/sinkValueSchemaMeta.json"));

        overrides.put("SOURCE_SCHEMA", loadSchemaFromFile("hostSchemas/sourceValueSchema.json"));
        overrides.put("SOURCE_VALUE_SCHEMA_META", loadSchemaFromFile("hostSchemas/sourceValueSchemaMeta.json"));
        overrides.put("SOURCE_KEY_SCHEMA", loadSchemaFromFile("hostSchemas/sourceKeySchema.json"));
        overrides.put("SOURCE_KEY_SCHEMA_META", loadSchemaFromFile("hostSchemas/sourceKeySchemaMeta.json"));

        return overrides;
    }

    private String loadSchemaFromFile(String fileName) {
        String sinkSchema;
        try (InputStream inputStream = getClass().getResourceAsStream("/" + fileName)) {
            assert inputStream != null;
            sinkSchema = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            sinkSchema = "{}";
        }
        return sinkSchema;
    }
}
