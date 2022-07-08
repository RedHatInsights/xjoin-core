package com.redhat.console;

import com.redhat.console.avro.SinkSchema;
import com.redhat.console.avro.SourceSchema;
import com.redhat.console.wiremock.HostMockSchemaRegistry;
import com.redhat.console.testprofile.HostTestProfile;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import io.apicurio.registry.serde.avro.AvroSerde;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;

@QuarkusTest
@TestProfile(HostTestProfile.class)
@QuarkusTestResource(HostMockSchemaRegistry.class)
public class HostTopologyProducerTest {

    @Inject
    @SourceSchema
    Schema sourceValueSchema;

    @Inject
    @SinkSchema
    Schema sinkValueSchema;

    @Inject
    Topology topology;

    @ConfigProperty(name = "SOURCE_KEY_SCHEMA")
    String sourceKeySchemaString;

    @ConfigProperty(name = "SCHEMA_REGISTRY_URL")
    String schemaRegistryUrl;

    TestInputTopic<GenericRecord, GenericRecord> inputTopic;
    TestOutputTopic<String, GenericRecord> outputTopic;
    TopologyTestDriver testDriver;

    Schema sourceKeySchema;

    @BeforeEach
    void beforeEach() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
        props.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        this.testDriver = new TopologyTestDriver(topology, props);

        final Map<String, Object> sourceSerdeConfig = new HashMap<>();
        sourceSerdeConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        sourceSerdeConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);

        AvroSerde<GenericRecord> sourceKeySerde = new AvroSerde<>();
        sourceKeySerde.configure(sourceSerdeConfig, true);

        AvroSerde<GenericRecord> sourceValueSerde = new AvroSerde<>();
        sourceValueSerde.configure(sourceSerdeConfig, false);

        this.inputTopic =
                testDriver.createInputTopic("test-topic", sourceKeySerde.serializer(), sourceValueSerde.serializer());

        final Map<String, Object> sinkValueSerdeConfig = new HashMap<>();
        sinkValueSerdeConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        sinkValueSerdeConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
        sinkValueSerdeConfig.put(AvroKafkaSerdeConfig.AVRO_ENCODING, AvroKafkaSerdeConfig.AVRO_ENCODING_BINARY);

        //headers need to be enabled on the output topic because the TopologyTestDriver adds headers to the output record
        //Even with ENABLE_HEADERS==false, the presence of any headers on the output record causes
        //the Avro Serializer to call the HEADERS_HANDLER to parse the headers when calling outputTopic.readValue().
        //When ENABLE_HEADERS==false, there is no HEADERS_HANDLER and an exception is thrown.
        //TestHeadersHandler is a hack to parse the headers into an empty record.
        sinkValueSerdeConfig.put(SerdeConfig.ENABLE_HEADERS, true);
        sinkValueSerdeConfig.put(SerdeConfig.HEADERS_HANDLER, TestHeadersHandler.class);

        AvroSerde<GenericRecord> sinkValueSerde = new AvroSerde<>();
        sinkValueSerde.configure(sinkValueSerdeConfig, false);

        this.outputTopic =
                testDriver.createOutputTopic("test-sink-topic", Serdes.String().deserializer(), sinkValueSerde.deserializer());

        Schema.Parser schemaParser = new Schema.Parser();
        this.sourceKeySchema = schemaParser.parse(sourceKeySchemaString);
    }

    @Test
    void processSimpleHost() {
        //build source
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(sourceKeySchema);
        keyBuilder.set("id", "1");
        GenericRecord keyRecord = keyBuilder.build();

        GenericRecordBuilder sourceValueBuilder = new GenericRecordBuilder(sourceValueSchema);
        sourceValueBuilder.set("id", "1");
        sourceValueBuilder.set("account", "540155");
        sourceValueBuilder.set("org_id", "540155");
        sourceValueBuilder.set("display_name", "test.host.com");
        sourceValueBuilder.set("created_on", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("modified_on", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("facts", "{}");
        sourceValueBuilder.set("tags", "{}");
        sourceValueBuilder.set("canonical_facts", "{}");
        sourceValueBuilder.set("system_profile_facts", "{}");
        sourceValueBuilder.set("ansible_host", "");
        sourceValueBuilder.set("stale_timestamp", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("reporter", "puptoo");
        sourceValueBuilder.set("per_reporter_staleness", "{}");
        GenericRecord valueRecord = sourceValueBuilder.build();

        //run source through the topology
        this.inputTopic.pipeInput(keyRecord, valueRecord);

        //validate sink topic values
        GenericRecordBuilder expectedHostBuilder = new GenericRecordBuilder(sinkValueSchema.getField("host").schema());
        expectedHostBuilder.set("id", "1");
        expectedHostBuilder.set("account", "540155");
        expectedHostBuilder.set("org_id", "540155");
        expectedHostBuilder.set("display_name", "test.host.com");
        expectedHostBuilder.set("created_on", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("modified_on", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("facts", "{}");
        expectedHostBuilder.set("tags", "{}");
        expectedHostBuilder.set("canonical_facts", "{}");
        expectedHostBuilder.set("system_profile_facts", "{}");
        expectedHostBuilder.set("ansible_host", "");
        expectedHostBuilder.set("stale_timestamp", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("reporter", "puptoo");
        expectedHostBuilder.set("per_reporter_staleness", "{}");
        expectedHostBuilder.set("tags_structured", new ArrayList<String>());
        expectedHostBuilder.set("tags_string", new ArrayList<String>());
        expectedHostBuilder.set("tags_search", new ArrayList<String>());

        GenericRecordBuilder expectedBuilder = new GenericRecordBuilder(sinkValueSchema);
        expectedBuilder.set("host", expectedHostBuilder.build());

        Assertions.assertEquals(expectedBuilder.build(), this.outputTopic.readValue());
    }

    @Test
    void processComplexHost() {
        //build source
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(sourceKeySchema);
        keyBuilder.set("id", "1");
        GenericRecord keyRecord = keyBuilder.build();

        GenericRecordBuilder sourceValueBuilder = new GenericRecordBuilder(sourceValueSchema);
        sourceValueBuilder.set("id", "1");
        sourceValueBuilder.set("account", "540155");
        sourceValueBuilder.set("org_id", "540155");
        sourceValueBuilder.set("display_name", "test.host.com");
        sourceValueBuilder.set("created_on", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("modified_on", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("facts", "{}");
        sourceValueBuilder.set("tags", """
                {
                    "tags": {
                      "Sat": {
                        "prod": []
                      },
                      "NS1": {
                        "key3": [
                          "val3"
                        ]
                      },
                      "SPECIAL": {
                        "key": [
                          "val"
                        ]
                      },
                      "NS3": {
                        "key3": [
                          "val3"
                        ]
                      }
                    }
                }""");
        sourceValueBuilder.set("canonical_facts", "{}");
        sourceValueBuilder.set("system_profile_facts", "{}");
        sourceValueBuilder.set("ansible_host", "");
        sourceValueBuilder.set("stale_timestamp", "2022-06-22 14:54:17.465324+00");
        sourceValueBuilder.set("reporter", "puptoo");
        sourceValueBuilder.set("per_reporter_staleness", "{}");
        GenericRecord valueRecord = sourceValueBuilder.build();

        //run source through the topology
        this.inputTopic.pipeInput(keyRecord, valueRecord);

        //validate sink topic values
        GenericRecordBuilder expectedHostBuilder = new GenericRecordBuilder(sinkValueSchema.getField("host").schema());
        expectedHostBuilder.set("id", "1");
        expectedHostBuilder.set("account", "540155");
        expectedHostBuilder.set("org_id", "540155");
        expectedHostBuilder.set("display_name", "test.host.com");
        expectedHostBuilder.set("created_on", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("modified_on", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("facts", "{}");
        expectedHostBuilder.set("tags", """
                {
                    "tags": {
                      "Sat": {
                        "prod": []
                      },
                      "NS1": {
                        "key3": [
                          "val3"
                        ]
                      },
                      "SPECIAL": {
                        "key": [
                          "val"
                        ]
                      },
                      "NS3": {
                        "key3": [
                          "val3"
                        ]
                      }
                    }
                }""");
        expectedHostBuilder.set("canonical_facts", "{}");
        expectedHostBuilder.set("system_profile_facts", "{}");
        expectedHostBuilder.set("ansible_host", "");
        expectedHostBuilder.set("stale_timestamp", "2022-06-22 14:54:17.465324+00");
        expectedHostBuilder.set("reporter", "puptoo");
        expectedHostBuilder.set("per_reporter_staleness", "{}");
        ArrayList<String> expectedTagsStructured = new ArrayList<>();
        expectedTagsStructured.add("""
                {
                    "namespace": "Sat"
                    "key": "prod"
                    "value": null
                }
                """);
        expectedTagsStructured.add("""
                {
                    "namespace": "SPECIAL"
                    "key": "key"
                    "value": "val"
                }
                """);
        expectedTagsStructured.add("""
                {
                    "namespace": "NS1"
                    "key": "key3"
                    "value": "val3"
                }
                """);
        expectedTagsStructured.add("""
                {
                    "namespace": "NS3"
                    "key": "key3"
                    "value": "val3"
                }
                """);
        expectedHostBuilder.set("tags_structured", expectedTagsStructured);
        expectedHostBuilder.set("tags_string", new ArrayList<String>());
        expectedHostBuilder.set("tags_search", new ArrayList<String>());

        GenericRecordBuilder expectedBuilder = new GenericRecordBuilder(sinkValueSchema);
        expectedBuilder.set("host", expectedHostBuilder.build());

        Assertions.assertEquals(expectedBuilder.build(), this.outputTopic.readValue());
    }
}