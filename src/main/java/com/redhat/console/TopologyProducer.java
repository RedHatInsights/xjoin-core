package com.redhat.console;

import java.util.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.redhat.console.schemaregistry.Subject.Reference;
import com.redhat.console.schemaregistry.Subject;
import com.redhat.console.schemaregistry.SubjectsService;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@ApplicationScoped
public class TopologyProducer {
    @ConfigProperty(name = "KAFKA_BOOTSTRAP")
    String kafkaBootstrap;

    @ConfigProperty(name = "SOURCE_TOPICS")
    String sourceTopics;

    @ConfigProperty(name = "SINK_TOPIC")
    String sinkTopic;

    @ConfigProperty(name = "SCHEMA_REGISTRY_URL")
    String schemaRegistryUrl;

    @Inject
    @RestClient
    SubjectsService subjectsService;

    @Produces
    public Topology buildTopology() {
        //parse the sink/index schema and it's references
        Schema sinkSchema = buildSinkSchema();

        //build the streams pipeline
        StreamsBuilder builder = new StreamsBuilder();

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaRegistryUrl);

        Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        keyGenericAvroSerde.configure(serdeConfig, true);

        Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        builder.stream(
            sourceTopics,
            Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde))
        .mapValues(
                record -> {
                    GenericRecord outputRecord = new GenericData.Record(sinkSchema);

                    //this assumes a 1-1 index from source to sink
                    outputRecord.put(sinkSchema.getFields().get(0).name(), record);
                    return outputRecord;
                })
        .selectKey((key, value) -> {
            return key.get("id").toString();
        })
        .to(
            sinkTopic,
            Produced.with(Serdes.String(), valueGenericAvroSerde)
        );

        return builder.build();
    }

    private Schema buildSinkSchema() {
        Subject sinkSchemaSubject = subjectsService.getSubject(sinkTopic + "-value", "1");
        Schema.Parser schemaParser = new Schema.Parser();

        for (Reference reference : sinkSchemaSubject.references) {
            String referenceSchema = subjectsService.getSchema(reference.subject, reference.version);
            schemaParser.parse(referenceSchema);
        }
        return schemaParser.parse(sinkSchemaSubject.schema);
    }
}