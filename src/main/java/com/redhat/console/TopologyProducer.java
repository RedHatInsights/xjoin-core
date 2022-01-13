package com.redhat.console;

import java.util.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redhat.console.avro.SinkSchema;
import com.redhat.console.avro.transformation.Transformer;
import io.apicurio.registry.serde.avro.AvroSerde;
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
import io.apicurio.registry.serde.SerdeConfig;

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

    @Inject @SinkSchema
    Schema sinkSchema;

    @Inject
    Transformer transformer;

    @Produces
    public Topology buildTopology() {
        //build the streams pipeline
        StreamsBuilder builder = new StreamsBuilder();

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaRegistryUrl);

//        Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
//        keyGenericAvroSerde.configure(serdeConfig, true);
//        Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
//        valueGenericAvroSerde.configure(serdeConfig, false);

        //key serde
        Map<String, Object> keyConfig = new HashMap<>();
        keyConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        keyConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
//        keyConfig.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, true);
//        keyConfig.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class);
        AvroSerde<GenericRecord> keyGenericAvroSerde = new AvroSerde<>();
        keyGenericAvroSerde.configure(keyConfig, true);

        //value serde
        Map<String, Object> valueConfig = new HashMap<>();
        valueConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        valueConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
        AvroSerde<GenericRecord> valueGenericAvroSerde = new AvroSerde<>();
        valueGenericAvroSerde.configure(valueConfig, false);

        //topology
        builder.stream(
            sourceTopics,
            Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde))
        .mapValues(
                record -> {
                    try {
                        GenericRecord outputRecord = new GenericData.Record(sinkSchema);
                        //this assumes a 1-1 index from source to sink
                        outputRecord.put(sinkSchema.getFields().get(0).name(), record);
                        outputRecord = transformer.transform(outputRecord, sinkSchema);
                        return outputRecord;
                    } catch (JsonProcessingException e) {
                        throw new IllegalStateException(e);
                    }
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
}