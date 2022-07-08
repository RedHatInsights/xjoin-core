package com.redhat.console;

import java.util.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redhat.console.avro.SinkSchema;
import com.redhat.console.avro.transformation.Transformer;
import io.apicurio.registry.serde.avro.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    @Inject
    @SinkSchema
    Schema sinkSchema;

    @Inject
    Transformer transformer;

    @Produces
    public Topology buildTopology() {
        //build the streams pipeline
        StreamsBuilder builder = new StreamsBuilder();

        final Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        serdeConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);

        AvroSerde<GenericRecord> keyGenericAvroSerde = new AvroSerde<>();
        keyGenericAvroSerde.configure(serdeConfig, true);

        AvroSerde<GenericRecord> valueGenericAvroSerde = new AvroSerde<>();
        valueGenericAvroSerde.configure(serdeConfig, false);

        final Map<String, Object> outputSerdeConfig = new HashMap<>();
        outputSerdeConfig.put(SerdeConfig.REGISTRY_URL, schemaRegistryUrl);
        outputSerdeConfig.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
        outputSerdeConfig.put(AvroKafkaSerdeConfig.AVRO_ENCODING, AvroKafkaSerdeConfig.AVRO_ENCODING_BINARY);
        outputSerdeConfig.put(SerdeConfig.ENABLE_HEADERS, false);

        AvroSerde<GenericRecord> outputGenericSerde = new AvroSerde<>();
        outputGenericSerde.configure(outputSerdeConfig, false);

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
                            } catch (JsonProcessingException | ClassNotFoundException e) {
                                throw new IllegalStateException(e);
                            }
                        })
                .selectKey((key, value) -> key.get("id").toString())
                .to(
                        sinkTopic,
                        Produced.with(Serdes.String(), outputGenericSerde)
                );

        return builder.build();
    }
}