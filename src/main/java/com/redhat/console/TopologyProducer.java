package com.redhat.console;

import java.util.Collections;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TopologyProducer {

    @ConfigProperty(name = "SOURCE_TOPICS")
    String sourceTopics;

    @ConfigProperty(name = "SINK_TOPIC")
    String sinkTopic;

    @ConfigProperty(name = "SCHEMA_REGISTRY_URL")
    String schemaRegistryUrl;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaRegistryUrl);

        Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        keyGenericAvroSerde.configure(serdeConfig, true);

        Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        builder.stream(
            sourceTopics,
            Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde)
        )
        .to(
            sinkTopic,
            Produced.with(keyGenericAvroSerde, valueGenericAvroSerde)
        );

        return builder.build();
    }
}