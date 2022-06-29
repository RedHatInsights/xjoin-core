package com.redhat.console.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Produces;
import java.io.Serializable;

@ApplicationScoped
public class SinkSchemaBuilder implements Serializable {

    @ConfigProperty(name = "SINK_SCHEMA")
    String sinkSchema;

    @Produces
    @SinkSchema
    public Schema buildSinkSchema() {
        Schema.Parser schemaParser = new Schema.Parser();
        return schemaParser.parse(sinkSchema);
    }

    @Produces
    @SinkSchemaPOJO
    public AvroSchema buildSinkSchemaPOJO() throws JsonProcessingException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(sinkSchema, AvroSchema.class);
    }
}