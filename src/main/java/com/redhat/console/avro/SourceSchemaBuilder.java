package com.redhat.console.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Produces;
import java.io.Serializable;

@ApplicationScoped
public class SourceSchemaBuilder implements Serializable {

    @ConfigProperty(name = "SOURCE_SCHEMA")
    String sourceSchema;

    @Produces
    @SourceSchema
    public Schema buildSourceSchema() {
        Schema.Parser schemaParser = new Schema.Parser();
        return schemaParser.parse(sourceSchema);
    }

    @Produces
    @SourceSchemaPOJO
    public AvroSchema buildSourceSchemaPOJO() throws JsonProcessingException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(sourceSchema, AvroSchema.class);
    }
}

