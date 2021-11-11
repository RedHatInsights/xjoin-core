package com.redhat.console.avro;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

class AvroTypeDeserializer extends JsonDeserializer<AvroSchema.Type> {
    @Override
    public AvroSchema.Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        AvroSchema.Type avroType = new AvroSchema.Type();

        if (p.currentToken() == JsonToken.VALUE_STRING) {
            JavaType javaType = ctxt.getTypeFactory().constructType(String.class);
            JsonDeserializer<Object> deserializer = ctxt.findRootValueDeserializer(javaType);
            avroType.type = (String) deserializer.deserialize(p, ctxt);
        } else if (p.currentToken() == JsonToken.START_OBJECT) {
            JavaType javaType = ctxt.getTypeFactory().constructType(AvroSchema.Type.class);
            JsonDeserializer<Object> deserializer = ctxt.findRootValueDeserializer(javaType);
            avroType = (AvroSchema.Type) deserializer.deserialize(p, ctxt);
        } else {
            throw new IOException("Invalid JSON value for type field. Must be an array of strings and/or objects.");
        }
        return avroType;
    }
}
