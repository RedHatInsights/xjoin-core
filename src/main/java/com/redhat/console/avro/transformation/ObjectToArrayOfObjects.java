package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.avro.generic.GenericData;

import java.util.List;

@RegisterForReflection
public class ObjectToArrayOfObjects extends ObjectToArrayTransformation<GenericData.Record> {
    @JsonAlias("transformation.parameters")
    public Parameters parameters;

    @RegisterForReflection
    public static class Parameters implements ObjectToArrayParameters {
        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        public List<String> keys;

        public List<String> getParameters() {
            return this.keys;
        }
    }

    @Override
    public ObjectToArrayParameters getObjectToArrayParameters() {
        return parameters;
    }

    @Override
    public int getMaxKeys() {
        return getObjectToArrayParameters().getParameters().size();
    }

    @Override
    public String getNullValue() {
        return null;
    }
}