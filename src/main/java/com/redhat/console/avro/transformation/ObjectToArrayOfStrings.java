package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms a JSON object into an array of strings. e.g.
 * <p>
 * {
 * "NS": {
 * "Key": "value"
 * },
 * "Foo": {
 * "Bar": "Baz"
 * }
 * }
 * <p>
 * becomes
 * <p>
 * ["NS/Key/Value", "Foo/Bar/Baz"]
 */
@RegisterForReflection
public class ObjectToArrayOfStrings extends ObjectToArrayTransformation<String> {
    @JsonAlias("transformation.parameters")
    public Parameters parameters;

    @RegisterForReflection
    public static class Parameters implements ObjectToArrayParameters {
        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        public List<String> delimiters;

        public List<String> getParameters() {
            return this.delimiters;
        }
    }

    @Override
    public ObjectToArrayParameters getObjectToArrayParameters() {
        return parameters;
    }

    @Override
    public int getMaxKeys() {
        return getObjectToArrayParameters().getParameters().size() + 1;
    }

    @Override
    public String getNullValue() {
        return "";
    }
}