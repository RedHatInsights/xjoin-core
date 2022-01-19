package com.redhat.console.avro;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.redhat.console.avro.transformation.Transformation;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public class AvroSchema {

    public AvroSchema(){}

    @JsonAlias("xjoin.type")
    public String xjoinType;

    public String type;
    public String name;
    public String namespace;
    public List<Field> fields;

    @JsonAlias("xjoin.transformations")
    public List<Transformation> transformations;

    @RegisterForReflection
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Field {
        public String name;

        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        @JsonDeserialize(contentUsing = AvroTypeDeserializer.class)
        public List<Type> type;

        @JsonAlias("default")
        public String defaultValue;

        @JsonAlias("xjoin.index")
        public boolean xjoinIndex;
    }

    @RegisterForReflection
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Type {
        public String type;

        @JsonAlias("xjoin.type")
        public String xjoinType;

        @JsonAlias("xjoin.fields")
        public List<Field> xjoinFields;
        public List<Field> fields;

        public String name;

        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        @JsonDeserialize(contentUsing = AvroTypeDeserializer.class)
        public List<Type> items;

        @JsonAlias("xjoin.case")
        public String xjoinCase;
    }

    public Field getFieldByName(String name, List<Field> fields) throws IllegalStateException {
        for (Field field : fields) {
            if (field.name == name) {
                return field;
            }
        }

        throw new IllegalStateException("Field " + name + " not found in schema");
    }
}
