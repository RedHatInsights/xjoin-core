package com.redhat.console.schemaregistry;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AvroSchema {

    public String type;
    public String name;
    public String namespace;
    public List<Field> fields;

    public static class Field {
        public String name;

        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        public List<Type> type;
    }

    public static class Type {
        public String type;

        @JsonAlias("xjoin.type")
        public String xjoinType;

        @JsonAlias("xjoin.fields")
        public List<Field> xjoinFields;
    }
}
