package com.redhat.console.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Subject {
    public String subject;
    public String version;
    public String id;
    public String schema;
    public List<Reference> references;

    public static class Reference {
        public String name;
        public String version;
        public String subject;
    }
}
