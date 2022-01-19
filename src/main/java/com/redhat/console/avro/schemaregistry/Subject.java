package com.redhat.console.avro.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
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
