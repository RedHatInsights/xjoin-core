package com.redhat.console;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.headers.HeadersHandler;
import org.apache.kafka.common.header.Headers;

public class TestHeadersHandler implements HeadersHandler {
    @Override
    public void writeHeaders(Headers headers, ArtifactReference reference) {

    }

    @Override
    public ArtifactReference readHeaders(Headers headers) {
        return ArtifactReference.builder().build();
    }
}
