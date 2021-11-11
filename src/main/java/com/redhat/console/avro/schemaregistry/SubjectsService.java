package com.redhat.console.avro.schemaregistry;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@RegisterRestClient()
public interface SubjectsService {
    @GET
    @Path("/subjects/{subject}/versions/{version}/schema")
    String getSchema(@PathParam("subject") String subject, @PathParam("version") String version);

    @GET
    @Path("/subjects/{subject}/versions/{version}")
    Subject getSubject(@PathParam("subject") String subject, @PathParam("version") String version);
}