package com.redhat.console.wiremock;

import java.util.Collections;
import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.eclipse.microprofile.config.ConfigProvider;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

//mocks for ApiCurio SerDe REST API calls
public class HostMockSchemaRegistry implements QuarkusTestResourceLifecycleManager {

    private WireMockServer wireMockServer;

    @Override
    public Map<String, String> start() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();

        String sinkValueSchema = ConfigProvider.getConfig().getConfigValue("SINK_SCHEMA").getValue();
        String sinkValueSchemaMeta = ConfigProvider.getConfig().getConfigValue("SINK_VALUE_SCHEMA_META").getValue();

        String sourceValueSchema = ConfigProvider.getConfig().getConfigValue("SOURCE_SCHEMA").getValue();
        String sourceValueSchemaMeta = ConfigProvider.getConfig().getConfigValue("SOURCE_VALUE_SCHEMA_META").getValue();
        String sourceKeySchema = ConfigProvider.getConfig().getConfigValue("SOURCE_KEY_SCHEMA").getValue();
        String sourceKeySchemaMeta = ConfigProvider.getConfig().getConfigValue("SOURCE_KEY_SCHEMA_META").getValue();

        //mock source schema key
        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/groups/default/artifacts/test-topic-key/meta"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceKeySchemaMeta)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/2?dereference=false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceKeySchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/2?dereference=true"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceKeySchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/2/references"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[]")));

        //mock source schema value
        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/groups/default/artifacts/test-topic-value/meta"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceValueSchemaMeta)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/1?dereference=false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceValueSchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/1?dereference=true"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sourceValueSchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/1/references"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[]"
                        )));

        //mock sink schema value
        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/groups/default/artifacts/test-sink-topic-value/meta"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sinkValueSchemaMeta)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/3?dereference=false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sinkValueSchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/3?dereference=true"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(sinkValueSchema)));

        wireMockServer.stubFor(get(urlEqualTo("/apis/registry/v2/ids/globalIds/3/references"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[]"
                        )));

        return Collections.singletonMap("quarkus.rest-client.\"org.acme.rest.client.ExtensionsService\".url", wireMockServer.baseUrl());
    }

    @Override
    public void stop() {
        if (null != wireMockServer) {
            wireMockServer.stop();
        }
    }
}