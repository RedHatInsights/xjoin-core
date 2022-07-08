package com.redhat.console.avro.transformation;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;

public interface TransformationInterface {
    Object transform(JsonObject fieldJson, Schema schema) throws ClassNotFoundException;
}
