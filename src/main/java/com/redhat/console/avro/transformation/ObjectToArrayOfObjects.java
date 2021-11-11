package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;

public class ObjectToArrayOfObjects extends Transformation {
    @JsonAlias("transformation.parameters")
    public Parameters parameters;

    private static class Parameters {
        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        public List<String> keys;
    }

    @Override
    public GenericRecord transform(GenericRecord record, Schema transformedSchema) throws IllegalStateException {
        return super.transform(record, transformedSchema, this::transformRecordData);
    }

    private GenericData.Array<GenericData.Record> transformRecordData(JsonObject input, Schema outputSchema) {
        List<GenericData.Record> records = new ArrayList<>();
        records = parseInput(input, 0, records, null, outputSchema);

        GenericData.Array<GenericData.Record> transformedRecord = new GenericData.Array<>(records.size(), outputSchema);
        transformedRecord.addAll(records);
        return transformedRecord;
    }

    //TODO: verify input has correct number of fields
    //TODO: cleanup logic/duplication
    private List<GenericData.Record> parseInput(JsonElement input, int nodeLevel, List<GenericData.Record> records, GenericData.Record record, Schema schema) {
        int maxKeys = parameters.keys.size();

        if (input.isJsonObject()) {
            JsonObject object = input.getAsJsonObject();
            for (String key : object.keySet()) {
                if (nodeLevel == 0) {
                    record = new GenericData.Record(schema.getElementType());
                }
                JsonElement nextNode = object.get(key);
                if (nextNode.isJsonObject()) {
                    //parse each child as a completely separate tree
                    JsonObject childNode = nextNode.getAsJsonObject();
                    if (childNode.keySet().size() > 1) {
                        for (String childKey : childNode.keySet()) {
                            JsonObject reducedChild = new JsonObject();
                            reducedChild.add(childKey, childNode.get(childKey));

                            JsonObject reducedParent = new JsonObject();
                            reducedParent.add(key, reducedChild);
                            records = parseInput(reducedParent, nodeLevel, records, record, schema);
                        }
                    } else {
                        record.put(parameters.keys.get(nodeLevel), key);
                        records = parseInput(nextNode, nodeLevel + 1, records, record, schema);
                    }
                } else {
                    record.put(parameters.keys.get(nodeLevel), key);
                    records = parseInput(nextNode, nodeLevel + 1, records, record, schema);
                }
            }
        } else if (input.isJsonArray()) {
            JsonArray array = input.getAsJsonArray();
            if (array.size() > 0) {
                for (JsonElement arrayElem : array) {
                    GenericData.Record recordCopy = new GenericData.Record(record, true);
                    records = parseInput(arrayElem, nodeLevel, records, recordCopy, schema);
                }
            } else {
                record.put(parameters.keys.get(nodeLevel), null);

                if (nodeLevel == maxKeys - 1 ) {
                    records.add(record);
                }
            }
        } else if (input.isJsonPrimitive()) {
            JsonPrimitive primitive = input.getAsJsonPrimitive();
            record.put(parameters.keys.get(nodeLevel), primitive.getAsString());

            if (nodeLevel == maxKeys - 1 ) {
                records.add(record);
            }
        }

        return records;
    }
}