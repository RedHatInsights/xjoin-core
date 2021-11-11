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

import java.util.ArrayList;
import java.util.List;

public class ObjectToArrayOfStrings extends Transformation {
    @JsonAlias("transformation.parameters")
    public Parameters parameters;

    private static class Parameters {
        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        public List<String> delimiters;
    }

    @Override
    public GenericRecord transform(GenericRecord record, Schema transformedSchema) throws IllegalStateException {
        return super.transform(record, transformedSchema, this::transformOutputFieldData);
    }

    private GenericData.Array<String> transformOutputFieldData(JsonObject input, Schema outputFieldSchema) {
        List<String> records = new ArrayList<>();
        records = parseInput(input, 0, records, "");

        GenericData.Array<String> transformedRecord = new GenericData.Array<>(records.size(), outputFieldSchema);
        transformedRecord.addAll(records);
        return transformedRecord;
    }

    //TODO: validate input against number of delimiters
    //TODO: cleanup logic/duplication
    private List<String> parseInput(JsonElement input, int nodeLevel, List<String> completeList, String element) {
        int maxKeys = parameters.delimiters.size() + 1;

        if (input.isJsonObject()) {
            JsonObject object = input.getAsJsonObject();
            for (String key : object.keySet()) {
                if (nodeLevel == 0) {
                    element = "";
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
                            completeList = parseInput(reducedParent, nodeLevel, completeList, element);
                        }
                    } else {
                        element = element + getDelimiterForNodeLevel(nodeLevel) + key;
                        completeList = parseInput(nextNode, nodeLevel + 1, completeList, element);
                    }
                } else {
                    element = element + getDelimiterForNodeLevel(nodeLevel) + key;
                    completeList = parseInput(nextNode, nodeLevel + 1, completeList, element);
                }
            }
        } else if (input.isJsonArray()) {
            JsonArray array = input.getAsJsonArray();
            if (array.size() > 0) {
                for (JsonElement arrayElem : array) {
                    completeList = parseInput(arrayElem, nodeLevel, completeList, element);
                }
            } else {
                element = element + getDelimiterForNodeLevel(nodeLevel);

                if (nodeLevel == maxKeys - 1 ) {
                    completeList.add(element);
                }
            }
        } else if (input.isJsonPrimitive()) {
            JsonPrimitive primitive = input.getAsJsonPrimitive();
            element = element + getDelimiterForNodeLevel(nodeLevel) + primitive.getAsString();

            if (nodeLevel == maxKeys - 1 ) {
                completeList.add(element);
            }
        }

        return completeList;
    }

    //TODO: check if out of bounds
    private String getDelimiterForNodeLevel(int nodeLevel) {
        if (nodeLevel == 0) {
            return "";
        } else {
            return parameters.delimiters.get(nodeLevel-1);
        }
    }
}
