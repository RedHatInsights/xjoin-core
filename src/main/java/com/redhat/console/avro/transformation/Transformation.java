package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.annotation.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "transformation")
@JsonSubTypes({
        @JsonSubTypes.Type(value=ObjectToArrayOfStrings.class, name="object_to_array_of_strings"),
        @JsonSubTypes.Type(value=ObjectToArrayOfObjects.class, name="object_to_array_of_objects") }
)
abstract public class Transformation {
    @JsonTypeId
    public String transformation;

    @JsonAlias("input.field")
    private String inputField;

    @JsonAlias("output.field")
    private String outputField;

    public String getInputField() {
        return inputField;
    }

    public void setInputField(String inputField) {
        this.inputField = inputField;
    }

    public String getOutputField() {
        return outputField;
    }

    public void setOutputField(String outputField) {
        this.outputField = outputField;
    }

    public abstract GenericRecord transform(GenericRecord record, Schema transformedSchema) throws IllegalStateException;

    public GenericRecord transform(GenericRecord record, Schema transformedSchema, TransformationInterface fieldTransformer) throws IllegalStateException {
        JsonObject inputField = getInputFieldJson(record);

        //locate output field in transformedSchema
        String[] nodeNames = this.getOutputField().split("\\.");
        Object currentRecordNode = record.get(nodeNames[0]);
        Schema currentSchemaNode = transformedSchema.getField(nodeNames[0]).schema();

        for (int i = 1; i < nodeNames.length; i++) {
            if (!(currentRecordNode instanceof GenericData.Record)) {
                throw new IllegalStateException("output field '" + this.getOutputField() + "' not found in schema");
            }

            if (i < nodeNames.length - 1) {
                currentRecordNode = ((GenericData.Record) currentRecordNode).getSchema().getField(nodeNames[i]);
                currentSchemaNode = currentSchemaNode.getField(nodeNames[i]).schema();
            } else {
                //at the insertion point
                currentSchemaNode = currentSchemaNode.getField(nodeNames[i]).schema();
                ((GenericData.Record) currentRecordNode).put(
                        nodeNames[i],
                        fieldTransformer.transform(inputField, currentSchemaNode));
            }
        }

        return record;
    };

    private JsonObject getInputFieldJson(GenericRecord fullRecord) throws IllegalStateException{
        String[] nodeNames = this.getInputField().split("\\.");

        GenericData.Record currentNode = (GenericData.Record) fullRecord.get(nodeNames[0]); //TODO check cast

        for (int i = 1; i < nodeNames.length; i++) {
            Object nodeObj = currentNode.get(nodeNames[i]);
            if (i < nodeNames.length - 1 && !(nodeObj instanceof GenericData.Record)) {
                throw new IllegalStateException("input field '" + this.getInputField() + "' not found");
            } else if (i < nodeNames.length -1) {
                currentNode = (GenericData.Record) nodeObj; //TODO check cast
            } else if (nodeObj instanceof Utf8) {
                String currentNodeString = ((Utf8) nodeObj).toString();
                Gson gson = new Gson();
                return gson.fromJson(currentNodeString, JsonObject.class);
            } else {
                throw new IllegalStateException("invalid input field: '" + this.getInputField() + "'");
            }
        }

        throw new IllegalStateException("invalid input field: '" + this.getInputField() + "'");
    }
}
