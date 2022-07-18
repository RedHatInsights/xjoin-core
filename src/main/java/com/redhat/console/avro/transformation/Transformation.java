package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.annotation.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * Abstract class to be used by specific Transformations. This contains utility code common to each Transformation,
 * e.g. extracting the Json representation of a field to be transformed.
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "transformation")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ObjectToArrayOfStrings.class, name = "object_to_array_of_strings"),
        @JsonSubTypes.Type(value = ObjectToArrayOfObjects.class, name = "object_to_array_of_objects")}
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

    public abstract GenericRecord transform(GenericRecord record, Schema transformedSchema) throws IllegalStateException, ClassNotFoundException;

    /**
     * Transform this.inputField JSON value into this.outputField using fieldTransformer.
     *
     * @param record a GenericRecord instance that needs to be transformed
     * @param transformedSchema a Schema that contains the transformation fields
     * @param fieldTransformer implementation of TransformationInterface to be used to transform inputField
     * @return The transformed GenericRecord
     * @throws IllegalStateException when the output field is invalid
     */
    public <T> GenericRecord transform(GenericRecord record, Schema transformedSchema, TransformationInterface fieldTransformer) throws IllegalStateException, ClassNotFoundException {
        JsonObject inputField = getInputFieldJsonValue(record);

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
    }

    /**
     * Extracts the InputField from the full GenericRecord and returns the InputField's value as a JsonObject
     *
     * @param fullRecord the full GenericRecord that contains InputField
     * @return JsonObject representation of InputField
     * @throws IllegalStateException when the InputField is not found in the fullRecord
     * @throws JsonSyntaxException   when the InputField is not valid JSON
     */
    private JsonObject getInputFieldJsonValue(GenericRecord fullRecord) throws IllegalStateException, JsonSyntaxException {
        //InputField is a period delimited string, e.g. host.system_profile_facts.arch
        String[] nodeNames = this.getInputField().split("\\.");

        if (nodeNames.length < 2) {
            throw new IllegalStateException("Invalid InputField: '" + this.getInputField() + "', " +
                    "InputField can not be the root field.");
        }

        //retrieve the top level node
        if (!(fullRecord.get(nodeNames[0]) instanceof GenericData.Record currentNode)) {
            throw new IllegalStateException("Invalid InputField: '" + this.getInputField() + "', " +
                    "root field is not an instance of GenericData.Record");
        }

        for (int i = 1; i < nodeNames.length; i++) {
            Object nodeObj = currentNode.get(nodeNames[i]);

            if (nodeObj instanceof Utf8 && i == nodeNames.length - 1) {
                //found the InputField
                String currentNodeString = ((Utf8) nodeObj).toString();
                Gson gson = new Gson();
                return gson.fromJson(currentNodeString, JsonObject.class);
            } else if (i < nodeNames.length - 1 && !(nodeObj instanceof GenericData.Record)) {
                //validate each node in the InputField path is a GenericData.Record
                throw new IllegalStateException("Invalid InputField: '" + this.getInputField() + "'. " +
                        "Parent field (" + nodeNames[i] + ") is not an instance of GenericData.Record.");
            } else if (i < nodeNames.length - 1 && (nodeObj instanceof GenericData.Record)) {
                //go to the next node of InputField
                currentNode = (GenericData.Record) nodeObj;
            } else {
                throw new IllegalStateException("Invalid InputField: '" + this.getInputField() + "'. " +
                        "InputField node is invalid: " + nodeNames[i]);
            }
        }

        throw new IllegalStateException("Invalid InputField: '" + this.getInputField() + "'. " +
                "Unable to locate field in Record.");
    }
}
