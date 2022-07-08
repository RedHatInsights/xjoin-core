package com.redhat.console.avro.transformation;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

abstract public class ObjectToArrayTransformation<T> extends Transformation {
    Schema schema;

    @Override
    public GenericRecord transform(GenericRecord record, Schema transformedSchema) throws IllegalStateException, ClassNotFoundException {
        return super.transform(record, transformedSchema, this::transformOutputFieldData);
    }

    abstract public ObjectToArrayParameters getObjectToArrayParameters();

    public GenericData.Array<T> transformOutputFieldData(JsonObject input, Schema outputFieldSchema) throws ClassNotFoundException {
        this.schema = outputFieldSchema;
        List<TransformedField> transformedFields = this.transformInput(input);
        GenericData.Array<T> transformedRecord = new GenericData.Array<>(transformedFields.size(), outputFieldSchema);
        transformedRecord.addAll(TransformedField.convertList(transformedFields));
        return transformedRecord;
    }

    public List<TransformedField> transformInput(JsonElement input) throws ClassNotFoundException {
        List<TransformedField> completeList = new ArrayList<>();
        return transformInput(input, 0, completeList, null);
    }

    /**
     * Recursively transforms JsonElement into an array of strings
     *
     * @param input              the JsonElement to be transformed
     * @param nodePosition       tracks the position in the tree during recursion
     * @param completeList       the complete list of transformed fields
     * @param transformedField   the representation of the transformed field that is being built recursively
     * @return a list of transformed elements
     */
    private List<TransformedField> transformInput(JsonElement input, int nodePosition, List<TransformedField> completeList, TransformedField transformedField) throws ClassNotFoundException {
        int maxKeys = getObjectToArrayParameters().getParameters().size() + 1;

        if (nodePosition > maxKeys - 1) {
            throw new IllegalStateException("Invalid JSON provided to ObjectToArray transformation. " +
                    "JSON structure is deeper than the number of delimiters (" + getObjectToArrayParameters().getParameters().size() + ").");
        }

        if (input.isJsonObject()) {
            JsonObject inputObject = input.getAsJsonObject();
            for (String inputKey : inputObject.keySet()) {
                /*
                    {
                        "NS": <----
                        {
                            "Key": ["Value"]
                            "Something": ["Another"]
                        },
                        "Foo": <----
                        {
                            "Bar": ["Baz"]
                        }
                    }
                 */
                if (nodePosition == 0) {
                    //when nodePosition == 0 this is a root JsonObject
                    //so the transformed element is reset to an empty field
                    transformedField = TransformedFieldFactory.create(this.getClass(), schema.getElementType(), getObjectToArrayParameters());
                }
                JsonElement value = inputObject.get(inputKey);
                if (value.isJsonObject()) {
                    //parse each child as a completely separate tree
                    JsonObject valueObject = value.getAsJsonObject();
                    if (valueObject.keySet().size() > 1) {
                        /*
                            {
                                "NS":
                                { <-----
                                    "Key": ["Value"],
                                    "Something": ["Another"]
                                },
                                "Foo":
                                {
                                    "Bar": ["Baz"]
                                }
                            }
                         */
                        for (String childKey : valueObject.keySet()) {
                            JsonObject reducedChild = new JsonObject();
                            reducedChild.add(childKey, valueObject.get(childKey));

                            JsonObject reducedParent = new JsonObject();
                            reducedParent.add(inputKey, reducedChild);
                            completeList = transformInput(reducedParent, nodePosition, completeList, transformedField);
                        }
                    } else {
                        /*
                            {
                                "NS":
                                {
                                    "Key": ["Value"],
                                    "Something": ["Another"]
                                },
                                "Foo":
                                { <-----
                                    "Bar": ["Baz"]
                                }
                            }
                         */
                        transformedField.addElement(nodePosition, inputKey);
                        completeList = transformInput(value, nodePosition + 1, completeList, transformedField);
                    }
                } else {
                    /*
                        {
                            "NS":
                            {
                                "Key": ["Value"],
                                "Something": ["Another"]
                            },
                            "Foo":
                            {
                                "Bar": <-----
                                [
                                    "Baz"
                                ]
                            }
                        }
                     */
                    transformedField.addElement(nodePosition, inputKey);
                    completeList = transformInput(value, nodePosition + 1, completeList, transformedField);
                }
            }
        } else if (input.isJsonArray()) {
            /*
                {
                    "Foo":
                    {
                        "Bar":
                        [ <-----
                            "Baz"
                        ]
                    }
                }
             */
            JsonArray array = input.getAsJsonArray();
            if (array.size() > 0) {
                for (JsonElement arrayElem : array) {
                    completeList = transformInput(arrayElem, nodePosition, completeList, transformedField);
                }
            } else {
                transformedField.addElement(nodePosition, "");

                if (nodePosition == maxKeys - 1) {
                    completeList.add(transformedField);
                }
            }
        } else if (input.isJsonPrimitive()) {
            /*
                {
                    "Foo":
                    {
                        "Bar":
                        [
                            "Baz" <----
                        ]
                    }
                }
             */
            JsonPrimitive primitive = input.getAsJsonPrimitive();
            transformedField.addElement(nodePosition, primitive.getAsString());

            if (nodePosition == maxKeys - 1) {
                completeList.add(transformedField);
            }
        }

        return completeList;
    }
}
