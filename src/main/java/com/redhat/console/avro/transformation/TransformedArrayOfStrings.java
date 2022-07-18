package com.redhat.console.avro.transformation;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class TransformedArrayOfStrings extends TransformedField {
    String transformedRecord;

    public TransformedArrayOfStrings(Schema schema, ObjectToArrayParameters parameters) {
        super(schema, parameters);
        transformedRecord = "";
    }

    @Override
    public void addElement(int nodeLevel, String element) {
        transformedRecord = transformedRecord + getDelimiterForNodeLevel(nodeLevel) + element;
    }

    public String getTransformedRecord() {
        return this.transformedRecord;
    }

    private String getDelimiterForNodeLevel(int nodeLevel) {
        if (nodeLevel > parameters.getParameters().size()) {
            throw new IllegalStateException("Invalid JSON provided to ObjectToArrayOfStrings transformation. " +
                    "JSON structure is deeper than the number of delimiters (" + parameters.getParameters().size() + ").");
        }

        if (nodeLevel == 0) {
            return "";
        } else {
            return parameters.getParameters().get(nodeLevel - 1);
        }
    }
}
