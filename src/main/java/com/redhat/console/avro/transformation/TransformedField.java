package com.redhat.console.avro.transformation;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

abstract public class TransformedField {
    Schema schema;
    ObjectToArrayParameters parameters;

    public TransformedField(Schema schema, ObjectToArrayParameters parameters) {
        this.schema = schema;
        this.parameters = parameters;
    }
    abstract void addElement(int nodeLevel, String element);

    abstract public <T> T getTransformedRecord();

    public static <T> List<T> convertList(List<TransformedField> transformedFields) {
        List<T> convertedList = new ArrayList<>();
        for (TransformedField field : transformedFields) {
            convertedList.add(field.getTransformedRecord());
        }
        return convertedList;
    }
}
