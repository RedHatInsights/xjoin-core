package com.redhat.console.avro.transformation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class TransformedArrayOfObjects extends TransformedField {
    GenericData.Record transformedRecord;

    public TransformedArrayOfObjects(Schema schema, ObjectToArrayParameters parameters) {
        super(schema, parameters);
        transformedRecord = new GenericData.Record(schema);
    }

    public GenericData.Record getTransformedRecord() {
        return transformedRecord;
    }

    @Override
    public void addElement(int nodeLevel, String element) {
        transformedRecord.put(parameters.getParameters().get(nodeLevel), element);
    }
}
