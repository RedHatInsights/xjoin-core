package com.redhat.console.avro.transformation;

import org.apache.avro.Schema;

public class TransformedFieldFactory {
    public static <T> TransformedField create(Class<T> clazz, Schema schema, ObjectToArrayParameters parameters) throws ClassNotFoundException {
        if (clazz.getSimpleName().equals("ObjectToArrayOfStrings")) {
            return new TransformedArrayOfStrings(schema, parameters);
        } else if (clazz.getSimpleName().equals("ObjectToArrayOfObjects")) {
            return new TransformedArrayOfObjects(schema, parameters);
        } else {
            throw new ClassNotFoundException("Invalid class passed to TransformedFieldFactory: " + clazz.getName());
        }
    }
}