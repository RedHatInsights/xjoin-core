package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redhat.console.avro.AvroSchema;
import com.redhat.console.avro.SinkSchemaPOJO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * This is the top level class that initiates various Transformations on an AvroSchema.
 */
@ApplicationScoped
public class Transformer {

    @Inject
    @SinkSchemaPOJO
    AvroSchema sinkSchemaPOJO;

    /**
     * Transforms a GenericRecord given the transformed Schema.
     *
     * @param record the GenericRecord to transform
     * @param transformedSchema a Schema containing the transformed fields
     * @return the transformed GenericRecord
     * @throws JsonProcessingException when unable to process input field as JSON
     */
    public GenericRecord transform(GenericRecord record, Schema transformedSchema) throws JsonProcessingException, ClassNotFoundException {
        //create new record with transformedSchema
        record = transformRecordSchema((GenericData.Record) record, transformedSchema);

        //apply transformations
        if (sinkSchemaPOJO.transformations != null) {
            for (Transformation transformation : sinkSchemaPOJO.transformations) {
                record = transformation.transform(record, transformedSchema);
            }
        }

        return record;
    }

    /**
     * Builds a new GenericData.Record with the transformed fields from an existing GenericData.Record.
     *
     * @param record a GenericData.Record without the transformation fields
     * @param transformedSchema a Schema containing the transformation fields
     * @return a GenericData.Record with the transformation fields in the schema
     */
    private GenericData.Record transformRecordSchema(GenericData.Record record, Schema transformedSchema) {
        GenericData.Record transformedRecord = new GenericData.Record(transformedSchema);
        for (Schema.Field field : record.getSchema().getFields()) {
            Object recordField = record.get(field.name());

            if (field.schema().getType() == Schema.Type.RECORD) {
                Schema.Field transformedField = transformedSchema.getField(field.name());
                if (transformedField == null) {
                    throw new IllegalStateException("Field " + field.name() + " not found on transformation record.");
                }

                transformedRecord.put(
                        field.name(),
                        transformRecordSchema((GenericData.Record) recordField, transformedField.schema()));
            } else {
                transformedRecord.put(field.name(), recordField);
            }
        }
        return transformedRecord;
    }

}