package com.redhat.console.avro.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redhat.console.avro.AvroSchema;
import com.redhat.console.avro.SinkSchemaPOJO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class Transformer {

    @Inject
    @SinkSchemaPOJO
    AvroSchema sinkSchemaPOJO;

    public GenericRecord transform(GenericRecord record, Schema transformedSchema) throws JsonProcessingException {
        //create new record with transformedSchema
        record = transformRecordSchema((GenericData.Record) record, transformedSchema);

        //apply transformations
        for (Transformation transformation : sinkSchemaPOJO.transformations) {
            record = transformation.transform(record, transformedSchema);
        }

        return record;
    }

    //TODO handle AvroRuntimeExceptions when field is missing from transformed schema
    private GenericData.Record transformRecordSchema(GenericData.Record record, Schema transformedSchema) {
        GenericData.Record transformedRecord = new GenericData.Record(transformedSchema);
        for (Schema.Field field : record.getSchema().getFields()) {
            if (field.schema().getType() == Schema.Type.RECORD) {
                GenericData.Record childRecord = (GenericData.Record) record.get(field.name());
                transformedRecord.put(
                        field.name(),
                        transformRecordSchema(childRecord, transformedSchema.getField(field.name()).schema()));
            } else {
                transformedRecord.put(field.name(), record.get(field.name()));
            }
        }
        return transformedRecord;
    }

}