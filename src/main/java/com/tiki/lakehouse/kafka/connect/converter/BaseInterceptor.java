package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Comparator;
import java.util.function.Function;

public abstract class BaseInterceptor implements Interceptor {


    protected Schema cloneSchema(Schema schema, Function<SchemaBuilder, SchemaBuilder> cb) {

        SchemaBuilder builder;
        switch (schema.type()) {
            case STRUCT:
                builder = new SchemaBuilder(Schema.Type.STRUCT);
                var structFields = schema.fields();
                if (!structFields.isEmpty()) {
                    structFields.sort(Comparator.comparingInt(Field::index));
                    for (var f : structFields) {
                        builder.field(f.name(), f.schema());
                    }
                }
                break;
            case ARRAY:
                builder = SchemaBuilder.array(schema.valueSchema());
                break;
            case MAP:
                builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
                break;
            default:
                builder = SchemaBuilder.type(schema.type());
                break;
        }


        if (schema.isOptional()) {
            builder.optional();
        } else {
            builder.required();
        }

        var schemaDefaultValue = schema.defaultValue();
        if (schemaDefaultValue != null) builder.defaultValue(schemaDefaultValue);

        builder.name(schema.name());
        builder.version(schema.version());
        builder.doc(schema.doc());

        var schemaParameters = schema.parameters();
        if (schemaParameters != null) builder.parameters(schemaParameters);

        return cb.apply(builder).build();
    }

}
