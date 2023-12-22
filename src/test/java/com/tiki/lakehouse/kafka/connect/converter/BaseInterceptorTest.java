package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseInterceptorTest {

    private BaseInterceptor interceptor;

    @Before
    public void before() {
        interceptor = new BaseInterceptor() {

            @Override
            public Schema Apply(Schema target) {
                return target;
            }
        };
    }

    @Test
    public void testSchemaTypeOfClonedSchema() {

        for (Schema.Type type : Schema.Type.values()) {
            Schema sourceSchema;

            switch (type) {
                case MAP:
                    sourceSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA);
                    break;
                case ARRAY:
                    sourceSchema = SchemaBuilder.array(Schema.STRING_SCHEMA);
                    break;
                default:
                    sourceSchema = SchemaBuilder.type(type).build();
                    break;
            }

            interceptor.cloneSchema(sourceSchema, s -> {
                assertEquals(type, s.type());
                return s;
            });

        }
    }

    @Test
    public void testIsOptionalCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).optional().build(), s -> {
            assertEquals(true, s.isOptional());
            return s;
        });
    }

    @Test
    public void testDefaultValueCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).defaultValue(Byte.valueOf((byte)4)).build(), s -> {
            assertEquals(Byte.valueOf((byte)4), s.defaultValue());
            return s;
        });
    }

    @Test
    public void testNameCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).name("name").build(), s -> {
            assertEquals("name", s.name());
            return s;
        });
    }

    @Test
    public void testVersionCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).version(8).build(), s -> {
            assertEquals(new Integer(8), s.version());
            return s;
        });
    }

    @Test
    public void testDocCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).doc("doc").build(), s -> {
            assertEquals("doc", s.doc());
            return s;
        });
    }

    @Test
    public void testParametersCloning() {
        interceptor.cloneSchema(SchemaBuilder.type(Schema.Type.INT8).parameter("foo","bar").build(), s -> {
            assertEquals(1, s.parameters().size());
            assertEquals("bar", s.parameters().get("foo"));
            return s;
        });
    }
}
