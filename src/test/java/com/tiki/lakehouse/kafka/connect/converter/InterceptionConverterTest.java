package com.tiki.lakehouse.kafka.connect.converter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InterceptionConverterTest {

    InterceptionConverter converter;
    Map<String, String> expectedParameters = new HashMap<>();

    AssertionConverter assertionConverter;

    @Before
    public void test() {
        converter = new InterceptionConverter();

        Map<String, String> configs = new HashMap<>();
        configs.put("wrapped.class", "com.tiki.lakehouse.kafka.connect.converter.AssertionConverter");
        configs.put("wrapped.config.propA", "foo");
        configs.put("wrapped.config.propB", "bar");
        configs.put("interceptors", "com.tiki.lakehouse.kafka.connect.converter.BarInterceptor,com.tiki.lakehouse.kafka.connect.converter.FooInterceptor");
        converter.configure(configs, false);

        expectedParameters.put("bar-interceptor", "bar");
        expectedParameters.put("foo-interceptor", "foo");

        assertionConverter = ((AssertionConverter) converter.wrappedConverter);
    }

    @Test
    public void testInitialization() {
        assertNotNull(converter.wrappedConverter);
        assertNotNull(converter.interceptors);
    }

    @Test
    public void testAlteredPrimitiveSchema() {
        var data = converter.fromConnectData("foo", Schema.INT32_SCHEMA, Integer.valueOf(4));
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, data);

        var conversionSchema = assertionConverter.conversionSchema;

        assertEquals(Schema.INT32_SCHEMA.name(), conversionSchema.name());
        assertEquals(Schema.INT32_SCHEMA.type(), conversionSchema.type());
        assertEquals(Schema.INT32_SCHEMA.isOptional(), conversionSchema.isOptional());
        assertEquals(Schema.INT32_SCHEMA.version(), conversionSchema.version());

        assertEquals(expectedParameters, conversionSchema.parameters());
    }

    @Test
    public void testAlteredStructSchema() {

        var schema = SchemaBuilder.struct()
                .field("foo", Schema.INT32_SCHEMA)
                .field("bar", Decimal.schema(3))
                .build();

        converter.fromConnectData("structTopic", schema, null);

        var conversionSchema = assertionConverter.conversionSchema;

        assertEquals(SchemaBuilder.struct().name(), conversionSchema.name());
        assertEquals(SchemaBuilder.struct().type(), conversionSchema.type());
        assertEquals(SchemaBuilder.struct().isOptional(), conversionSchema.isOptional());
        assertEquals(SchemaBuilder.struct().version(), conversionSchema.version());
        assertEquals(Schema.INT32_SCHEMA.type(), conversionSchema.field("foo").schema().type());
        assertEquals(Decimal.schema(3).type(), conversionSchema.field("bar").schema().type());

        assertEquals(expectedParameters, conversionSchema.parameters());
    }
}
